from queue import Queue
from unittest.mock import MagicMock, patch
import os

import pytest
from freezegun import freeze_time
from kubernetes.client.models import V1Deployment, V1ObjectMeta


@pytest.fixture
def ds():
    return DeploymentStore()


@pytest.fixture(autouse=False)
def instant_le_wait():
    """Patch LeaderElection.condition.wait to return immediately so tests don't
    sleep for LEASE_RENEW_INTERVAL_SEC between loop iterations."""
    import schedule_scaling.main as m

    with patch.object(m.LeaderElection.condition, "wait", return_value=None):
        yield


with (
    patch("kubernetes.config.load_incluster_config"),
    patch("kubernetes.config.load_kube_config"),
):
    from schedule_scaling.main import (
        DeploymentStore,
        ScaleTarget,
        check_lease_permission,
        get_controller_namespace,
        get_delta_sec,
        parse_schedules,
        process_deployment,
        process_watch_event,
        scale_deployment,
        scale_hpa,
    )


def create_mock_event(
    event_type, name, namespace, annotations=None, resource_version="123"
):
    """Helper to create a mock watch event."""
    return {
        "type": event_type,
        "object": V1Deployment(
            metadata=V1ObjectMeta(
                name=name,
                namespace=namespace,
                annotations=annotations or {},
                resource_version=resource_version,
            )
        ),
    }


def test_parse_schedules_valid():
    raw_json = '[{"schedule": "0 9 * * *", "replicas": "5"}]'
    identifier = ("default", "my-app")
    result = parse_schedules(raw_json, identifier)
    assert len(result) == 1
    assert result[0]["replicas"] == "5"
    assert result[0]["schedule"] == "0 9 * * *"


def test_parse_schedules_invalid():
    raw_json = "invalid-json"
    identifier = ("default", "my-app")
    result = parse_schedules(raw_json, identifier)
    assert result == []


# Exactly 30 seconds after the cron trigger
@freeze_time("2024-01-01 09:00:30")
def test_get_delta_sec_triggered():
    schedule = "0 9 * * *"
    delta = get_delta_sec(schedule)
    assert delta == 30


# 90 seconds after the trigger
@freeze_time("2024-01-01 09:01:30")
def test_get_delta_sec_not_triggered():
    schedule = "0 9 * * *"
    delta = get_delta_sec(schedule)
    assert delta == 90


def test_process_event_returns_resource_version(ds):
    schedule_json = '[{"schedule": "0 9 * * *", "replicas": 5}]'
    event = create_mock_event(
        "ADDED",
        "app-1",
        "default",
        {"zalando.org/schedule-actions": schedule_json},
        resource_version="12345",
    )

    assert process_watch_event(ds, event) == "12345"


def test_process_event_added_with_schedule(ds):
    schedule_json = '[{"schedule": "0 9 * * *", "replicas": 5}]'
    event = create_mock_event(
        "ADDED", "app-1", "default", {"zalando.org/schedule-actions": schedule_json}
    )

    process_watch_event(ds, event)

    key = ("default", "app-1")
    assert key in ds.deployments
    assert ds.deployments[key][0]["replicas"] == 5


def test_process_event_modified_updates_store(ds):
    key = ("default", "app-1")
    # Initial state
    ds.deployments[key] = [{"schedule": "old", "replicas": 1}]

    # Update event
    new_json = '[{"schedule": "new", "replicas": 10}]'
    event = create_mock_event(
        "MODIFIED", "app-1", "default", {"zalando.org/schedule-actions": new_json}
    )

    process_watch_event(ds, event)

    assert ds.deployments[key][0]["replicas"] == 10


def test_process_event_deleted_removes_from_store(ds):
    # PRE-CONDITION: Manually seed the store
    key = ("default", "app-1")
    ds.deployments[key] = [{"schedule": "* * * * *", "replicas": 1}]

    # Verify it is actually there first (Sanity check)
    assert key in ds.deployments

    # ACTION: Process the DELETED event
    event = create_mock_event("DELETED", "app-1", "default")
    process_watch_event(ds, event)

    # POST-CONDITION: Verify it was removed
    assert key not in ds.deployments


def test_process_event_annotation_removed(ds):
    # PRE-CONDITION: Deployment exists with a schedule
    key = ("default", "app-1")
    ds.deployments[key] = [{"schedule": "0 9 * * *", "replicas": 5}]

    assert key in ds.deployments

    # ACTION: Process a MODIFIED event where the annotation is missing
    # (Simulates a user running 'kubectl annotate deploy app-1 zalando.org/schedule-actions-')
    event = create_mock_event("MODIFIED", "app-1", "default", annotations={})

    process_watch_event(ds, event)

    # POST-CONDITION: The store should now be empty for this key
    assert key not in ds.deployments


def test_process_event_bookmark(ds):
    # Bookmarks look like this in the watch stream
    event = {
        "type": "BOOKMARK",
        "object": {"kind": "Deployment", "metadata": {"resourceVersion": "999"}},
    }

    # This should not crash and should not modify deployments
    process_watch_event(ds, event)
    assert len(ds.deployments) == 0


@freeze_time("2024-01-01 09:00:10")
def test_process_deployment_queue_deployment():
    queue = Queue()

    deployment_key = ("prod", "web")
    # Only replicas is set
    schedule_actions = [{"schedule": "0 9 * * *", "replicas": "10"}]

    process_deployment(deployment_key, schedule_actions, queue)

    assert queue.qsize() == 1
    item = queue.get()
    assert item == (ScaleTarget.DEPLOYMENT, "web", "prod", 10)


@freeze_time("2024-01-01 09:00:10")
def test_process_deployment_queue_hpa():
    queue = Queue()

    deployment_key = ("prod", "web")
    # Only minReplicas and maxReplicas are set
    schedule_actions = [
        {"schedule": "0 9 * * *", "minReplicas": "2", "maxReplicas": "20"}
    ]

    process_deployment(deployment_key, schedule_actions, queue)

    assert queue.qsize() == 1
    item = queue.get()
    assert item == (ScaleTarget.HORIZONAL_POD_AUTOSCALER, "web", "prod", 2, 20)


@freeze_time("2024-01-01 08:55:00")
def test_process_deployment_too_early_no_queue():
    queue = Queue()
    deployment_key = ("prod", "web")
    schedule_actions = [{"schedule": "0 9 * * *", "replicas": "10"}]

    process_deployment(deployment_key, schedule_actions, queue)

    # Nothing should be in the queue because delta is 5 mins
    assert queue.qsize() == 0


@patch("schedule_scaling.main.apps_v1.patch_namespaced_deployment_scale")
def test_scale_deployment_calls_api(mock_patch):
    scale_deployment("my-deploy", "my-ns", 3)

    # Verify the k8s python client was called with correct body
    mock_patch.assert_called_once_with(
        name="my-deploy", namespace="my-ns", body={"spec": {"replicas": 3}}
    )


@patch(
    "schedule_scaling.main.autoscaling_v1.patch_namespaced_horizontal_pod_autoscaler"
)
def test_scale_hpa_calls_api(mock_patch):
    from schedule_scaling.main import scale_hpa

    # Test case: both min and max replicas provided
    scale_hpa("my-hpa", "my-ns", min_replicas=2, max_replicas=10)

    # Verify the HPA client was called with the correct nested spec body
    mock_patch.assert_called_once_with(
        name="my-hpa",
        namespace="my-ns",
        body={"spec": {"minReplicas": 2, "maxReplicas": 10}},
    )


@patch(
    "schedule_scaling.main.autoscaling_v1.patch_namespaced_horizontal_pod_autoscaler"
)
def test_scale_hpa_partial_patch_min_replicas(mock_patch):
    # Test case: Only min_replicas is provided
    scale_hpa("partial-hpa", "my-ns", min_replicas=5, max_replicas=None)

    # Verify that the patch body only contains the provided field
    mock_patch.assert_called_once_with(
        name="partial-hpa", namespace="my-ns", body={"spec": {"minReplicas": 5}}
    )


@patch(
    "schedule_scaling.main.autoscaling_v1.patch_namespaced_horizontal_pod_autoscaler"
)
def test_scale_hpa_partial_patch_max_replicas(mock_patch):
    # Test case: Only max_replicas is provided
    scale_hpa("partial-hpa", "my-ns", min_replicas=None, max_replicas=5)

    # Verify that the patch body only contains the provided field
    mock_patch.assert_called_once_with(
        name="partial-hpa", namespace="my-ns", body={"spec": {"maxReplicas": 5}}
    )


def test_deployment_store_lock():
    ds = DeploymentStore()
    assert ds.deployments == {}
    # Ensure lock exists
    assert not ds.lock.locked()
    with ds.lock:
        assert ds.lock.locked()


# ---------------------------------------------------------------------------
# get_controller_namespace
# ---------------------------------------------------------------------------


def test_get_controller_namespace_reads_sa_file(tmp_path):
    ns_file = tmp_path / "namespace"
    ns_file.write_text("my-namespace")
    with patch("builtins.open", return_value=ns_file.open()):
        result = get_controller_namespace()
    assert result == "my-namespace"


def test_get_controller_namespace_fallback_to_env(monkeypatch):
    monkeypatch.setenv("LEASE_NAMESPACE", "from-env")
    with patch("builtins.open", side_effect=OSError):
        result = get_controller_namespace()
    assert result == "from-env"


def test_get_controller_namespace_default(monkeypatch):
    monkeypatch.delenv("LEASE_NAMESPACE", raising=False)
    with patch("builtins.open", side_effect=OSError):
        result = get_controller_namespace()
    assert result == "kube-schedule-scaler"


# ---------------------------------------------------------------------------
# check_lease_permission
# ---------------------------------------------------------------------------


def _make_sar_response(allowed: bool) -> MagicMock:
    resp = MagicMock()
    resp.status.allowed = allowed
    return resp


@patch("schedule_scaling.main.authorization_v1.create_self_subject_access_review")
def test_check_lease_permission_all_allowed(mock_sar):
    mock_sar.return_value = _make_sar_response(True)
    assert check_lease_permission("kube-schedule-scaler") is True
    # Called once per verb: get, create, update
    assert mock_sar.call_count == 3


@patch("schedule_scaling.main.authorization_v1.create_self_subject_access_review")
def test_check_lease_permission_one_denied(mock_sar):
    # First call (get) returns denied
    mock_sar.return_value = _make_sar_response(False)
    assert check_lease_permission("kube-schedule-scaler") is False
    # Should short-circuit after the first denied verb
    assert mock_sar.call_count == 1


@patch("schedule_scaling.main.authorization_v1.create_self_subject_access_review")
def test_check_lease_permission_api_exception(mock_sar):
    from kubernetes.client.rest import ApiException

    mock_sar.side_effect = ApiException(status=403, reason="Forbidden")
    assert check_lease_permission("kube-schedule-scaler") is False


# ---------------------------------------------------------------------------
# LeaderElection — Collector notification on leadership transition
# ---------------------------------------------------------------------------


def _make_lease(holder: str, renew_time, duration: int = 15, transitions: int = 0):
    """Build a mock V1Lease with the given spec fields."""
    from kubernetes import client as k8s_client

    lease = MagicMock(spec=k8s_client.V1Lease)
    lease.metadata = k8s_client.V1ObjectMeta(
        name="kube-schedule-scaler", namespace="default"
    )
    spec = MagicMock(spec=k8s_client.V1LeaseSpec)
    spec.holder_identity = holder
    spec.renew_time = renew_time
    spec.lease_duration_seconds = duration
    spec.lease_transitions = transitions
    lease.spec = spec
    return lease


@patch("schedule_scaling.main.coordination_v1.replace_namespaced_lease")
@patch("schedule_scaling.main.coordination_v1.read_namespaced_lease")
def test_leader_election_notifies_collector_on_new_leadership(
    mock_read, mock_replace, instant_le_wait
):
    """Collector is notified when transitioning from non-leader to leader."""
    from datetime import datetime, timezone, timedelta
    import schedule_scaling.main as m

    expired_time = datetime.now(timezone.utc) - timedelta(seconds=60)
    mock_read.return_value = _make_lease(holder="other-pod", renew_time=expired_time)

    # Shut down after the first successful replace so the loop exits
    def replace_and_stop(*args, **kwargs):
        m.shutdown = True

    mock_replace.side_effect = replace_and_stop

    m.is_leader = False
    m.shutdown = False

    with patch.object(m.Collector.condition, "notify") as mock_notify:
        m.LeaderElection.run("default")

    mock_notify.assert_called_once()
    m.shutdown = False  # restore for other tests


@patch("schedule_scaling.main.coordination_v1.replace_namespaced_lease")
@patch("schedule_scaling.main.coordination_v1.read_namespaced_lease")
def test_leader_election_does_not_notify_collector_on_renewal(
    mock_read, mock_replace, instant_le_wait
):
    """Collector is NOT notified when already the leader and just renewing."""
    from datetime import datetime, timezone, timedelta
    import schedule_scaling.main as m

    recent_time = datetime.now(timezone.utc) - timedelta(seconds=2)
    mock_read.return_value = _make_lease(holder="our-pod", renew_time=recent_time)

    def replace_and_stop(*args, **kwargs):
        m.shutdown = True

    mock_replace.side_effect = replace_and_stop

    m.is_leader = True
    m.shutdown = False

    with (
        patch.dict(os.environ, {"HOSTNAME": "our-pod"}),
        patch.object(m.Collector.condition, "notify") as mock_notify,
    ):
        m.LeaderElection.run("default")

    mock_notify.assert_not_called()
    m.shutdown = False  # restore for other tests


@patch("schedule_scaling.main.coordination_v1.create_namespaced_lease")
@patch("schedule_scaling.main.coordination_v1.read_namespaced_lease")
def test_leader_election_notifies_collector_on_create(
    mock_read, mock_create, instant_le_wait
):
    """Collector is notified when leadership is acquired by creating a new lease."""
    from kubernetes.client.rest import ApiException
    import schedule_scaling.main as m

    mock_read.side_effect = ApiException(status=404, reason="Not Found")

    def create_and_stop(*args, **kwargs):
        m.shutdown = True

    mock_create.side_effect = create_and_stop

    m.is_leader = False
    m.shutdown = False

    with patch.object(m.Collector.condition, "notify") as mock_notify:
        m.LeaderElection.run("default")

    mock_notify.assert_called_once()
    m.shutdown = False  # restore for other tests


# ---------------------------------------------------------------------------
# Collector is_leader gate
# ---------------------------------------------------------------------------


@freeze_time("2024-01-01 09:00:10")
@patch("schedule_scaling.main.is_leader", False)
def test_collector_skips_jobs_when_not_leader():
    """When is_leader is False, no jobs should be enqueued."""
    import schedule_scaling.main as m

    ds = DeploymentStore()
    ds.deployments[("prod", "web")] = [{"schedule": "0 9 * * *", "replicas": "3"}]
    queue = Queue()

    # Run one iteration of the collector body (without the loop)
    deployments = ds.deployments.copy()
    if m.is_leader:
        for deployment, schedule_action in deployments.items():
            m.process_deployment(deployment, schedule_action, queue)

    assert queue.qsize() == 0


@freeze_time("2024-01-01 09:00:10")
@patch("schedule_scaling.main.is_leader", True)
def test_collector_adds_jobs_when_leader():
    """When is_leader is True, jobs should be enqueued normally."""
    import schedule_scaling.main as m

    ds = DeploymentStore()
    ds.deployments[("prod", "web")] = [{"schedule": "0 9 * * *", "replicas": "3"}]
    queue = Queue()

    deployments = ds.deployments.copy()
    if m.is_leader:
        for deployment, schedule_action in deployments.items():
            m.process_deployment(deployment, schedule_action, queue)

    assert queue.qsize() == 1
    item = queue.get()
    assert item == (ScaleTarget.DEPLOYMENT, "web", "prod", 3)


# ---------------------------------------------------------------------------
# LeaderElection — renew deadline
# ---------------------------------------------------------------------------


@patch("schedule_scaling.main.coordination_v1.replace_namespaced_lease")
@patch("schedule_scaling.main.coordination_v1.read_namespaced_lease")
def test_renew_deadline_not_exceeded_keeps_leadership(
    mock_read, mock_replace, instant_le_wait
):
    """A renewal failure within the deadline window keeps is_leader=True."""
    from datetime import datetime, timezone, timedelta
    from kubernetes.client.rest import ApiException
    import schedule_scaling.main as m

    # Lease is ours and not expired
    recent_time = datetime.now(timezone.utc) - timedelta(seconds=2)
    mock_read.return_value = _make_lease(holder="our-pod", renew_time=recent_time)

    call_count = 0
    # Track is_leader value immediately after the first failure
    is_leader_after_failure = None

    def replace_fail_then_stop(*args, **kwargs):
        nonlocal call_count, is_leader_after_failure
        call_count += 1
        if call_count == 1:
            raise ApiException(status=500, reason="Internal Server Error")
        # On second call (successful renewal): capture is_leader and stop
        is_leader_after_failure = m.is_leader
        m.shutdown = True

    mock_replace.side_effect = replace_fail_then_stop

    m.is_leader = True
    m.shutdown = False

    with patch.dict(os.environ, {"HOSTNAME": "our-pod"}):
        m.LeaderElection.run("default")

    # Leadership must have been retained after the first failure (deadline not exceeded)
    assert is_leader_after_failure is True
    m.shutdown = False


@patch("schedule_scaling.main.coordination_v1.replace_namespaced_lease")
@patch("schedule_scaling.main.coordination_v1.read_namespaced_lease")
def test_renew_deadline_exceeded_drops_leadership(
    mock_read, mock_replace, instant_le_wait
):
    """Consecutive renewal failures past the deadline drop is_leader to False."""
    from datetime import datetime, timezone, timedelta
    from kubernetes.client.rest import ApiException
    import schedule_scaling.main as m

    recent_time = datetime.now(timezone.utc) - timedelta(seconds=2)
    mock_read.return_value = _make_lease(holder="our-pod", renew_time=recent_time)

    # Simulate the deadline already being exceeded by patching _handle_renew_failure
    # to return None (deadline exceeded signal) immediately.
    def handle_failure_exceeded(renew_failure_since, currently_leader):
        return None  # signals: deadline exceeded, drop leadership

    mock_replace.side_effect = ApiException(status=500, reason="Internal Server Error")

    leadership_dropped_at = []

    original_handle = m.LeaderElection._handle_renew_failure

    call_count = 0

    def patched_handle(renew_failure_since, currently_leader):
        nonlocal call_count
        call_count += 1
        result = handle_failure_exceeded(renew_failure_since, currently_leader)
        if result is None:
            m.shutdown = True  # stop after deadline is exceeded
        return result

    m.is_leader = True
    m.shutdown = False

    with (
        patch.dict(os.environ, {"HOSTNAME": "our-pod"}),
        patch.object(m.LeaderElection, "_handle_renew_failure", patched_handle),
    ):
        m.LeaderElection.run("default")

    assert m.is_leader is False
    m.shutdown = False


# ---------------------------------------------------------------------------
# LeaderElection — release on shutdown
# ---------------------------------------------------------------------------


@patch("schedule_scaling.main.coordination_v1.replace_namespaced_lease")
@patch("schedule_scaling.main.coordination_v1.read_namespaced_lease")
def test_release_on_shutdown_writes_expired_lease(
    mock_read, mock_replace, instant_le_wait
):
    """On shutdown while holding the lease, _release_lease is called."""
    from datetime import datetime, timezone, timedelta
    import schedule_scaling.main as m

    recent_time = datetime.now(timezone.utc) - timedelta(seconds=2)
    mock_read.return_value = _make_lease(holder="our-pod", renew_time=recent_time)

    # First replace (renewal) stops the loop; _release_lease will call read+replace again
    renewal_done = False

    def replace_side_effect(*args, **kwargs):
        nonlocal renewal_done
        if not renewal_done:
            renewal_done = True
            m.shutdown = True
        # return a fresh lease for the release read
        return None

    mock_replace.side_effect = replace_side_effect

    m.is_leader = True
    m.shutdown = False

    with patch.dict(os.environ, {"HOSTNAME": "our-pod"}):
        m.LeaderElection.run("default")

    # replace should have been called at least twice: once for renewal, once for release
    assert mock_replace.call_count >= 2

    # The release call must set lease_duration_seconds=1 and holder_identity=""
    release_call_body = (
        mock_replace.call_args_list[-1].kwargs.get("body")
        or mock_replace.call_args_list[-1].args[2]
    )
    release_spec = release_call_body.spec
    assert release_spec.lease_duration_seconds == 1
    assert release_spec.holder_identity == ""
    m.shutdown = False


@patch("schedule_scaling.main.coordination_v1.replace_namespaced_lease")
@patch("schedule_scaling.main.coordination_v1.read_namespaced_lease")
def test_no_release_on_shutdown_if_not_leader(mock_read, mock_replace):
    """No release write is made when shutting down without holding the lease."""
    from datetime import datetime, timezone, timedelta
    import schedule_scaling.main as m

    # Lease is held by someone else
    recent_time = datetime.now(timezone.utc) - timedelta(seconds=2)
    mock_read.return_value = _make_lease(holder="other-pod", renew_time=recent_time)

    m.is_leader = False
    m.shutdown = True  # exit immediately without any iteration

    replace_call_count_before = mock_replace.call_count
    m.LeaderElection.run("default")

    # No replace should have been called (no release, no renewal)
    assert mock_replace.call_count == replace_call_count_before
    m.shutdown = False
