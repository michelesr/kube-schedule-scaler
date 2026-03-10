#!/usr/bin/env python
"""Main module of kube-schedule-scaler"""

import concurrent.futures
import json
import logging
import os
import socket
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import partial
from queue import Queue
from signal import SIGABRT, SIGINT, SIGQUIT, SIGTERM, signal, strsignal
from sys import exit
from types import FrameType
from typing import cast

import dateutil
from croniter import croniter
from kubernetes import client, config, watch
from kubernetes.client.models import V1Deployment, V1ObjectMeta
from kubernetes.client.rest import ApiException

# client is shared across the program
try:
    config.load_incluster_config()
except config.ConfigException:
    config.load_kube_config()

apps_v1 = client.AppsV1Api()
autoscaling_v1 = client.AutoscalingV1Api()
coordination_v1 = client.CoordinationV1Api()
authorization_v1 = client.AuthorizationV1Api()

# custom type
ScheduleActions = list[dict[str, str]]

# when this is True, gracefully terminate
shutdown = False
# exit code to return when all threads are terminated
exit_status_code = 0

# Leader election state:
# - is_leader: True when this instance holds the lease (or lease election is disabled)
# - When lease RBAC permissions are absent, is_leader is set to True permanently
#   so the controller behaves as before (all replicas schedule).
is_leader = False

# Leader election configuration (tunable via env vars)
# Defaults mirror the client-go leader election defaults:
#   LeaseDuration=15s, RenewDeadline=10s, RetryPeriod/RenewInterval=5s
# Constraint: LEASE_DURATION_SEC > LEASE_RENEW_DEADLINE_SEC > LEASE_RENEW_INTERVAL_SEC
LEASE_NAME = os.environ.get("LEASE_NAME", "kube-schedule-scaler")
LEASE_DURATION_SEC = int(os.environ.get("LEASE_DURATION_SEC", "15"))
LEASE_RENEW_DEADLINE_SEC = int(os.environ.get("LEASE_RENEW_DEADLINE_SEC", "10"))
LEASE_RENEW_INTERVAL_SEC = int(os.environ.get("LEASE_RENEW_INTERVAL_SEC", "5"))
LEASE_RETRY_INTERVAL_SEC = int(os.environ.get("LEASE_RETRY_INTERVAL_SEC", "10"))

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S",
)


class ScaleTarget(Enum):
    DEPLOYMENT = 0
    HORIZONAL_POD_AUTOSCALER = 1


@dataclass
class DeploymentStore:
    deployments: dict[tuple[str, str], ScheduleActions]
    lock: threading.Lock

    def __init__(self) -> None:
        self.deployments = {}
        self.lock = threading.Lock()


def get_controller_namespace() -> str:
    """Determine the namespace the controller runs in.

    Priority:
    1. The projected service account namespace file (in-cluster)
    2. LEASE_NAMESPACE environment variable
    3. Hardcoded default 'kube-schedule-scaler'
    """
    sa_ns_file = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
    try:
        with open(sa_ns_file) as f:
            return f.read().strip()
    except OSError:
        pass
    return os.environ.get("LEASE_NAMESPACE", "kube-schedule-scaler")


def check_lease_permission(namespace: str) -> bool:
    """Return True if the running identity has the RBAC permissions needed for
    leader election: get, create, and update on leases in coordination.k8s.io."""
    try:
        for verb in ("get", "create", "update"):
            sar = client.V1SelfSubjectAccessReview(
                spec=client.V1SelfSubjectAccessReviewSpec(
                    resource_attributes=client.V1ResourceAttributes(
                        group="coordination.k8s.io",
                        resource="leases",
                        verb=verb,
                        namespace=namespace,
                    )
                )
            )
            resp = cast(
                client.V1SelfSubjectAccessReview,
                authorization_v1.create_self_subject_access_review(body=sar),
            )
            if not cast(client.V1SubjectAccessReviewStatus, resp.status).allowed:
                logging.debug("Lease permission check: verb '%s' not allowed", verb)
                return False
        return True
    except Exception as e:
        logging.debug("Lease permission check failed with exception: %s", e)
        return False


class LeaderElection:
    """Runs a continuous leader election loop using a Kubernetes Lease object.

    The global ``is_leader`` flag is set to True only while this instance holds
    the lease.  A condition variable is used so that ``handle_shutdown`` can
    wake the thread immediately.
    """

    condition = threading.Condition()

    @classmethod
    def run(cls, namespace: str) -> None:
        global is_leader, shutdown

        identity = os.environ.get("HOSTNAME") or socket.gethostname()
        logging.info(
            "Starting leader election thread (identity=%s, lease=%s/%s)",
            identity,
            namespace,
            LEASE_NAME,
        )

        # Tracks when a continuous run of renewal failures started.
        # None means the last operation was successful.
        renew_failure_since: datetime | None = None

        while not shutdown:
            try:
                try:
                    lease = cast(
                        client.V1Lease,
                        coordination_v1.read_namespaced_lease(
                            name=LEASE_NAME, namespace=namespace
                        ),
                    )
                    spec = cast(client.V1LeaseSpec, lease.spec or client.V1LeaseSpec())
                    holder = spec.holder_identity
                    renew_time: datetime | None = spec.renew_time
                    duration = spec.lease_duration_seconds or LEASE_DURATION_SEC

                    now = datetime.now(timezone.utc)

                    # Lease is expired when the holder hasn't renewed within the duration
                    if renew_time is not None and renew_time.tzinfo is None:
                        renew_time = renew_time.replace(tzinfo=timezone.utc)
                    expired = (
                        renew_time is None
                        or (now - renew_time).total_seconds() > duration
                    )

                    if holder == identity or expired:
                        # Acquire or renew
                        transitions = spec.lease_transitions or 0
                        if expired and holder != identity:
                            transitions += 1
                            spec.acquire_time = now
                            logging.info(
                                "Lease expired (held by %s) — acquiring (transition #%d)",
                                holder,
                                transitions,
                            )
                        spec.holder_identity = identity
                        spec.lease_duration_seconds = LEASE_DURATION_SEC
                        spec.renew_time = now
                        spec.lease_transitions = transitions
                        lease.spec = spec
                        coordination_v1.replace_namespaced_lease(
                            name=LEASE_NAME, namespace=namespace, body=lease
                        )
                        # Successful write — reset failure clock
                        renew_failure_since = None
                        if not is_leader:
                            logging.info("Became leader")
                            is_leader = True
                            with Collector.condition:
                                Collector.condition.notify()
                        else:
                            is_leader = True  # renewal — no notify
                    else:
                        if is_leader:
                            logging.info(
                                "Lost leadership — lease now held by %s", holder
                            )
                        is_leader = False
                        renew_failure_since = None

                except ApiException as e:
                    if e.status == 404:
                        # Lease does not exist yet — create it
                        now = datetime.now(timezone.utc)
                        new_lease = client.V1Lease(
                            metadata=client.V1ObjectMeta(
                                name=LEASE_NAME, namespace=namespace
                            ),
                            spec=client.V1LeaseSpec(
                                holder_identity=identity,
                                lease_duration_seconds=LEASE_DURATION_SEC,
                                acquire_time=now,
                                renew_time=now,
                                lease_transitions=0,
                            ),
                        )
                        coordination_v1.create_namespaced_lease(
                            namespace=namespace, body=new_lease
                        )
                        renew_failure_since = None
                        logging.info("Created lease and became leader")
                        is_leader = True
                        with Collector.condition:
                            Collector.condition.notify()
                    elif e.status == 409:
                        # Conflict on replace — another instance just acquired it
                        logging.debug("Lease update conflict — will retry")
                        is_leader = False
                        renew_failure_since = None
                    else:
                        logging.error("Lease API error: %s", e)
                        renew_failure_since = cls._handle_renew_failure(
                            renew_failure_since, is_leader
                        )
                        if renew_failure_since is None:
                            is_leader = False

            except Exception as e:
                logging.error("Leader election error: %s", e)
                renew_failure_since = cls._handle_renew_failure(
                    renew_failure_since, is_leader
                )
                if renew_failure_since is None:
                    is_leader = False

            wait = LEASE_RENEW_INTERVAL_SEC if is_leader else LEASE_RETRY_INTERVAL_SEC
            with cls.condition:
                cls.condition.wait(timeout=wait)

        # Release the lease on graceful shutdown so other instances can take
        # over immediately rather than waiting for the full LEASE_DURATION_SEC.
        if is_leader:
            cls._release_lease(namespace, identity)
        is_leader = False
        logging.info("Leader election thread: exit")

    @classmethod
    def _handle_renew_failure(
        cls, renew_failure_since: datetime | None, currently_leader: bool
    ) -> datetime | None:
        """Track consecutive renewal failures against the deadline.

        Returns the updated ``renew_failure_since`` value:
        - If the deadline has been exceeded (or we were never leader), returns
          ``None`` to signal that leadership should be dropped.
        - Otherwise returns the timestamp when failures started.
        """
        if not currently_leader:
            # Non-leaders don't have a deadline — just keep retrying.
            return renew_failure_since

        now = datetime.now(timezone.utc)
        if renew_failure_since is None:
            logging.warning(
                "Failed to renew lease — will retry for up to %ds before yielding",
                LEASE_RENEW_DEADLINE_SEC,
            )
            return now

        elapsed = (now - renew_failure_since).total_seconds()
        if elapsed >= LEASE_RENEW_DEADLINE_SEC:
            logging.warning(
                "Failed to renew lease for %.1fs (deadline=%ds) — yielding leadership",
                elapsed,
                LEASE_RENEW_DEADLINE_SEC,
            )
            return None  # signal: drop leadership

        logging.debug(
            "Renewal failure ongoing for %.1fs (deadline=%ds)",
            elapsed,
            LEASE_RENEW_DEADLINE_SEC,
        )
        return renew_failure_since  # keep waiting

    @classmethod
    def _release_lease(cls, namespace: str, identity: str) -> None:
        """Best-effort: write the lease with duration=1s and no holder so other
        instances can take over immediately after shutdown."""
        try:
            lease = cast(
                client.V1Lease,
                coordination_v1.read_namespaced_lease(
                    name=LEASE_NAME, namespace=namespace
                ),
            )
            spec = cast(client.V1LeaseSpec, lease.spec or client.V1LeaseSpec())
            # Only release if we still own it — another instance may have
            # already taken over during a slow shutdown.
            if spec.holder_identity != identity:
                logging.debug(
                    "Lease already held by %s — skipping release", spec.holder_identity
                )
                return
            now = datetime.now(timezone.utc)
            spec.holder_identity = ""
            spec.lease_duration_seconds = 1
            spec.renew_time = now
            lease.spec = spec
            coordination_v1.replace_namespaced_lease(
                name=LEASE_NAME, namespace=namespace, body=lease
            )
            logging.info("Released lease on shutdown")
        except Exception as e:
            logging.warning("Failed to release lease on shutdown (best-effort): %s", e)


def parse_schedules(schedules: str, identifier: tuple[str, str]) -> ScheduleActions:
    """Parse the JSON schedule"""
    try:
        return json.loads(schedules)
    except (TypeError, json.JSONDecodeError) as err:
        logging.error("%s - Error in parsing JSON %s", identifier, schedules)
        logging.exception(err)
        return []


def get_delta_sec(schedule: str, timezone_name: str | None = None) -> int:
    """Returns the number of seconds passed since last occurence of the given cron expression"""
    # localize the time to the provided timezone, if specified
    if not timezone_name:
        tz = None
    else:
        tz = dateutil.tz.gettz(timezone_name)

    # get current time
    now = datetime.now(tz)
    # get the last previous occurrence of the cron expression
    time = croniter(schedule, now).get_prev()
    # convert now to unix timestamp
    now_ts = now.timestamp()
    # return the delta
    return int(now_ts - time)


def get_wait_sec() -> float:
    """Return the number of seconds to wait before the next minute"""
    now = datetime.now()
    future = datetime(now.year, now.month, now.day, now.hour, now.minute) + timedelta(
        minutes=1
    )
    return (future - now).total_seconds()


def process_deployment(
    deployment: tuple[str, str], sa: ScheduleActions, queue: Queue
) -> None:
    """Determine actions to run for the given deployment and list of schedules"""
    namespace, name = deployment
    for schedule in sa:
        # when provided, convert the values to int
        replicas = schedule.get("replicas", None)
        if replicas is not None:
            replicas = int(replicas)
        min_replicas = schedule.get("minReplicas", None)
        if min_replicas is not None:
            min_replicas = int(min_replicas)
        max_replicas = schedule.get("maxReplicas", None)
        if max_replicas is not None:
            max_replicas = int(max_replicas)

        schedule_expr = schedule.get("schedule", None)

        if not schedule_expr:
            logging.warning(
                f"Ignoring expression with no schedule in {namespace}/{name}: {schedule}"
            )
            continue

        schedule_timezone = schedule.get("tz", None)
        logging.debug("%s %s", deployment, schedule)

        # if less than 60 seconds have passed from the trigger
        if get_delta_sec(schedule_expr, schedule_timezone) < 60:
            if replicas is not None:
                queue.put((ScaleTarget.DEPLOYMENT, name, namespace, replicas))
            if min_replicas is not None or max_replicas is not None:
                queue.put(
                    (
                        ScaleTarget.HORIZONAL_POD_AUTOSCALER,
                        name,
                        namespace,
                        min_replicas,
                        max_replicas,
                    )
                )


def scale_deployment(name: str, namespace: str, replicas: int) -> None:
    """Scale the deployment to the given number of replicas"""
    try:
        patch_body = {"spec": {"replicas": replicas}}
        apps_v1.patch_namespaced_deployment_scale(
            name=name, namespace=namespace, body=patch_body
        )
        logging.info(
            "Deployment %s/%s scaled to %s replicas", namespace, name, replicas
        )
    except ApiException as e:
        if e.status == 404:
            logging.warning("Deployment %s/%s not found", namespace, name)
        else:
            logging.error("API error patching deployment %s/%s: %s", namespace, name, e)


def scale_hpa(
    name: str, namespace: str, min_replicas: int | None, max_replicas: int | None
) -> None:
    """Adjust HPA min/max replicas via a direct patch"""

    patch_body = {}
    if min_replicas is not None:
        patch_body["minReplicas"] = min_replicas
    if max_replicas is not None:
        patch_body["maxReplicas"] = max_replicas

    if not patch_body:
        return

    try:
        autoscaling_v1.patch_namespaced_horizontal_pod_autoscaler(
            name=name, namespace=namespace, body={"spec": patch_body}
        )

        if min_replicas:
            logging.info(
                "HPA %s/%s minReplicas set to %s", namespace, name, min_replicas
            )
        if max_replicas:
            logging.info(
                "HPA %s/%s maxReplicas set to %s", namespace, name, max_replicas
            )

    except ApiException as e:
        if e.status == 404:
            logging.warning("HPA %s/%s not found", namespace, name)
        else:
            logging.error("API error patching HPA %s/%s: %s", namespace, name, e)


def process_watch_event(ds: DeploymentStore, event: dict) -> str:
    obj: dict | V1Deployment = event["object"]
    event_type = event["type"]

    # some events (e.g. BOOKMARK) return a dict
    if isinstance(obj, dict):
        last_resource_version = obj["metadata"]["resourceVersion"]
    else:
        last_resource_version = cast(
            str, cast(V1ObjectMeta, obj.metadata).resource_version
        )
    logging.debug(f"watch last_resource_version -> {last_resource_version}")

    match event_type:
        case "ADDED" | "MODIFIED" | "DELETED":
            if not isinstance(obj, V1Deployment):
                raise ValueError(f"{event_type} event is not a V1Deployment object")

            metadata = cast(V1ObjectMeta, obj.metadata)
            logging.debug(f"watch {event_type}: {metadata.namespace}/{metadata.name}")
            key = (cast(str, metadata.namespace), cast(str, metadata.name))

            if event_type != "DELETED" and (
                schedules := cast(dict[str, str], metadata.annotations).get(
                    "zalando.org/schedule-actions"
                )
            ):
                res = parse_schedules(schedules, key)
                with ds.lock:
                    ds.deployments[key] = res
            else:
                with ds.lock:
                    ds.deployments.pop(key, None)
        case _:
            logging.debug(f"watch {event_type} {obj}")

    return last_resource_version


def watch_deployments(ds: DeploymentStore, queue: Queue) -> None:
    """Sync deployment objects between k8s api server and kube-schedule-scaler"""
    global shutdown
    logging.info("Starting watcher thread")

    w = watch.Watch()

    last_resource_version = None
    while not shutdown:
        try:
            # watch bookmarks help with having the latest resource version
            # necessary to resume the event stream (on reconnect) without
            # getting 410 "Resource version too old" errors
            stream = w.stream(
                apps_v1.list_deployment_for_all_namespaces,
                resource_version=last_resource_version,
                allow_watch_bookmarks=True,
            )

            for event in stream:
                # watch can keep running for a long time so we need this here
                if shutdown:
                    logging.info("Watcher thread: exit")
                    return

                if not isinstance(event, dict):
                    logging.warning(f"Skipping non dict event data: {event}")
                    continue

                last_resource_version = process_watch_event(ds, event)

                logging.debug(f"Deployments: {ds.deployments}")

        except ApiException as e:
            logging.error(f"Kubernetes API error: {e}")

            # Handle 410 Gone (Resource version too old)
            if e.status == 410:
                logging.debug("Resetting watch last_resource_version: expired")
                last_resource_version = None
                with ds.lock:
                    ds.deployments.clear()
            else:
                logging.error(f"Watcher failed: {type(e).__name__}: {e}")
                handle_shutdown(SIGQUIT, None, queue, exit_code=2)

        except Exception as e:
            logging.error(f"Watcher failed: {type(e).__name__}: {e}")
            handle_shutdown(SIGQUIT, None, queue, exit_code=3)

    logging.info("Watcher thread: exit")


class Collector:
    # collector is wrapped in a class so that we can use the condition
    # to notify it and wake it up on graceful shutdown
    condition = threading.Condition()

    @classmethod
    def collect_scaling_jobs(cls, ds: DeploymentStore, queue: Queue) -> None:
        """Collect scaling jobs and adds them to the queue"""
        global shutdown

        logging.info("Starting collector thread")

        while not shutdown:
            with ds.lock:
                # work on a copy so that we can release the lock sooner
                deployments = ds.deployments.copy()

            if is_leader:
                for deployment, schedule_action in deployments.items():
                    process_deployment(deployment, schedule_action, queue)
            else:
                logging.debug("Not the leader — skipping job collection")
            logging.debug(f"queue items: {list(queue.queue)}")
            # wait until next minute but wake up if you have to shutdown
            with cls.condition:
                cls.condition.wait(timeout=get_wait_sec())

        logging.info("Collector thread: exit")


def process_scaling_jobs(queue: Queue) -> None:
    """Processes scaling jobs"""
    global shutdown
    logging.info("Starting processor thread")

    while not shutdown:
        # this blocks but we can add a dummy item to wake the thread
        # if we want to shut down gracefully
        item = queue.get()
        match item[0]:
            case ScaleTarget.DEPLOYMENT:
                scale_deployment(*item[1:])
            case ScaleTarget.HORIZONAL_POD_AUTOSCALER:
                scale_hpa(*item[1:])

    logging.info("Processor thread: exit")


def handle_shutdown(
    signum: int, _: FrameType | None, queue: Queue, exit_code: int
) -> None:
    """Handle shutdown related signals"""
    global shutdown
    global exit_status_code

    if shutdown:
        # it means it's been already triggered by another signal before
        # no need to do the work twice and it can cause issues
        return

    sig_str = strsignal(signum)
    sig_str = sig_str.split(":")[0] if sig_str else "Unknown"
    logging.info(f"Received {sig_str}: exiting gracefully")
    shutdown = True
    # wake up the processor
    queue.put("notify")
    # wake up the collector
    with Collector.condition:
        Collector.condition.notify()
    # wake up the leader election thread
    with LeaderElection.condition:
        LeaderElection.condition.notify()
    exit_status_code = exit_code


if __name__ == "__main__":
    ds = DeploymentStore()
    queue = Queue()

    signal(SIGTERM, partial(handle_shutdown, queue=queue, exit_code=143))
    signal(SIGINT, partial(handle_shutdown, queue=queue, exit_code=130))
    signal(SIGQUIT, partial(handle_shutdown, queue=queue, exit_code=131))
    signal(SIGABRT, partial(handle_shutdown, queue=queue, exit_code=134))

    lease_namespace = get_controller_namespace()
    lease_election_enabled = check_lease_permission(lease_namespace)

    if lease_election_enabled:
        logging.info(
            f"Lease RBAC check passed — leader election enabled (namespace={lease_namespace})"
        )
    else:
        logging.warning(
            "Lease RBAC check failed — leader election disabled" 
        )
        is_leader = True

    # for the watcher, we use a daemon thread so that it won't block graceful shutdown
    # since there's no easy way to interrupt a watch and the thread could
    # sleep for a long time
    threading.Thread(target=watch_deployments, args=[ds, queue], daemon=True).start()

    # leader election runs in the executor so the process won't exit until it
    # completes, ensuring _release_lease always has a chance to run
    max_workers = 3
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(Collector.collect_scaling_jobs, ds, queue): "collector",
            executor.submit(process_scaling_jobs, queue): "processor",
        }
        if lease_election_enabled:
            futures[executor.submit(LeaderElection.run, lease_namespace)] = "leader-election"

        # NOTE: block waiting for the tasks, but report their success or failure as
        # soon as each individual one completes
        for future in concurrent.futures.as_completed(futures):
            task_name = futures[future]
            try:
                result = future.result()
                logging.debug(f"success: {task_name}: {result}")
            except Exception as e:
                logging.error(f"failure: task {task_name}: {type(e).__name__}: {e}")
                handle_shutdown(SIGQUIT, None, queue, exit_code=1)

    # expliticly return the correct status code since we're trapping signals
    exit(exit_status_code)
