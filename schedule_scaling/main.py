#!/usr/bin/env python
"""Main module of kube-schedule-scaler"""

import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Coroutine, cast

import dateutil
from croniter import croniter
from kubernetes_asyncio import client, config, watch
from kubernetes_asyncio.client.models import V1Deployment, V1ObjectMeta
from kubernetes_asyncio.client.rest import ApiException

apps_v1 = None
autoscaling_v1 = None

# custom type
ScheduleActions = list[dict[str, str]]

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

    def __init__(self) -> None:
        self.deployments = {}


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


async def process_deployment(
    deployment: tuple[str, str], sa: ScheduleActions, queue: asyncio.Queue
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
            return

        schedule_timezone = schedule.get("tz", None)
        logging.debug("%s %s", deployment, schedule)

        # if less than 60 seconds have passed from the trigger
        if get_delta_sec(schedule_expr, schedule_timezone) < 60:
            if replicas is not None:
                item = (ScaleTarget.DEPLOYMENT, name, namespace, replicas)
                await queue.put(item)
                logging.debug(f"Queue item added: {item}")
            if min_replicas is not None or max_replicas is not None:
                item = (
                    ScaleTarget.HORIZONAL_POD_AUTOSCALER,
                    name,
                    namespace,
                    min_replicas,
                    max_replicas,
                )
                await queue.put(item)
                logging.debug(f"Queue item added: {item}")


async def scale_deployment(name: str, namespace: str, replicas: int) -> None:
    """Scale the deployment to the given number of replicas"""
    if apps_v1 is None:
        raise ValueError("apps_v1 client not initialized")

    try:
        patch_body = {"spec": {"replicas": replicas}}

        await cast(
            Coroutine,
            apps_v1.patch_namespaced_deployment_scale(
                name=name, namespace=namespace, body=patch_body
            ),
        )

        logging.info(
            "Deployment %s/%s scaled to %s replicas", namespace, name, replicas
        )
    except ApiException as e:
        if e.status == 404:
            logging.warning("Deployment %s/%s not found", namespace, name)
        else:
            logging.error("API error patching deployment %s/%s: %s", namespace, name, e)


async def scale_hpa(
    name: str,
    namespace: str,
    min_replicas: int | None,
    max_replicas: int | None,
) -> None:
    """Adjust HPA min/max replicas via a direct patch"""

    if autoscaling_v1 is None:
        raise ValueError("autoscaling_v1 client not initialized")

    patch_body = {}
    if min_replicas is not None:
        patch_body["minReplicas"] = min_replicas
    if max_replicas is not None:
        patch_body["maxReplicas"] = max_replicas

    if not patch_body:
        return

    try:
        await cast(
            Coroutine,
            autoscaling_v1.patch_namespaced_horizontal_pod_autoscaler(
                name=name, namespace=namespace, body={"spec": patch_body}
            ),
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
                ds.deployments[key] = res
            else:
                ds.deployments.pop(key, None)
        case _:
            logging.debug(f"watch {event_type} {obj}")

    return last_resource_version


async def watch_deployments(ds: DeploymentStore):
    """Sync deployment objects between k8s api server and kube-schedule-scaler"""
    if apps_v1 is None:
        raise ValueError("apps_v1 client not initialized")

    logging.info("Watcher task started")

    w = watch.Watch()
    last_resource_version = None

    while True:
        try:
            async for event in w.stream(
                apps_v1.list_deployment_for_all_namespaces,
                resource_version=last_resource_version,
                allow_watch_bookmarks=True,
            ):
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
                ds.deployments.clear()
            else:
                raise


async def collect_scaling_jobs(ds: DeploymentStore, queue: asyncio.Queue):
    """
    Checks the store every minute and puts jobs into the queue.
    """
    logging.info("Collector task started")

    while True:
        for deployment, schedule_action in ds.deployments.items():
            await process_deployment(deployment, schedule_action, queue)
        await asyncio.sleep(get_wait_sec())


async def process_scaling_jobs(queue: asyncio.Queue) -> None:
    """Processes scaling jobs"""
    logging.info("Processor task started")

    while True:
        item = await queue.get()
        logging.debug(f"Queue item extracted: {item}")
        match item[0]:
            case ScaleTarget.DEPLOYMENT:
                await scale_deployment(*item[1:])
            case ScaleTarget.HORIZONAL_POD_AUTOSCALER:
                await scale_hpa(*item[1:])
        queue.task_done()


async def main():
    try:
        config.load_incluster_config()
    except config.ConfigException:
        await config.load_kube_config()

    global apps_v1, autoscaling_v1

    async with client.ApiClient() as api_client:
        apps_v1 = client.AppsV1Api(api_client)
        autoscaling_v1 = client.AutoscalingV1Api(api_client)

        ds = DeploymentStore()
        queue = asyncio.Queue()

        tasks = [
            asyncio.create_task(watch_deployments(ds)),
            asyncio.create_task(collect_scaling_jobs(ds, queue)),
            asyncio.create_task(process_scaling_jobs(queue)),
        ]

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logging.info("Interrupted")


if __name__ == "__main__":
    asyncio.run(main())
