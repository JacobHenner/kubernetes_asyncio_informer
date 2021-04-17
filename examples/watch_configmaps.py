#!/usr/bin/env python3

import asyncio
import logging

from kubernetes_asyncio import client, config

from kubernetes_asyncio_informer import EventHandler, SimpleInformer


async def print_added(object_):
    print(f"Added: ({object_.metadata.name}, {object_.metadata.namespace})")


async def print_deleted(object_):
    print(f"Deleted: ({object_.metadata.name}, {object_.metadata.namespace})")


async def print_updated(old_object, new_object):
    print(
        f"Updated: ({old_object.metadata.name}, {old_object.metadata.namespace}) {old_object.metadata.resource_version} -> {new_object.metadata.resource_version}"
    )


async def main():
    logging.basicConfig(level=logging.DEBUG)
    await config.load_kube_config()

    async with client.ApiClient() as api:
        v1 = client.CoreV1Api(api)

        simple_configmap_informer = SimpleInformer(
            v1.list_namespaced_config_map,
            namespace="default",
        )

        # This basic event handler will print the name and namespace of added,
        # updated, or deleted objects. For more complex tasks (e.g. a
        # controller), consider writing handler functions that queue objects for
        # further processing.
        print_event_handler = EventHandler(print_added, print_updated, print_deleted)

        simple_configmap_informer.add_event_handler(print_event_handler)

        asyncio.create_task(simple_configmap_informer.start())

        # Schedule a stop in one minute
        async def stop():
            await asyncio.sleep(60)
            await simple_configmap_informer.stop()

        stop_task = asyncio.create_task(stop())
        await stop_task


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
