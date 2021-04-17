import asyncio
import logging
from functools import partial
from typing import Any, Awaitable, Callable, Dict, Hashable, List, Optional, Tuple

from kubernetes_asyncio import watch

KubernetesResourceObject = Any
ObjectNameNamespace = Tuple[str, Optional[str]]

logger = logging.getLogger(__name__)


class EventHandler:
    """
    EventHandler encapsulates functions that will be used to handle three types
    of Kubernetes watch event: adds, updates, and deletions. Each function
    should return an Awaitable. The Awaitable will be used to generate a
    coroutine, and that coroutine will be scheduled as a task within the asyncio
    event loop.
    """

    def __init__(
        self,
        on_add: Optional[Callable[[KubernetesResourceObject], Awaitable[None]]] = None,
        on_update: Optional[
            Callable[
                [KubernetesResourceObject, KubernetesResourceObject], Awaitable[None]
            ]
        ] = None,
        on_delete: Optional[
            Callable[[KubernetesResourceObject], Awaitable[None]]
        ] = None,
    ):
        """
        Create a new EventHandler

        Args:
            on_add (Optional[Callable[[KubernetesResourceObject], Awaitable[None]]], optional):
                Add event handler. Will be ignored by informer if value is None.
                Defaults to None.
            on_update (Optional[ Callable[ [KubernetesResourceObject, KubernetesResourceObject], Awaitable[None] ] ], optional):
                Update event handler. Will be ignored by informer if value is None.
                Defaults to None.
            on_delete (Optional[ Callable[[KubernetesResourceObject], Awaitable[None]] ], optional):
                Delete event handler. Will be ignored by informer if value is None.
                Defaults to None.
        """
        self.on_add = on_add
        self.on_update = on_update
        self.on_delete = on_delete


class SimpleInformer:
    """
    SimpleInformer wraps a kubernetes_asyncio list function to provide event
    notifications using a resilient watch. Unlike other informer
    implementations, SimpleInformer is coupled with its cache.
    """

    def __init__(self, list_function: Callable, *args: Any, **kwargs: Any):
        """
        Create the SimpleInformer

        Args:
            list_function (Callable): Kubernetes object list function

        Raises:
            ValueError: resource_version supplied to list_function.
        """
        kwargs["allow_watch_bookmarks"] = True
        self.list_function = partial(list_function, *args, **kwargs)
        self._cache: Dict[Hashable, KubernetesResourceObject] = {}
        self._stop = False
        self.index_function = object_name_namespace
        self.event_handlers: List[EventHandler] = []
        if "resource_version" in kwargs:
            raise ValueError("resource_version should not be supplied")

    def _sync_resource_version(self):
        """
        Set the watcher's resource_version to the informer's.
        """
        if self._resource_version is None:
            del self._watcher.func.keywords["resource_version"]
            return
        self._watcher.func.keywords["resource_version"] = self._resource_version
        self._watcher.resource_version = self._resource_version

    def _reset_watch(self):
        """
        Reset the kubernetes_asyncio watch. This is tightly coupled to the
        kubernetes_asyncio implementation, and should be a candidate for
        inclusion in the library.
        """
        self._sync_resource_version()
        self._watcher.resp.close()
        self._watcher.resp = None

    async def start(self):
        """
        Start the SimpleInformer
        """
        self._run_task = asyncio.create_task(
            self._process_watch_events_cancellation_handler()
        )
        await self._run_task

    async def stop(self):
        """
        Stop the SimpleInformer through asyncio task cancellation.
        """
        self._run_task.cancel()

    async def _process_watch_events_cancellation_handler(self):
        """
        Await watch event processing and handle cancellations.
        """
        try:
            await self._process_watch_events()
        except asyncio.CancelledError:
            self._watcher.stop()
            await self._watcher.close()

    async def _initialize(self, handle_unwatched_events: bool = True):
        """
        Initialize (or reinitialize) the informer resource version and cache.

        Args:
            handle_unwatched_events (bool, optional): Whether to trigger event handling
                for actions that occurred while the watch was not running - including
                additions, modifications, and deletions. Defaults to True.
        """
        initial_object_list = await self.list_function()

        self._resource_version = initial_object_list.metadata.resource_version
        objects_seen_in_list = set()
        for object_ in initial_object_list.items:
            objects_seen_in_list.add(self.index_function(object_))

            if self.index_function(object_) in self._cache:
                original_object = self._cache[self.index_function(object_)]
                if (
                    original_object.metadata.resource_version
                    != object_.metadata.resource_version
                ):
                    self._cache[self.index_function(object_)] = object_
                    if handle_unwatched_events:
                        self._handle_modified(original_object, object_)
            else:
                self._cache[self.index_function(object_)] = object_
                if handle_unwatched_events:
                    self._handle_added(object_)

            missing_objects = self._cache.keys() - objects_seen_in_list
            for missing_object in missing_objects:
                # These objects were deleted while the watch was being recreated
                old_object = self._cache.pop(missing_object)
                if handle_unwatched_events:
                    self._handle_deleted(old_object)

    async def _process_watch_events(self):
        """
        Stream events and create/schedule asyncio tasks for each event as defined in
        event handlers.

        Raises:
            SimpleInformerWatchException: On unrecoverable watch error
        """

        await self._initialize()
        self._watcher = watch.Watch()

        # Keep track of whether a 410 Gone has been received. If it has, retry
        # once with the latest resource_version (set here by a bookmark, or in
        # kubernetes_asyncio.watch). If that attempt fails, re-list and
        # re-watch.
        #
        # It's possible that this logic (or a subset of it) belongs in the Watch
        # class itself.
        resource_version_gone = False

        async with self._watcher.stream(
            self.list_function.func,
            *self.list_function.args,
            **self.list_function.keywords,
            # Used initially to avoid artificial watch events
            **{"resource_version": self._resource_version},
        ) as watcher_stream:
            async for event in watcher_stream:
                event_type = event["type"].lower()

                if event_type == "error":
                    if event["raw_object"]["code"] == 410:

                        if resource_version_gone:
                            # Reinitialize the watcher using a list-watch.
                            logger.debug(
                                f"resource_version {self._resource_version} gone, reinitializing with a list-watch"
                            )
                            await self._initialize()

                            # No longer gone, can proceed. Reset watch.
                            resource_version_gone = False
                            self._reset_watch()
                            continue

                        # First attempt at retrying the watch. Prevents the need
                        # to reinitialize when bookmarks are available.
                        resource_version_gone = True
                        self._reset_watch()
                    else:
                        raise SimpleInformerWatchException(
                            f"Unrecoverable watch error: {event}"
                        )
                else:
                    resource_version_gone = False

                if event_type == "bookmark":
                    new_resource_version = event["object"].metadata.resource_version
                    previous_resource_version = self._resource_version
                    logger.debug(
                        f"Bookmark received. Syncing resource_version to"
                        f" {new_resource_version}. Was {previous_resource_version}."
                    )
                    self._resource_version = new_resource_version
                    self._sync_resource_version()

                # Don't break if a new event type is added upstream.
                if event_type in ["added", "modified", "deleted"]:
                    identifier = self.index_function(event["object"])
                    object_ = event["object"]

                if event_type == "added":
                    self._cache[identifier] = object_
                    self._handle_added(object_)
                elif event_type == "modified":
                    old_object = self._cache[identifier]
                    self._cache[identifier] = object_
                    self._handle_modified(old_object, object_)
                elif event_type == "deleted":
                    self._cache.pop(identifier, None)
                    self._handle_deleted(object_)

    def _handle_added(self, added_object: KubernetesResourceObject):
        """
        Create and schedule on_add tasks for each event handler.

        Args:
            added_object (KubernetesResourceObject): Added Kubernetes object
        """
        for event_handler in self.event_handlers:
            if event_handler.on_add is not None:
                asyncio.create_task(event_handler.on_add(added_object))

    def _handle_modified(
        self, old_object: KubernetesResourceObject, new_object: KubernetesResourceObject
    ):
        """
        Create and schedule on_update tasks for each event handler.

        Args:
            old_object (KubernetesResourceObject): Original Kubernetes object
            new_object (KubernetesResourceObject): Modified Kubernetes object
        """
        for event_handler in self.event_handlers:
            if event_handler.on_update is not None:
                asyncio.create_task(event_handler.on_update(old_object, new_object))

    def _handle_deleted(self, deleted_object: KubernetesResourceObject):
        """
        Create and schedule on_delete tasks for each event handler.

        Args:
            deleted_object (KubernetesResourceObject): [description]
        """
        for event_handler in self.event_handlers:
            if event_handler.on_delete is not None:
                asyncio.create_task(event_handler.on_delete(deleted_object))

    def add_event_handler(self, event_handler: EventHandler):
        """
        Add event_handler as an informer event handler.

        Provided for convenience - users can also modify self.event_handlers
        directly.

        Args: event_handler (EventHandler): Informer event handler to add
        """
        self.event_handlers.append(event_handler)


def object_name_namespace(object_: KubernetesResourceObject) -> ObjectNameNamespace:
    """
    Return (name, namespace) tuple for given Kubernetes object object_.

    Args:
        object_ (KubernetesResourceObject): Kubernetes object

    Returns:
        ObjectNameNamespace: (name, namespace) tuple. (name, None) if object is
            not namespaced.
    """
    return (object_.metadata.name, object_.metadata.namespace)


class SimpleInformerWatchException(Exception):
    """
    Unrecoverable error in SimpleInformer's watch.
    """

    pass
