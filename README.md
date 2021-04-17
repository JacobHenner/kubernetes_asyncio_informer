# kubernetes_asyncio_informer

## Description

_kubernetes_asyncio_informer_ is a **proof-of-concept** informer implementation
for [kubernetes_asyncio](https://github.com/tomplus/kubernetes_asyncio/). It's
intended to provide resilient watch functionality for the kubernetes_asyncio
library through an informer, and address three issues:

- [resource version set incorrectly while watching list
  functions](https://github.com/tomplus/kubernetes_asyncio/issues/135)
- [Raise exception when watch returns an
  error](https://github.com/tomplus/kubernetes_asyncio/issues/134)
- [Perform automatic retries for 410s during
  watches](https://github.com/tomplus/kubernetes_asyncio/issues/136)

There is some tight coupling between the informer and kubernetes_asyncio's Watch
object. There is also functionality included within the informer that might be
better suited for inclusion in kubernetes_asyncio's Watch object. Ideally this
will be resolved as the informer is considered for inclusion in
kubernetes_asyncio.
