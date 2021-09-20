# Write your own Runtime

The number of technologies that allow you to orchestrate data pipelines at scale is growing every day.

If you want to take advantage of _Dagger_'s existing features, but run DAGs using a different orchestration engine, a valid option is to implement your own runtime.

Implementing a runtime may be easy (for instance, we believe it would be fairly easy to implement a runtime for _Kubeflow Pipelines_ based on the existing runtime for _Argo Workflows_) or it may be a complex endeavor, depending on the orchestration engine you choose.

Therefore, we don't have a specific guide on how to write runtimes, but rather a series of tips based on our experience writing them:

- Explore the code of the [existing runtimes](https://github.com/larribas/dagger/tree/main/dagger/runtime) to understand how they're built.
- Explore the APIs of the core data structures ([tasks](https://github.com/larribas/dagger/tree/main/dagger/task), [dags](https://github.com/larribas/dagger/tree/main/dagger/dag), [inputs](https://github.com/larribas/dagger/tree/main/dagger/input), [outputs](https://github.com/larribas/dagger/tree/main/dagger/output) and [serializers](https://github.com/larribas/dagger/tree/main/dagger/serializer)) to see all the information you can get from them.
- Begin with a small proof of concept. Try to get a hello world example going. Then move on to more complex scenarios, bit by bit.
- Leverage the existing functionality available in the other runtimes. Do not copy; reuse.
- Open an issue in our [GitHub repo](https://github.com/larribas/dagger) or a thread in [GitHub Discussions](https://github.com/larribas/dagger/discussions). We will be happy to talk about your idea and provide some guidance along the way.
