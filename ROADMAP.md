# Roadmap

This document describes the current state of _Dagger_ and the features we will prioritize in the future.

The roadmap of the project is meant to be flexible, shaped by our interactions with the community and the time our contributors can dedicate to the project. Thus, if you have a suggestion or believe a certain ticket should be prioritized, please let us know through [GitHub Discussions](TODO) or via [email: dagger@larribas.me](mailto:dagger@larribas.me).


__Use cases__

- [x] Parameterize DAGs and use those parameters from Tasks.
- [x] Use the output of a node as the input of another.
- [x] Compose multiple DAGs together ([#5]).
- [x] Pass extra runtime options to any node ([#6]).
- [x] Define DAGs using an imperative, Pythonic DSL ([#7]).
- [x] Use hardcoded/literal values as parameters to a task ([#9]).
- [x] Support map-reduce operations via partitioned outputs and nodes ([#12]).
- [ ] Support conditional executions of tasks.
- [ ] Support exit hooks (e.g. `on_success`, `on_failure`).
- [ ] Support node caching/memoization.
- [ ] Support parallel execution of nodes in the local runtime.


__Supported Runtimes__

- [x] Local execution
- [x] CLI-driven execution
- [x] Argo Workflows ([official website](https://argoproj.github.io/workflows))
- [ ] Kubeflow Pipelines ([official website](https://www.kubeflow.org/docs/components/pipelines/overview/pipelines-overview/))
- [ ] Airflow ([official website](https://airflow.apache.org/))

_If you have developed a custom runtime outside of this repository and you believe it may be useful for the community, please open a PR linking to it here._


__Built-in Serializers__

- [x] AsJSON
- [x] AsPickle
- [ ] AsMessagePack ([format](https://msgpack.org/index.html))
- [ ] AsAvro ([format])(https://avro.apache.org/docs/current/)
- [ ] AsParquet (for Pandas DataFrames)
- [ ] AsCSV (for Pandas DataFrames)


_If you have developed a custom serializer outside of this repository and you believe it may be useful for the community, please open a PR linking to it here (or consider adding it to the main library)._


__Documentation__

- [x] README and project overview.
- [x] Contribution Guidelines.
- [ ] Web-based Documentation Portal.
- [ ] Curated examples covering beginner and advanced use cases.
- [ ] Quickstart guide.
- [ ] Tutorials.
- [ ] API reference.
- [ ] Core concepts:
    * [ ] DAGs and Tasks (Nodes)
    * [ ] Node Outputs
    * [ ] Node Inputs
    * [ ] Output and Node partitioning; Map-Reduce operations
    * [ ] Serializers
    * [ ] Runtimes
    * [ ] Imperative DSL




