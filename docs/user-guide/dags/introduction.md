# Modeling Data Pipelines

_Dagger_'s purpose is to allow you to define data pipelines in Python, and run them on production-grade distributed systems such as [Argo Workflows](https://argoproj.github.io/workflows) or [Apache Airflow](https://airflow.apache.org/).

This section of the User Guide focuses on the first part: How to model the behavior of your pipelines.


## DAGs

Pipelines in _Dagger_ are defined through a Directed Acyclic Graph (a DAG). DAGs are collections of nodes connected to each other through their inputs and outputs.

[![map reduce execution in Argo Workflows](../../assets/images/argo/map_reduce.png)](../../assets/images/argo/map_reduce.png)
<p align="center"><em>The execution of a DAG represented by Argo Workflows.</em></p>


## Data Structures

_Dagger_ uses a series of immutable data structures to represent DAGs. These structures are responsible for:

- Exposing all the relevant information to runtimes.
- Verifying all the pieces of a DAG fit together.


The following diagram shows the different groups of data structures _(click on the image to make it bigger)_:

[![core data structures](../../assets/images/diagrams/core_data_structures.png)](../../assets/images/diagrams/core_data_structures.png)


The rest of the guide will explain each of the components in depth. Let's start with _Dagger_'s basic building block: The [Task](tasks.md).

