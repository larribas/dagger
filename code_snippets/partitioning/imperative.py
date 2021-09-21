from typing import List

from dagger import dsl

DataSet = str


#
# DAG that processes and transforms a dataset
# ===========================================
#
@dsl.task()
def encode_field_a(dataset: DataSet) -> DataSet:
    return f"{dataset}, with field a encoded"


@dsl.task()
def aggregate_fields_b_and_c(dataset: DataSet) -> DataSet:
    return f"{dataset}, with fields b and c aggregated"


@dsl.task()
def calculate_moving_average_for_d(dataset: DataSet) -> DataSet:
    return f"{dataset}, with moving average for d calculated"


@dsl.DAG()
def transform_dataset(dataset: DataSet):
    ds_1 = encode_field_a(dataset)
    ds_2 = aggregate_fields_b_and_c(ds_1)
    return calculate_moving_average_for_d(ds_2)


#
# DAG that splits a large dataset into chunks
# and invokes the previous DAG for each chunk
# ===========================================
#
@dsl.task()
def retrieve_dataset() -> DataSet:
    return "original dataset"


@dsl.task()
def split_dataset_into_chunks(dataset: DataSet) -> List[DataSet]:
    return [f"{dataset} (chunk {i})" for i in range(3)]


@dsl.DAG()
def dag():
    dataset = retrieve_dataset()
    chunks = split_dataset_into_chunks(dataset)

    for chunk in chunks:
        transform_dataset(chunk)
