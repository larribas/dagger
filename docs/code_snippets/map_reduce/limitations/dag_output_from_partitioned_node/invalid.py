from dagger import dsl


@dsl.task()
def retrieve_dataset():
    return "dataset"


@dsl.task()
def split_dataset_into_chunks(dataset):
    return ["chunk1", "chunk2"]


@dsl.task()
def process_chunk(chunk):
    return f"processed {chunk}"


@dsl.DAG()
def dag():
    dataset = retrieve_dataset()
    return [process_chunk(chunk) for chunk in split_dataset_into_chunks(dataset)]
