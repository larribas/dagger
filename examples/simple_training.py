
from dagger import dsl


@dsl.task()
def prepare_datasets(sample_size):
    return {"training": ["..."], "test": ["..."]}


@dsl.task()
def train_model(training_dataset):
    return "trained model"


@dsl.task()
def measure_model_performance(model, test_dataset):
    return "performance report"


@dsl.DAG()
def dag(sample_size):
    datasets = prepare_datasets(sample_size)
    model = train_model(datasets["training"])
    return measure_model_performance(model, datasets["test"])
