"""
# ML Training Sequential.

This pipeline mocks a sequential ML model training composed of three simple tasks:

prepare_datasets -> train_model -> measure_model_performance
"""

from dagger import dsl


@dsl.task()
def prepare_datasets(sample_size):
    """ Mock the preparation of training and test datasets. """
    return {"training": ["..."], "test": ["..."]}


@dsl.task()
def train_model(training_dataset):
    """ Train the model on the training dataset. """
    return "trained model"


@dsl.task()
def measure_model_performance(model, test_dataset):
    """ Measure the model performance in the test dataset. """
    return "performance report"


@dsl.DAG()
def dag(sample_size):
    """ Define the DAG, i.e. the dependencies between the tasks. """
    datasets = prepare_datasets(sample_size)
    model = train_model(datasets["training"])
    return measure_model_performance(model, datasets["test"])
