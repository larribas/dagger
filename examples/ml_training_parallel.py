"""
# ML Training Parallel.

This pipeline mocks a parallel ML model training composed of four tasks:

* prepare_datasets: gets a train and a test dataset.

* get_models: gets a dictionary with a mapping from model_name to model_specification.

* train_models: map task that trains each model in the dictionary.

* choose_best_model: reduce task that evaluates each model and selects the best on the
    testing dataset.

"""

from dagger import dsl


@dsl.task()
def prepare_datasets():
    """ Mock preparation of training and test datasets. """
    return {"training": ["..."], "test": ["..."]}


@dsl.task()
def get_models():
    """ Mocks definition of models to be trained and compared """
    return {"neural_network": ["..."], "boosted_tree": ["..."]}


@dsl.task()
def train_model(training_dataset, model):
    """ Mocks training of a model in a training dataset """
    return "trained model"


@dsl.task()
def choose_best_model(alternative_models, test_dataset):
    """ Mocks evaluation of model on a test dataset """
    return "best_model"


@dsl.DAG()
def training_workflow():
    """Defines the dependencies in between the tasks, in particular defines the
    map-reduce operation in a single line with a python list comprehension operation."""
    data = prepare_datasets()
    trained_models = [train_model(data["training"], model) for model in get_models()]
    return choose_best_model(trained_models, data["test"])


if __name__ == "__main__":
    """Defines a local interface for this DAG, using the local runtime. Check the
    documentation to understand why this is relevant or necessary."""
    from dagger.runtime.local import invoke

    dag = dsl.build(training_workflow)
    print(invoke(dag))
