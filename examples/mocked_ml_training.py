from dagger import dsl


@dsl.task()
def prepare_datasets():
    return {"training": ["..."], "test": ["..."]}


@dsl.task()
def get_models():
    return {"neural_network": ["..."], "boosted_tree": ["..."]}


@dsl.task()
def train_model(training_dataset, model):
    return "trained model"


@dsl.task()
def choose_best_model(alternative_models, test_dataset):
    return "best_model"


@dsl.DAG()
def training_workflow():
    data = prepare_datasets()
    trained_models = [train_model(data["training"], model) for model in get_models()]
    return choose_best_model(trained_models, data["test"])


if __name__ == "__main__":
    from dagger.runtime.local import invoke

    dag = dsl.build(training_workflow)
    print(invoke(dag))
