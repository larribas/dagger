from dagger import dsl


@dsl.task()
def prepare_datasets():
    return {
        "training": ["..."],
        "test": ["..."],
    }


@dsl.task()
def generate_training_combinations():
    return [
        {"model_type": "neural_network", "params": ["..."]},
        {"model_type": "boosted_tree", "params": ["..."]},
    ]


@dsl.task()
def train_model(training_dataset, parameters):
    return "trained model"


@dsl.task()
def choose_best_model(alternative_models, test_dataset):
    return "best_model"


@dsl.DAG()
def dag():
    datasets = prepare_datasets()
    alternative_models = []

    for training_parameters in generate_training_combinations():
        model = train_model(datasets["training"], training_parameters)
        alternative_models.append(model)

    best_model = choose_best_model(alternative_models, datasets["test"])

    return best_model
