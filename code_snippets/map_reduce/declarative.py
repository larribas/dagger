from dagger import DAG, FromKey, FromNodeOutput, FromReturnValue, Task


def prepare_datasets():
    return {
        "training": ["..."],
        "test": ["..."],
    }


def generate_training_combinations():
    return [
        {"model_type": "neural_network", "params": ["..."]},
        {"model_type": "boosted_tree", "params": ["..."]},
    ]


def train_model(training_dataset, parameters):
    return "trained model"


def choose_best_model(alternative_models, test_dataset):
    return "best_model"


dag = DAG(
    nodes={
        "prepare-datasets": Task(
            prepare_datasets,
            outputs={
                "training": FromKey("training"),
                "test": FromKey("test"),
            },
        ),
        "generate-training-combinations": Task(
            generate_training_combinations,
            outputs={
                "combinations": FromReturnValue(),
            },
        ),
        "train-model": Task(
            train_model,
            inputs={
                "training_dataset": FromNodeOutput("prepare-datasets", "training"),
                "parameters": FromNodeOutput(
                    "generate-training-combinations", "combinations"
                ),
            },
            outputs={
                "model": FromReturnValue(),
            },
            partition_by_input="parameters",
        ),
        "choose-best-model": Task(
            choose_best_model,
            inputs={
                "alternative_models": FromNodeOutput("train-model", "model"),
                "test_dataset": FromNodeOutput("prepare-datasets", "test"),
            },
            outputs={
                "best_model": FromReturnValue(),
            },
        ),
    },
    outputs={
        "best_model": FromNodeOutput("choose-best-model", "best_model"),
    },
)
