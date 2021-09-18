from dagger import DAG, FromKey, FromNodeOutput, FromParam, FromReturnValue, Task


def prepare_datasets(sample_size):
    return {
        "training": ["..."],
        "test": ["..."],
    }


def train_model(training_dataset):
    return "trained model"


def measure_model_performance(model, test_dataset):
    return "performance report"


dag = DAG(
    inputs={
        "sample_size": FromParam(),
    },
    outputs={
        "performance_report": FromNodeOutput(
            "measure-model-performance", "performance_report"
        ),
    },
    nodes={
        "prepare-datasets": Task(
            prepare_datasets,
            inputs={
                "sample_size": FromParam("sample_size"),
            },
            outputs={
                "training_dataset": FromKey("training"),
                "test_dataset": FromKey("test"),
            },
        ),
        "train-model": Task(
            train_model,
            inputs={
                "training_dataset": FromNodeOutput(
                    "prepare-datasets", "training_dataset"
                ),
            },
            outputs={
                "model": FromReturnValue(),
            },
        ),
        "measure-model-performance": Task(
            measure_model_performance,
            inputs={
                "model": FromNodeOutput("train-model", "model"),
                "test_dataset": FromNodeOutput("prepare-datasets", "test_dataset"),
            },
            outputs={
                "performance_report": FromReturnValue(),
            },
        ),
    },
)
