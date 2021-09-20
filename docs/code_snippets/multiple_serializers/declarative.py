from dagger import AsPickle, FromKey, Task


class CustomType:
    pass


def generate_multiple_outputs():
    return {
        "a_boolean": True,
        "a_custom_object": CustomType(),
    }


task = Task(
    generate_multiple_outputs,
    outputs={
        "boolean": FromKey("a_boolean"),
        "custom_obj": FromKey("a_custom_object", serializer=AsPickle()),
    },
)
