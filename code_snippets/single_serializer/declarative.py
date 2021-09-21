from dagger import AsPickle, FromReturnValue, Task


class CustomType:
    pass


def generate_object():
    return CustomType()


task = Task(
    generate_object,
    outputs={
        "object": FromReturnValue(serializer=AsPickle()),
    },
)
