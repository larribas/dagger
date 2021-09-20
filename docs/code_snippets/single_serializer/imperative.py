from dagger import AsPickle, dsl


class CustomType:
    pass


@dsl.task(serializer=dsl.Serialize(AsPickle()))
def generate_object():
    return CustomType()
