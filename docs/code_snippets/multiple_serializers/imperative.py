from dagger import AsJSON, AsPickle, dsl


class CustomType:
    pass


@dsl.task(
    serializer=dsl.Serialize(
        a_boolean=AsJSON(),
        a_custom_object=AsPickle(),
    )
)
def generate_multiple_outputs():
    return {
        "a_boolean": True,
        "a_custom_object": CustomType(),
    }
