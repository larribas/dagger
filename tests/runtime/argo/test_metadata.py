from dagger.runtime.argo.metadata import Metadata, object_meta


def test__metadata__representation():
    annotations = {"my": "annotation"}
    labels = {"my": "label"}
    metadata = Metadata(
        name="my-name",
        generate_name_from_prefix=True,
        namespace="my-namespace",
        annotations=annotations,
        labels=labels,
    )
    assert (
        repr(metadata)
        == f"Metadata(name=my-name, generate_name_from_prefix=True, namespace=my-namespace, annotations={repr(annotations)}, labels={repr(labels)})"
    )


def test__metadata__eq():
    name = "my-name"
    namespace = "my-namespace"
    annotations = {"my": "annotation"}
    labels = {"my": "label"}
    metadata = Metadata(
        name=name,
        generate_name_from_prefix=True,
        namespace=namespace,
        annotations=annotations,
        labels=labels,
    )

    assert metadata != Metadata(name=name)
    assert metadata == Metadata(
        name=name,
        generate_name_from_prefix=True,
        namespace=namespace,
        annotations=annotations,
        labels=labels,
    )


def test__object_meta__only_with_name():
    metadata = Metadata(name="my-name")
    assert object_meta(metadata) == {"name": metadata.name}


def test__object_meta__generates_name_from_prefix():
    metadata = Metadata(name="my-name", generate_name_from_prefix=True)
    assert object_meta(metadata) == {"generateName": metadata.name}


def test__object_meta__with_namespace():
    metadata = Metadata(name="my-name", namespace="my-namespace")
    assert object_meta(metadata) == {
        "name": metadata.name,
        "namespace": metadata.namespace,
    }


def test__object_meta__with_labels():
    metadata = Metadata(
        name="my-name",
        labels={
            "my.label/one": "1",
            "my.label/two": "2",
        },
    )
    assert object_meta(metadata) == {
        "name": metadata.name,
        "labels": metadata.labels,
    }


def test__object_meta__with_annotations():
    metadata = Metadata(
        name="my-name",
        annotations={
            "my.annotation/one": "1",
            "my.annotation/two": "2",
        },
    )
    assert object_meta(metadata) == {
        "name": metadata.name,
        "annotations": metadata.annotations,
    }
