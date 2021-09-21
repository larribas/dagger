from dagger.runtime.local import invoke
from docs.code_snippets.task.declarative import task
from docs.code_snippets.task.imperative import hello


def test_declarative_and_imperative_are_equivalent():
    name = "world"
    assert task.func(name) == hello.func(name)


def test_invocation():
    invoke(task, params={"name": "docs"}) == {"hello_message": b"Hello docs!"}
