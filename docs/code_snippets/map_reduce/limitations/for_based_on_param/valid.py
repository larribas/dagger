from typing import List

from dagger import dsl


@dsl.task()
def identity(x):
    return x


@dsl.task()
def do_something_for(country: str):
    pass


@dsl.DAG()
def dag(countries: List[str]):
    countries_ = identity(countries)

    for country in countries_:
        do_something_for(country)
