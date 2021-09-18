from typing import List

from dagger import dsl


@dsl.task()
def do_something_for(country: str):
    pass


@dsl.DAG()
def dag(countries: List[str]):
    for country in countries:
        do_something_for(country)
