import random

from dagger import dsl
from dagger.runtime.local import invoke


def coin_toss():
    return random.choice(["heads", "tails"])


@dsl.task()
def order(food_type):
    print(f"Ordering {food_type}!")


@dsl.DAG()
def my_pipeline():
    if coin_toss() == "heads":
        order("chinese")
    else:
        order("italian")
