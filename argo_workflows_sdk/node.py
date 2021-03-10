from typing import Callable


class Node:
    def __init__(
        self,
        name: str,
        func: Callable,
    ):
        self.name = name
        self.func = func

    def __call__(self):
        return self.func()
