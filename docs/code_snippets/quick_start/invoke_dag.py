from dagger.runtime.local import invoke

from .build_dag import dag

result = invoke(dag, params={"seed": 1, "exponent": 2})
print(f"The final result was {result}")
