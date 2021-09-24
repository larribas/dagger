import tracemalloc
tracemalloc.start()

import json


snapshot1 = tracemalloc.take_snapshot()

with open("smallfile.txt", "r") as f:
    value = f.read()

serialized_value = json.dumps(value)

snapshot2 = tracemalloc.take_snapshot()

stats = snapshot2.compare_to(snapshot1, 'lineno')

print(stats[0].size_diff)
