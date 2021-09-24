import os
import tempfile

KB = 1024

with tempfile.TemporaryDirectory() as tmp:
    fname = os.path.join(tmp, "file")

    # Write a 1-GB file
    with open(fname, "w") as f:
        for i in range(KB**2):
            f.write("x" * KB)

