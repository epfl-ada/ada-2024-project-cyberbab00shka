import json
import sys


def is_cell_valid(cell):
    source = cell.get("source", [])
    if len(source) == 0:
        return True
    return source[0].strip().lower().find("# ignore") != 0 and source[0].strip().lower().find("#ignore") != 0


files = sys.argv[1:]

result = {
    "cells": [],
    "metadata": {
        "kernelspec": {
            "display_name": "Python 3 (ipykernel)",
            "language": "python",
            "name": "python3",
        },
        "language_info": {
            "codemirror_mode": {"name": "ipython", "version": 3},
            "file_extension": ".py",
            "mimetype": "text/x-python",
            "name": "python",
            "nbconvert_exporter": "python",
            "pygments_lexer": "ipython3",
            "version": "3.11.10",
        },
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}

for file in files:
    with open(file) as file:
        notebook = json.load(file)
    result["cells"] += notebook["cells"]
    result["metadata"] |= notebook["metadata"]

result["cells"] = list(filter(is_cell_valid, result["cells"]))
print(json.dumps(result, indent=2), end="")
