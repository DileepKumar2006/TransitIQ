"""Hadoop Streaming reducer: sum passenger counts by route."""
import sys

route = None
total = 0
for line in sys.stdin:
    key, value = line.rstrip("\n").split("\t", 1)
    if route is not None and route != key:
        print(f"{route}\t{total}")
        total = 0
    route = key
    total += int(value)
if route is not None:
    print(f"{route}\t{total}")
