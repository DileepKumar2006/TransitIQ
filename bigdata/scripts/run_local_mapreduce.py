"""Run the same mapper/sort/reducer stages locally for classroom demonstration."""
import csv, subprocess, sys
from pathlib import Path

root = Path(__file__).resolve().parents[2]
source = Path(sys.argv[1]) if len(sys.argv) > 1 else root / "datasets/sample/transit_sample.csv"
mapper = root / "bigdata/mapreduce/route_passengers.py"
reducer = root / "bigdata/mapreduce/route_passengers_reduce.py"
with source.open(encoding="utf-8", newline="") as f:
    mapped = subprocess.run([sys.executable, str(mapper)], input=f.read(), text=True, capture_output=True, check=True).stdout
sorted_lines = "\n".join(sorted(mapped.splitlines(), key=lambda x:x.split("\t")[0])) + "\n"
result = subprocess.run([sys.executable, str(reducer)], input=sorted_lines, text=True, capture_output=True, check=True).stdout
print("route_id\tpassengers")
print(result, end="")
print("Execution mode: Local Streaming simulation (not Hadoop distributed execution)", file=sys.stderr)
