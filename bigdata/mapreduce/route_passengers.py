"""Hadoop Streaming mapper/reducer. Use Python 3 as the Streaming executable."""
import sys

if __name__ == "__main__":
    for line in sys.stdin:
        fields = line.rstrip("\n").split(",")
        if fields[0].lower() == "date" or len(fields) < 7:
            continue
        try:
            print(f"{fields[2]}\t{int(float(fields[6]))}")
        except ValueError:
            continue
