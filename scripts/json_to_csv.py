#!/usr/bin/env python3

import pandas as pd
import sys


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"{sys.argv[0]} <input.json> <output.csv>")
        sys.exit(1)

    df = pd.read_json(sys.argv[1])
    df.to_csv(sys.argv[2], index=False)

