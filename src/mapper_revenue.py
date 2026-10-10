#!/usr/bin/env python3
import sys, csv
reader = csv.DictReader(sys.stdin)
for row in reader:
    if row.get("status") == "SUCCESS":
        category = row["category"]
        amount = int(row["amount"])
        print(f"{category}	{amount}")
