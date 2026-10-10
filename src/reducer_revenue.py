#!/usr/bin/env python3
import sys
current_key = None
current_sum = 0
for line in sys.stdin:
    key, value = line.strip().split("	")
    value = int(value)
    if current_key == key:
        current_sum += value
    else:
        if current_key is not None:
            print(f"{current_key}	{current_sum}")
        current_key = key
        current_sum = value
if current_key is not None:
    print(f"{current_key}	{current_sum}")
