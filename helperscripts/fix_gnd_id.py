#!/usr/bin/env python3

import sys
import json

for line in sys.stdin:
    record = json.loads(line)
    record["_id"] = record["024"][0]["7_"][0]["a"]
    print(json.dumps(record))
