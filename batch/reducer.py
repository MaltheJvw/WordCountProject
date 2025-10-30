#!/usr/bin/env python3

import sys

current_word = None
current_count = 0

# Read each line from STDIN
for line in sys.stdin:
    line = line.strip()
    
    # Skip empty lines
    if not line:
        continue

    # Expect tab-separated key-value
    parts = line.split('\t', 1)
    if len(parts) != 2:
        # Malformed line, skip
        continue

    word, count_str = parts

    # Convert count to integer
    try:
        count = int(count_str)
    except ValueError:
        continue

    # Aggregate counts
    if current_word == word:
        current_count += count
    else:
        if current_word is not None:
            # Output previous word and its count
            print(f"{current_word}\t{current_count}")
        current_word = word
        current_count = count

# Output the last word
if current_word is not None:
    print(f"{current_word}\t{current_count}")
