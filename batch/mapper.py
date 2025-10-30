#!/usr/bin/env python3
import sys

# Read each line from STDIN
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    # Split line into words
    words = line.split()
    for word in words:
        # Clean word of punctuation and lowercase it
        clean_word = word.strip('.,?!-:;()[]{}"\'').lower()
        if clean_word:
            # Output word and count 1, tab-separated
            print(f"{clean_word}\t1")
