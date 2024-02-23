# this solution is my "how slow can you go" ceiling. this linewise python
# implementation on a single thread took about 22 minutes on my machine.

from collections import defaultdict

counts = defaultdict(int)
sums = defaultdict(float)
mins = dict()
maxs = dict()

with open('./measurements.txt', 'r') as f:
    for line in f.readlines():
        loc, amount = line.split(';')
        amount = float(amount)
        counts[loc] += 1
        sums[loc] += amount

        if loc in mins:
            mins[loc] = min(mins[loc], amount)
        else:
            mins[loc] = amount

        if loc in maxs:
            maxs[loc] = max(maxs[loc], amount)
        else:
            maxs[loc] = amount

print(f"count = {sum(counts.values())}")

# print(f"counts = {counts}")
# print(f"sums = {sums}")
# print(f"mins = {mins}")
# print(f"maxs = {maxs}")
