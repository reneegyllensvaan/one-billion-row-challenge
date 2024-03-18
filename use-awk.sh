awk -F';' '
{
  counts[$1] += 1
  sums[$1] += $2
  if ($2 <= mins[$1]) {
    mins[$1] = $2
  }
  if ($2 >= maxs[$1]) {
    maxs[$1] = $2
  }
}

END {
  for (key in counts) {
    print key ": count=" counts[key] " sum=" sums[key] " min=" mins[key] " max=" maxs[key]
  }
}
' measurements.txt
