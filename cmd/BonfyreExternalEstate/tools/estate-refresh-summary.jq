# Estate refresh summary over report.json and density.json, per participation-law axes.
# Ported from estate-refresh-summary.mjs; byte-identical on the live estate vectors.
def lpad($n): tostring | (" " * ([$n - length, 0] | max)) + .;
def rpad($n): tostring | . + (" " * ([$n - length, 0] | max));
$report[0] as $r | $density[0] as $d | $r.total_occurrences as $t |
"estate: \($t) occurrences, \($r.owner_evidence_rows) owner evidence rows",
"  braid depth      \($r.braid_depth_distribution | tojson)",
"  owners           \($d.distinct_owners) distinct, \($d.owner_transition_count) transitions, longest path \($d.longest_owner_path)",
"  branch / rejoin  \($d.branch_points | tojson) / \($d.rejoin_points | tojson) (braid_excess \($d.braid_excess))",
(.axes[] as $a | "  \($a | rpad(12)) \($r.measured_axis_counts[$a] // 0 | lpad(6)) / \($t)"),
"  refresh fetched nothing; observation rows are receipts a sweep already earned",
"  every other axis is at the lowest rung its owner can defend locally"
