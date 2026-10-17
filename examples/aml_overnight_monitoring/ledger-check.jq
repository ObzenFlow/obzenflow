# jq -nr -f ledger-check.jq <run>/run_manifest.json... <ledger>...
# Give the manifests of every run in the lineage, in any order, then the ledgers.
def decided: .result == "scored" or .result == "opened";
[inputs | {file: input_filename, value: .}] as $inputs
| ($inputs | map(select(.value | has("journal_schema_version")) | .value)) as $manifests
| if ($manifests | length) == 0 then
    "error: no run manifest given. List the lineage's run_manifest.json files before the ledgers.\n" | halt_error(1)
  else . end
| ($manifests | map(.recorded_flow_id // .flow_id) | unique) as $firsts
| if ($firsts | length) > 1 then
    "error: the manifests belong to \($firsts | length) lineages, started by \($firsts | join(" and ")). List the runs of one lineage.\n" | halt_error(1)
  else . end
| $firsts[0] as $first
| ($manifests | map(.flow_id) + [$first] | unique) as $lineage
| ($manifests | map(select(.resume != null)) | length) as $resumes
| $inputs
| map(select(.value | has("key")))
| group_by(.file)[]
| .[0].file as $ledger
| map(.value) as $calls
| ($calls | map(select(.flow_id | IN($lineage[]) | not)) | group_by(.flow_id) | first // []) as $foreign
| ($calls | group_by(.key) | map(select(map(select(decided)) | length > 1)) | first) as $twice
| ($calls | map(select(.flow_id != $first and .result == "replayed")) | length) as $repeats
| if ($foreign | length) > 0 then
    "error: \($ledger) holds \($foreign | length) calls from \($foreign[0].flow_id), which is not part of the lineage started by \($first). List every run of the lineage. A fresh run starts with an empty ledger.\n" | halt_error(1)
  elif ($calls | any(.flow_id == $first) | not) then
    "error: \($ledger) holds no calls from \($first), which started this lineage. A resume never clears the ledger.\n" | halt_error(1)
  elif $twice != null then
    "error: \($twice[0].key) was decided twice in this lineage (\($twice | map(select(decided) | .flow_id) | join(", "))).\n" | halt_error(1)
  elif $repeats > $resumes then
    "error: \($ledger) holds \($repeats) calls repeated by resumes; each resume may repeat only the call its kill cut off.\n" | halt_error(1)
  else
    "ok: \($ledger) holds \($calls | length) calls from this lineage, \($repeats) repeated by a resume."
  end
