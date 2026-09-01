# Repository Agent Instructions

- Work directly in the primary checkout for this repository so the running Overwatch service sees live-reload edits.
- Do not create or use a Git worktree unless the user explicitly requests one.
- Keep recurring report refreshes fast: do not run a CLI subprocess or scan logs once per resource. Prefer structured SDK fields and batched cloud APIs such as CloudWatch; keep expensive per-resource log retrieval behind an explicit on-demand action.
- Do not infer structured status from logs when the provider exposes an authoritative field. Preserve known aggregate values and report unavailable breakdowns as unknown.
