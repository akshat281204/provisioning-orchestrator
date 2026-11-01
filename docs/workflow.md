# How this repository is built

Every phase in [`plan/`](../plan/) is implemented by an autonomous agent loop and lands as
its own pull request against `main`. Nobody hand-writes the phases; a planner produces the
plan, a builder implements one phase at a time, and a reviewer gates each one.

The loop is the [`stackflow`](https://github.com/akshat281204/skills/tree/main/stackflow)
skill. This document explains the conventions it relies on, so the repository stays
mergeable.

## The stack

```
main   ●───●─────────────────────────────●────────────────────●
            \                           /                    /
             a1──a2──a3   phase/1      /                    /
                        \             /                    /
                         b1──b2──b3  phase/2 ──────────────┘
                                    \
                                     c1──c2  phase/3 ...
```

Phase N branches off phase N−1, not off `main`. Every PR still targets `main`. GitHub diffs
against the merge-base, so while PR 1 is open, PR 2 shows phases 1 and 2; the moment PR 1
merges, PR 2 collapses to just its own commits.

## Why merge commits are mandatory

Merging in order is conflict-free **only** if `main` keeps the original commit objects.
A squash merge rewrites phase 1's commits into a new one, `a1..a3` never land on `main`,
the merge-base goes stale, and every downstream PR re-applies work that is already there.

- Merge commits: on
- Squash merging: off
- Rebase merging: off

`stack doctor` checks this and prints the `gh api` command to fix it.

If you ever do need to rewrite a merged-adjacent branch, run `stack sync` — it rebases
each phase onto the new parent tip using the base SHAs recorded in `.stack/state.json`.

## Merge order is enforced, not requested

`.github/workflows/stack-order.yml` fails PR k+1 while any lower-numbered phase PR is still
open, and re-evaluates every open phase PR whenever something lands on `main`, so the check
does not go stale after a legitimate merge.

PR titles must keep the `Phase N: <title>` form — the workflow parses it.

## What a phase must satisfy

- `make verify` is green. That is `gofmt`, `go vet`, `go test -race`, and a build.
- The repository is green **after this phase alone**. If phase 3 only compiles once phase 4
  lands, the plan is mis-sliced.
- Only files inside the phase's `Touches` list change, unless the spec says otherwise.
- Tests needing Postgres, Vault, or Docker sit behind `//go:build integration` so they
  never run in `make verify`.

## Running it

```bash
stack doctor                     # repo settings, auth, plan validity
stack validate                   # check plan/ shape
stack run                        # implement every phase, one PR each
stack run --from 4               # resume
stack status                     # what exists, what is open
stack sync --re-review           # after a branch is rewritten
```

## Failure modes

- **Out-of-order merge** — commits land on `main` in the wrong order. Blocked by the check.
- **Squash merge** — silently breaks the stack. Blocked by repo settings.
- **Mis-sliced plan** — each phase is fine alone but the sequence is not. No tool catches
  this; read the plan before running.
- **Same-family models** — review becomes theatre. Builder and reviewer are configured to
  different model families.
