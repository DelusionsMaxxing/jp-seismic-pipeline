## What changed

<!-- One or two sentences. The issue holds the context; this holds the diff's story. -->

Closes #

## What I ran

<!-- Real commands and real outcomes. "CI will tell us" is not an answer. -->

- [ ] `ruff format . && ruff check --fix . && pytest -q`
- [ ] `dbt build --target dev`
- [ ] Incremental models verified against `--full-refresh`
- [ ] DAG imports cleanly (`python dags/<file>.py`)

## What I did not verify

<!-- Say it plainly. An unverified area named here is cheaper than one found in prod. -->

## Data impact

- [ ] Grain of every changed model is unchanged, or the change is stated above
- [ ] Re-running this on an overlapping window is still safe
- [ ] Monitoring still reflects reality (new metric, panel or alert added if needed)
