# Demo utilities

These scripts build the local DataHub world used in the FlowJury demonstration.
They are setup tools, not part of the production assessment runtime.

Run them from the repository root, in this order:

```bash
python -m scripts.seed_demo_catalog
python -m scripts.add_demo_dag_sources
python -m scripts.seed_demo_memory
```

`seed_demo_catalog` now includes every DAG-source clue directly. The middle command is retained as
an idempotent repair step for demo catalogs created by older versions, and is safe to rerun.
