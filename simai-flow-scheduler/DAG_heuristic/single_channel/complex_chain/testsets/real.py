"""LLM-structure-derived DAG motifs and exported DAG JSON loader."""

from __future__ import annotations

import json
from pathlib import Path

from DAG_heuristic.common.benchmark import dag_from_json, llm_motif_benchmarks
from DAG_heuristic.common.interface import TestCase


def cases() -> list[TestCase]:
    return [
        TestCase(dag.name, "real_derived", dag) for dag in llm_motif_benchmarks()
    ]


def load_export(path: Path) -> list[TestCase]:
    """Load one DAG object or a list of DAG objects exported by the simulator."""

    payload = json.loads(path.read_text(encoding="utf-8"))
    values = payload if isinstance(payload, list) else [payload]
    dags = [dag_from_json(value) for value in values]
    return [TestCase(dag.name, "real_export", dag) for dag in dags]

