"""Static checks on the provisioned Grafana dashboards (the queries themselves
run against Trino, which CI doesn't start)."""

import json
import re
from pathlib import Path

import pytest

DASHBOARDS = sorted(Path(__file__).parent.parent.glob("infra/grafana/dashboards/*.json"))


def targets(dashboard):
    for p in dashboard["panels"]:
        for t in p["targets"]:
            yield p, t


def test_dashboards_exist():
    assert {p.name for p in DASHBOARDS} == {"market.json", "pipeline_health.json"}


@pytest.mark.parametrize("path", DASHBOARDS, ids=lambda p: p.stem)
def test_panels_query_trino_with_qualified_tables(path):
    d = json.loads(path.read_text())
    ids = [p["id"] for p in d["panels"]]
    assert len(ids) == len(set(ids))
    for panel, t in targets(d):
        assert t["datasource"]["uid"] == "trino", panel["title"]
        # The data source has no default catalog: every table must be lake.<schema>.<table>.
        for table in re.findall(r"\bFROM\s+([\w.\"$]+)", t["rawSQL"], re.IGNORECASE):
            assert table.startswith("lake."), (panel["title"], table)
