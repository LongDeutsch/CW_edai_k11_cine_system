"""Payload governance, lọc dataset, lineage và an toàn chỉ đọc."""

from __future__ import annotations

import subprocess
import unittest
from pathlib import Path

from governance.inventory import datasets, edges, schema_of
from governance.serve import build_view, filter_datasets
from serving.dashboard.server import ALLOWED

REPO = Path(__file__).resolve().parents[2]


def _raw(**overrides) -> dict:
    base = {
        "generated_at": "2026-10-23T00:00:00+00:00",
        "git_head": "abc",
        "streaming_enabled": False,
        "iceberg": {"status": "unavailable", "error": "down", "tables": None},
        "airflow": {
            "status": "unavailable",
            "error": "down",
            "paused": None,
            "summary": {"latest_state": "success", "production_success_implies_current_data": True},
        },
        "clickhouse": {"status": "unavailable", "error": "down", "production_rows": 0},
        "flink": {"status": "unavailable", "error": "down", "jobs_running": 0, "jobs_cancelled": 1},
        "marker": {"status": "unavailable", "error": "down", "payload": {"published": True}},
        "integration_names": None,
        "integration_error": "down",
        "health": {"status": "PASS"},
        "historical_proof_used_as_current_health": True,
    }
    base.update(overrides)
    return base


def _row(view: dict, dataset_id: str) -> dict:
    return next(item for item in view["datasets"] if item["id"] == dataset_id)


class GovernanceViewTests(unittest.TestCase):
    def test_unread_catalog_is_unknown_not_empty(self):
        view = build_view(_raw())
        self.assertFalse(view["historical_proof_used_as_current_health"])
        self.assertFalse(view["health"]["success_inferred_from_snapshot"])
        self.assertIsNone(view["health"]["airflow"])
        self.assertEqual(view["health"]["serving"]["status"], "unavailable")
        self.assertIsNone(view["health"]["marker"]["payload"])
        self.assertNotIn("jobs_cancelled", view["health"]["flink"])
        self.assertIsNone(view["drift"]["findings"])
        self.assertIsNone(view["integration"]["count"])
        self.assertEqual(len(view["datasets"]), len(datasets()))
        ratings = _row(view, "iceberg.gold.fact_ratings")
        self.assertEqual(ratings["status"], "unknown")
        self.assertEqual(ratings["presence"], "unknown")
        self.assertFalse(ratings["failure"])
        self.assertEqual(ratings["impact"]["effect"], "unknown")
        playback = _row(view, "iceberg.silver.stg_playback_windows")
        self.assertEqual(playback["status"], "unknown")
        self.assertEqual(playback["pipeline"], "disabled")
        self.assertNotEqual(playback["status"], "expected_absent")

    def test_lineage_edges_match_inventory_evidence(self):
        view = build_view(_raw())
        payload = {(item["upstream"], item["downstream"], item["path"], item["token"]) for item in view["edges"]}
        inventory = {(edge.upstream, edge.downstream, edge.evidence[0], edge.evidence[1]) for edge in edges()}
        self.assertEqual(payload, inventory)
        kinds = {item["kind"] for item in view["edges"]}
        self.assertEqual(len(view["edges"]), len({(edge.upstream, edge.downstream, edge.evidence[0], edge.evidence[1]) for edge in edges()}))
        self.assertTrue(kinds <= {"data_flow", "control_dependency", "observability", "api_consumption"})
        by_key = {(item["upstream"], item["downstream"], item["token"]): item["kind"] for item in view["edges"]}
        self.assertEqual(by_key[("iceberg.gold.fact_ratings", "minio.orchestration_marker", "fact_ratings")], "control_dependency")
        self.assertEqual(by_key[("minio.orchestration_marker", "clickhouse.movie_ratings_stage", "read_marker")], "control_dependency")
        self.assertEqual(by_key[("iceberg.gold.fact_ratings", "clickhouse.movie_ratings_stage", "_rating_rows")], "data_flow")
        self.assertEqual(by_key[("iceberg.gold.dim_movies", "clickhouse.movie_ratings_stage", "dim_movies")], "data_flow")
        self.assertEqual(by_key[("clickhouse.movie_ratings", "clickhouse.sync_log", "sync_log")], "observability")
        self.assertEqual(by_key[("clickhouse.movie_ratings", "api.ready", "movie_ratings")], "observability")
        self.assertEqual(by_key[("clickhouse.movie_ratings", "api.serving.status", "serving/status")], "observability")
        self.assertEqual(by_key[("clickhouse.movie_ratings", "api.movies.top_rated", "movie_ratings")], "api_consumption")
        self.assertEqual(by_key[("iceberg.bronze.raw_ratings", "iceberg.bronze.quality_results", "quality_results")], "observability")
        self.assertEqual(by_key[("iceberg.bronze.raw_ratings", "iceberg.bronze.quarantine_records", "quarantine_records")], "data_flow")
        self.assertFalse(
            any(
                item["upstream"] == "iceberg.gold.fact_watch_sessions"
                and item["downstream"] == "iceberg.gold.obt_movie_engagement"
                for item in view["edges"]
            )
        )
        ratings = _row(view, "iceberg.silver.stg_ratings")
        self.assertIn("iceberg.gold.fact_ratings", ratings["descendants"])
        self.assertIn("api.movies.top_rated", ratings["descendants"])
        self.assertNotIn("clickhouse.movie_windows", ratings["descendants"])
        self.assertIn("iceberg.gold.feat_user_movie_stats_daily", ratings["descendants"])
        self.assertIn("iceberg.gold.feat_movie_engagement_stats_daily", ratings["descendants"])
        watch = _row(view, "iceberg.gold.fact_watch_sessions")
        self.assertEqual(watch["downstream"], [])
        features = {
            "iceberg.gold.feat_user_movie_stats_daily",
            "iceberg.gold.feat_movie_engagement_stats_daily",
        }
        linked = {item["upstream"] for item in view["edges"]} | {item["downstream"] for item in view["edges"]}
        self.assertTrue(features <= linked)
        self.assertEqual(by_key[("iceberg.silver.stg_ratings", "iceberg.gold.feat_user_movie_stats_daily", "user_total_ratings_90d")], "data_flow")

    def test_filter_separates_environment_and_status(self):
        view = build_view(_raw())
        integration = filter_datasets(view["datasets"], environment="integration")
        self.assertEqual([item["id"] for item in integration], ["integration.resources"])
        matched = filter_datasets(view["datasets"], q="fact_ratings")
        self.assertIn("iceberg.gold.fact_ratings", [item["id"] for item in matched])
        self.assertTrue(all("fact_ratings" in item["id"] or "fact_ratings" in item["name"] for item in matched))
        unknown = filter_datasets(view["datasets"], status="unknown", zone="gold")
        self.assertIn("iceberg.gold.fact_ratings", [item["id"] for item in unknown])
        self.assertTrue(all(item["status"] == "unknown" for item in unknown))

    def test_live_drift_keeps_source_evidence_and_disabled_streaming(self):
        dataset = next(item for item in datasets() if item.id == "iceberg.silver.stg_ratings")
        fields = [{"name": name, "type": "string", "required": False} for name, _type in schema_of(dataset)]
        fields.append({"name": "dt", "type": "date", "required": False})
        view = build_view(
            _raw(
                iceberg={
                    "status": "read",
                    "error": None,
                    "tables": {
                        "silver.stg_ratings": {
                            "catalog": "present",
                            "snapshot_id": 8795824390230744395,
                            "total_records": 57665,
                            "committed_at": "2026-10-16T15:45:17+00:00",
                            "fields": fields,
                            "partition_fields": [],
                        }
                    },
                },
                marker={"status": "absent", "error": None, "payload": None},
                integration_names=["it_example"],
                integration_referenced=[],
            )
        )
        ratings = _row(view, "iceberg.silver.stg_ratings")
        self.assertEqual(ratings["snapshot_id"], "8795824390230744395")
        self.assertEqual(ratings["presence"], "physical")
        drift = next(item for item in view["drift"]["findings"] if item["table"] == "iceberg.silver.stg_ratings")
        self.assertEqual(drift["physical_source"], "iceberg-rest")
        self.assertTrue(str(drift["expected_source"]).endswith("stg_tables.sql"))
        extra = next(item for item in drift["findings"] if item["column"] == "dt")
        self.assertEqual(extra["kind"], "extra")
        self.assertFalse(drift["partition"]["repartition"])
        self.assertIn("event_date", drift["partition"]["missing_partition_fields"])
        playback = _row(view, "iceberg.silver.stg_playback_windows")
        self.assertEqual(playback["status"], "expected_absent")
        self.assertEqual(playback["pipeline"], "disabled")
        self.assertEqual(playback["impact"]["effect"], "disabled_path")
        extra_table = _row(view, "integration.silver.it_example")
        self.assertEqual(extra_table["environment"], "integration")
        self.assertFalse(extra_table["declared_in_code"])
        self.assertEqual(extra_table["downstream"], [])
        self.assertEqual(view["integration"]["count"], 1)
        self.assertEqual(view["health"]["freshness"]["batch"], "marker_unread")
        self.assertEqual(view["health"]["freshness"]["streaming"], "disabled")

    def test_serving_unknown_when_task_list_is_unread(self):
        view = build_view(
            _raw(
                clickhouse={
                    "status": "read",
                    "error": None,
                    "production_rows": 0,
                    "engagement_versions": 0,
                    "rating_versions": 0,
                    "same_batch": None,
                }
            )
        )
        self.assertEqual(view["health"]["serving"]["status"], "unknown")

    def test_serving_not_run_comes_from_current_tasks(self):
        view = build_view(
            _raw(
                airflow={
                    "status": "read",
                    "error": None,
                    "paused": True,
                    "summary": {
                        "latest_state": "success",
                        "latest_is_production": False,
                        "production_tasks": ["publish_success"],
                        "production_success_implies_current_data": True,
                    },
                },
                clickhouse={
                    "status": "read",
                    "error": None,
                    "production_rows": 0,
                    "engagement_versions": 0,
                    "rating_versions": 0,
                    "same_batch": None,
                },
            )
        )
        self.assertEqual(view["health"]["serving"]["status"], "not_run")
        self.assertFalse(view["health"]["airflow"]["production_success_implies_current_data"])
        self.assertFalse(view["health"]["airflow"]["latest_is_production"])
        self.assertEqual(_row(view, "airflow.cinestream_daily_batch_lakehouse")["pipeline"], "disabled")

    def test_modules_do_not_write_or_load_historical_proof(self):
        for relative in ("governance/serve.py", "governance/read_live.py", "serving/api/governance_routes.py"):
            text = (REPO / relative).read_text()
            for banned in ("write_text", "phase5a", "phase5b", "phase5c", "phase5d", "ALTER TABLE", "DROP TABLE"):
                self.assertNotIn(banned, text)
        self.assertIn("governance/", ALLOWED)
        page = (REPO / "serving/dashboard/static/governance.html").read_text()
        self.assertIn('id="gov-banner"', page)
        self.assertIn('id="gov-lineage"', page)
        self.assertIn("data flow", page)
        self.assertIn("observability", page)
        self.assertIn("governance.html", (REPO / "serving/dashboard/static/index.html").read_text())

    def test_frontend_error_state_does_not_mean_zero_datasets(self):
        node = subprocess.run(["node", "--version"], text=True, capture_output=True, check=False)
        if node.returncode != 0:
            self.skipTest("node không có")
        script = r"""
import { pageState, filterDatasets, layoutGraph, selectionView, detailModel, focusLayout, arrowPointsDownstream, edgePaint } from "./serving/dashboard/static/governance.js";
const failed = pageState(null, "không tới được FastAPI");
if (failed.showCatalog !== false || failed.countLabel !== null) process.exit(1);
const missing = pageState({ datasets: [], sources: { iceberg: { status: "unavailable" } } }, null);
if (missing.showCatalog !== false || missing.countLabel !== null) process.exit(1);
const catalog = pageState({
  datasets: [
    { id: "iceberg.gold.fact_ratings", name: "fact_ratings", location: "gold", environment: "production", status: "unknown", zone: "gold", layer: "gold", declared_in_code: true },
    { id: "integration.resources", name: "resources", location: "tests", environment: "integration", status: "integration_only", zone: "integration", layer: "consume", declared_in_code: true },
  ],
  sources: { iceberg: { status: "unavailable" } },
  integration: { status: "unavailable" },
  drift: { status: "unavailable", findings: null },
  historical_proof_used_as_current_health: false,
}, null);
if (!catalog.showCatalog || catalog.countLabel !== "2") process.exit(1);
if (!catalog.banner.includes("unknown") && !catalog.banner.includes("unavailable")) process.exit(1);
const filtered = filterDatasets(catalog && [
  { id: "iceberg.gold.fact_ratings", name: "fact_ratings", location: "", environment: "production", status: "unknown", zone: "gold" },
  { id: "integration.resources", name: "resources", location: "", environment: "integration", status: "integration_only", zone: "integration" },
], { environment: "integration" });
if (filtered.length !== 1 || filtered[0].id !== "integration.resources") process.exit(1);
const graph = layoutGraph([
  { id: "a", name: "a", layer: "silver", declared_in_code: true },
  { id: "b", name: "b", layer: "gold", declared_in_code: true },
], [{ upstream: "a", downstream: "b", path: "job.py", token: "fact" }], "a");
if (!graph.links[0].active || graph.links[0].token !== "fact") process.exit(1);

const datasets = [
  { id: "minio.ratings", name: "ratings", layer: "source", environment: "production", pipeline: "not_applicable", expected_schema: [{ name: "user_id", type: "LONG" }] },
  { id: "iceberg.bronze.raw_ratings", name: "raw_ratings", layer: "bronze", environment: "production", pipeline: "not_applicable", expected_schema: [{ name: "rating_id", type: "LONG" }] },
  { id: "iceberg.silver.stg_ratings", name: "stg_ratings", layer: "silver", environment: "production", pipeline: "not_applicable", expected_schema: [] },
  { id: "iceberg.gold.fact_ratings", name: "fact_ratings", layer: "gold", environment: "production", pipeline: "not_applicable", expected_schema: [{ name: "rating_sk", type: "BIGINT" }] },
  { id: "iceberg.gold.fact_watch_sessions", name: "fact_watch_sessions", layer: "gold", environment: "production", pipeline: "not_applicable", expected_schema: [{ name: "session_sk", type: "BIGINT" }], downstream: [] },
  { id: "iceberg.gold.obt_movie_engagement", name: "obt_movie_engagement", layer: "gold", environment: "production", pipeline: "not_applicable", expected_schema: [{ name: "movie_sk", type: "BIGINT" }] },
  { id: "clickhouse.movie_ratings_stage", name: "movie_ratings_stage", layer: "serving", environment: "production", pipeline: "not_applicable" },
  { id: "iceberg.silver.stg_playback_windows", name: "stg_playback_windows", layer: "silver", environment: "production", pipeline: "disabled" },
  { id: "integration.resources", name: "resources", layer: "consume", environment: "integration", pipeline: "not_applicable" },
];
const edges = [
  { upstream: "minio.ratings", downstream: "iceberg.bronze.raw_ratings", path: "ingest.py", token: "raw_ratings", kind: "data_flow" },
  { upstream: "iceberg.bronze.raw_ratings", downstream: "iceberg.silver.stg_ratings", path: "publish.py", token: "stg_ratings", kind: "data_flow" },
  { upstream: "iceberg.silver.stg_ratings", downstream: "iceberg.gold.fact_ratings", path: "build.py", token: "fact_ratings", kind: "data_flow" },
  { upstream: "iceberg.gold.fact_ratings", downstream: "clickhouse.movie_ratings_stage", path: "sync.py", token: "_rating_rows", kind: "data_flow" },
  { upstream: "iceberg.gold.fact_ratings", downstream: "minio.orchestration_marker", path: "dag.py", token: "fact_ratings", kind: "control_dependency" },
  { upstream: "clickhouse.movie_ratings_stage", downstream: "api.ready", path: "app.py", token: "movie_ratings", kind: "observability" },
  { upstream: "kafka.movie.playback.heartbeat", downstream: "iceberg.silver.stg_playback_windows", path: "job.py", token: "playback_windows", kind: "data_flow" },
];
const edgeKey = (edge) => edge.upstream + ">" + edge.downstream + ">" + edge.path + ">" + edge.token;
const known = new Set(edges.map(edgeKey));
let selected = "iceberg.gold.fact_ratings";
const hidden = selectionView(datasets, selected, { q: "watch" });
if (hidden.showDetail || hidden.graph) process.exit(2);
if (hidden.selected.id !== "iceberg.gold.fact_ratings") process.exit(2);
if (hidden.filtered.some((row) => row.id === "iceberg.gold.fact_ratings")) process.exit(2);
selected = "iceberg.gold.fact_watch_sessions";
const next = selectionView(datasets, selected, {});
if (!next.showDetail || next.selected.id !== selected) process.exit(3);
const detail = detailModel(next.selected);
if (detail.schemaNames.includes("rating_sk") || detail.downstream.length !== 0) process.exit(3);
const watch = focusLayout(datasets, edges, selected, "downstream");
if (watch.links.length !== 0 || !watch.emptyDownstream) process.exit(4);
if (watch.nodes.some((node) => node.id === "iceberg.gold.obt_movie_engagement")) process.exit(4);
const full = focusLayout(datasets, edges, "iceberg.gold.fact_ratings", "full");
const fullIds = new Set(full.nodes.map((node) => node.id));
for (const link of full.links) {
  if (!known.has(edgeKey(link)) || !fullIds.has(link.upstream) || !fullIds.has(link.downstream)) process.exit(5);
}
if (full.links.some((link) => link.upstream === "iceberg.gold.fact_watch_sessions" && link.downstream === "iceberg.gold.obt_movie_engagement")) process.exit(5);
if (fullIds.has("iceberg.gold.obt_movie_engagement") || fullIds.has("iceberg.silver.stg_playback_windows")) process.exit(5);
const upstream = focusLayout(datasets, edges, "iceberg.gold.fact_ratings", "upstream");
if (upstream.links.length !== 1 || upstream.links[0].upstream !== "iceberg.silver.stg_ratings") process.exit(6);
for (let i = 0; i < full.nodes.length; i += 1) {
  for (let j = i + 1; j < full.nodes.length; j += 1) {
    const a = full.nodes[i];
    const b = full.nodes[j];
    const apart = a.x + a.w <= b.x || b.x + b.w <= a.x || a.y + a.h <= b.y || b.y + b.h <= a.y;
    if (!apart) process.exit(7);
  }
}
const raw = full.nodes.find((node) => node.id === "minio.ratings");
const gold = full.nodes.find((node) => node.id === "iceberg.gold.fact_ratings");
if (!(raw.x < gold.x)) process.exit(8);
const disabled = focusLayout(datasets, edges, "iceberg.silver.stg_playback_windows", "full");
if (!disabled.links[0] || disabled.links[0].disabled !== true || disabled.links[0].token !== "playback_windows") process.exit(9);
if (disabled.links[0].kind !== "data_flow" || edgePaint(disabled.links[0]).stroke !== "#8a3d12") process.exit(9);
const dataPaint = edgePaint({ kind: "data_flow" });
const controlPaint = edgePaint({ kind: "control_dependency" });
const healthPaint = edgePaint({ kind: "observability" });
const unknownPaint = edgePaint({});
if (controlPaint.stroke === dataPaint.stroke || !controlPaint.dash) process.exit(10);
if (healthPaint.stroke === dataPaint.stroke || !healthPaint.dash) process.exit(10);
if (unknownPaint.stroke === dataPaint.stroke) process.exit(10);
const down = focusLayout(datasets, edges, "iceberg.gold.fact_ratings", "downstream");
const marker = down.links.find((link) => link.token === "fact_ratings");
const rows = down.links.find((link) => link.token === "_rating_rows");
if (!marker || marker.kind !== "control_dependency" || !rows || rows.kind !== "data_flow") process.exit(10);
if (edgePaint(marker).stroke === edgePaint(rows).stroke) process.exit(10);
for (const link of full.links) {
  const source = full.nodes.find((node) => node.id === link.upstream);
  const target = full.nodes.find((node) => node.id === link.downstream);
  if (link.direction !== "upstream_to_downstream" || !arrowPointsDownstream(link.d, source, target)) process.exit(11);
}
const alone = focusLayout(datasets, edges, "integration.resources", "downstream");
if (alone.links.length !== 0 || !alone.emptyDownstream) process.exit(12);
const apiLayout = focusLayout(
  [
    { id: "api.movies.top_rated", name: "top_rated", layer: "consume", environment: "production", pipeline: "not_applicable" },
    { id: "dashboard.analytics", name: "dashboard", layer: "consume", environment: "production", pipeline: "not_applicable" },
  ],
  [{ upstream: "api.movies.top_rated", downstream: "dashboard.analytics", path: "app.js", token: "/movies/top-rated", kind: "api_consumption" }],
  "api.movies.top_rated",
  "downstream",
);
const apiLink = apiLayout.links[0];
if (!apiLink || edgePaint(apiLink).stroke === dataPaint.stroke) process.exit(10);
if (!arrowPointsDownstream(apiLink.d, apiLayout.nodes.find((node) => node.id === apiLink.upstream), apiLayout.nodes.find((node) => node.id === apiLink.downstream))) process.exit(11);
"""
        completed = subprocess.run(
            ["node", "--input-type=module", "-e", script],
            cwd=REPO,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr or completed.stdout)


try:
    from fastapi.testclient import TestClient

    from serving.api.app import app
    from serving.api.governance_routes import clear_cache, get_view
except ImportError:
    TestClient = None


@unittest.skipUnless(TestClient, "fastapi chưa được cài")
class GovernanceRouteTests(unittest.TestCase):
    def setUp(self):
        clear_cache()
        self.view = build_view(_raw())
        app.dependency_overrides[get_view] = lambda: self.view
        self.client = TestClient(app)

    def tearDown(self):
        app.dependency_overrides.clear()
        clear_cache()

    def test_get_schema_filter_and_reject_writes(self):
        snapshot = self.client.get("/governance/snapshot")
        self.assertEqual(snapshot.status_code, 200)
        body = snapshot.json()
        self.assertIn("datasets", body)
        self.assertIsNone(body["drift"]["findings"])
        self.assertFalse(body["historical_proof_used_as_current_health"])
        listed = self.client.get("/governance/datasets", params={"environment": "production", "q": "stg_ratings"})
        self.assertEqual(listed.status_code, 200)
        ids = [item["id"] for item in listed.json()["datasets"]]
        self.assertIn("iceberg.silver.stg_ratings", ids)
        self.assertNotIn("integration.resources", ids)
        detail = self.client.get("/governance/datasets/iceberg.silver.stg_ratings")
        self.assertEqual(detail.status_code, 200)
        self.assertEqual(detail.json()["status"], "unknown")
        missing = self.client.get("/governance/datasets/does.not.exist")
        self.assertEqual(missing.status_code, 404)
        lineage = self.client.get("/governance/lineage/iceberg.silver.stg_ratings")
        self.assertIn("iceberg.gold.fact_ratings", lineage.json()["descendants"])
        impact = self.client.get("/governance/impact/iceberg.gold.fact_watch_sessions")
        self.assertEqual(impact.json()["edges"], [])
        self.assertEqual(self.client.get("/governance/drift").json()["status"], "unavailable")
        self.assertEqual(self.client.get("/governance/health").json()["freshness"]["streaming"], "disabled")
        for method in (self.client.post, self.client.put, self.client.delete):
            response = method("/governance/datasets")
            self.assertEqual(response.status_code, 405)
        spec = self.client.get("/openapi.json").json()["paths"]
        for path, methods in spec.items():
            if path.startswith("/governance"):
                self.assertTrue(set(methods).issubset({"get", "head"}))

    def test_cache_reads_loader_once_inside_ttl(self):
        from serving.api.governance_routes import cached_live_view

        calls = {"n": 0}

        def loader():
            calls["n"] += 1
            return _raw()

        clear_cache()
        cached_live_view(loader=loader, now=100)
        cached_live_view(loader=loader, now=110)
        self.assertEqual(calls["n"], 1)
        cached_live_view(loader=loader, now=120)
        self.assertEqual(calls["n"], 2)


if __name__ == "__main__":
    unittest.main()
