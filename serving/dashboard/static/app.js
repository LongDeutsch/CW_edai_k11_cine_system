import { getJson } from "./client.js";
import { present } from "./view.js";

const PATHS = [
  ["overview", "/analytics/overview"],
  ["topRated", "/movies/top-rated?limit=10"],
  ["mostWatched", "/movies/most-watched?limit=10"],
  ["trending", "/movies/trending?window_minutes=15&limit=10"],
  ["status", "/serving/status"],
];

let lastGood = null;

document.querySelector("#refresh").addEventListener("click", () => load(true));
load(false);
setInterval(() => load(false), 30000);

async function load(manual) {
  const button = document.querySelector("#refresh");
  button.disabled = true;
  document.querySelector("#read-state").textContent = manual ? "Đang đọc lại FastAPI…" : "Đang đọc FastAPI…";
  try {
    const payload = await loadAll();
    const anyOk = PATHS.some(([key]) => payload[key]);
    if (anyOk) lastGood = payload;
    render(present(payload));
  } catch (error) {
    const transport = { status: error.status || 0, detail: error.detail || error.message };
    const payload = { ...(lastGood || {}), transport, hold: Boolean(lastGood) };
    render(present(payload));
  } finally {
    button.disabled = false;
  }
}

async function loadAll() {
  const entries = await Promise.all(PATHS.map(async ([key, path]) => {
    try {
      return [key, await getJson(path), null];
    } catch (error) {
      return [key, null, { status: error.status || 0, detail: error.detail || error.message }];
    }
  }));
  const payload = {};
  let failures = 0;
  for (const [key, value, error] of entries) {
    payload[key] = value;
    payload[`${key}Error`] = error;
    if (error) failures += 1;
  }
  if (failures === PATHS.length) {
    const first = entries.find((item) => item[2]);
    const failure = new Error(first[2].detail);
    failure.status = first[2].status;
    failure.detail = first[2].detail;
    throw failure;
  }
  return payload;
}

function render(model) {
  window.__cinestream = model;
  const banner = document.querySelector("#banner");
  banner.hidden = !model.banner;
  banner.textContent = model.banner || "";

  text("#database", model.database || "chưa đọc được database");
  text("#batch-source", model.batchDatabase ? `batch ${model.batchDatabase}` : "batch —");
  text("#trend-source", model.trendingDatabase ? `trending ${model.trendingDatabase}` : "trending —");
  text("#overview-source", model.batchDatabase || "—");
  text("#trend-db", model.trendingDatabase || "—");
  const live = document.querySelector("#live");
  live.hidden = !model.live;
  live.textContent = "live";
  const fresh = document.querySelector("#freshness");
  fresh.hidden = !model.freshness;
  fresh.textContent = model.freshness || "";
  fresh.dataset.state = model.freshness || "";

  const batch = document.querySelector("#batch-consistent");
  if (model.batchConsistent === null) {
    batch.hidden = true;
  } else {
    batch.hidden = false;
    batch.textContent = model.batchConsistent ? "batch nhất quán" : "batch không nhất quán";
    batch.dataset.state = model.batchConsistent ? "ok" : "bad";
  }

  renderOverview(model.overview, model.overviewEmpty);
  renderStatus(model.status);
  renderMovies("#top-rated", model.topRated, "ratings_snapshot_id", (item) => [
    item.movie_id,
    item.title,
    item.content_rating,
    item.avg_rating,
    item.rating_count,
  ]);
  renderMovies("#most-watched", model.mostWatched, "engagement_snapshot_id", (item) => [
    item.movie_id,
    item.title,
    item.watch_seconds,
    item.session_count,
  ]);
  renderTrending(model.trending);
  document.querySelector("#read-state").textContent = "Đọc lại mỗi 30 giây qua proxy FastAPI.";
}

function renderOverview(overview, empty) {
  const emptyNote = document.querySelector("#overview-empty");
  emptyNote.hidden = !empty;
  const fields = ["session_count", "total_watch_seconds", "watched_movie_count", "rated_movie_count", "rating_count", "mean_rating"];
  for (const field of fields) {
    const value = overview ? overview[field] : null;
    metric(field, value);
  }
  text("#overview-batch", overview ? line(overview.batch_id) : "—");
  text("#overview-published", overview ? line(overview.published_at) : "—");
  text("#overview-engagement-snapshot", overview ? line(overview.engagement_snapshot_id) : "—");
  text("#overview-ratings-snapshot", overview ? line(overview.ratings_snapshot_id) : "—");
}

function renderStatus(status) {
  const fields = [
    ["#status-batch", status && status.batch_id],
    ["#status-published", status && status.published_at],
    ["#status-engagement-snapshot", status && status.engagement_snapshot_id],
    ["#status-ratings-snapshot", status && status.ratings_snapshot_id],
    ["#status-log", status ? String(status.log_present) : null],
    ["#status-consistent", status ? String(status.consistent) : null],
    ["#status-detail", status && status.detail],
    ["#status-engagement-rows", status && status.engagement_rows],
    ["#status-rating-rows", status && status.rating_rows],
    ["#status-trend-snapshot", status && status.trending_snapshot_id],
    ["#status-trend-rows", status && status.trending_rows],
    ["#status-trend-synced", status && status.trending_synced_at],
    ["#status-trend-window", status && status.trending_latest_window_end],
    ["#status-trend-fresh", status && status.trending_freshness],
    ["#status-trend-live", status ? String(status.trending_live) : null],
    ["#status-trend-poll", status && status.trending_poll_status],
    ["#status-trend-polled", status && status.trending_polled_at],
  ];
  for (const [selector, value] of fields) text(selector, line(value));
}

function renderMovies(selector, body, snapshotKey, cells) {
  const root = document.querySelector(selector);
  const snapshot = root.querySelector("[data-snapshot]");
  const table = root.querySelector("tbody");
  const empty = root.querySelector("[data-empty]");
  table.replaceChildren();
  if (!body) {
    snapshot.textContent = "—";
    empty.hidden = false;
    empty.textContent = "Không có payload cho bảng này.";
    return;
  }
  snapshot.textContent = line(body[snapshotKey]);
  empty.hidden = body.items.length > 0;
  empty.textContent = "API trả items rỗng.";
  for (const item of body.items) {
    const row = document.createElement("tr");
    for (const value of cells(item)) {
      const cell = document.createElement("td");
      cell.textContent = line(value);
      row.append(cell);
    }
    table.append(row);
  }
}

function renderTrending(trending) {
  text("#trend-window-end", trending ? line(trending.latest_window_end) : "—");
  text("#trend-synced", trending ? line(trending.synced_at) : "—");
  text("#trend-snapshot", trending ? line(trending.iceberg_snapshot_id) : "—");
  text("#trend-poll", trending ? line(trending.poll_status) : "—");
  text("#trend-polled", trending ? line(trending.polled_at) : "—");
  text("#trend-event-age", trending ? line(trending.event_age_seconds) : "—");
  text("#trend-sync-age", trending ? line(trending.sync_age_seconds) : "—");
  text("#trend-stale-after", trending ? line(trending.stale_after_seconds) : "—");
  text("#trend-minutes", trending ? line(trending.window_minutes) : "—");
  const body = document.querySelector("#trending tbody");
  const empty = document.querySelector("#trending-empty");
  body.replaceChildren();
  if (!trending) {
    empty.hidden = false;
    empty.textContent = "Không có payload trending.";
    return;
  }
  empty.hidden = trending.items.length > 0;
  empty.textContent = trending.freshness === "empty" ? "Chưa có cửa sổ Flink." : "API trả items rỗng.";
  for (const item of trending.items) {
    const row = document.createElement("tr");
    for (const value of [item.movie_id, item.recent_heartbeats, item.recent_watch_seconds, item.peak_minute_unique_viewers]) {
      const cell = document.createElement("td");
      cell.textContent = line(value);
      row.append(cell);
    }
    body.append(row);
  }
}

function metric(field, value) {
  const node = document.querySelector(`[data-metric="${field}"]`);
  node.dataset.value = value == null ? "" : String(value);
  node.textContent = value == null ? "—" : formatMetric(field, value);
}

function formatMetric(field, value) {
  if (field === "mean_rating" && typeof value === "number") return value.toFixed(4);
  if (typeof value === "number") return new Intl.NumberFormat("en-US").format(value);
  return String(value);
}

function text(selector, value) {
  document.querySelector(selector).textContent = value;
}

function line(value) {
  if (value == null || value === "") return "—";
  return String(value);
}
