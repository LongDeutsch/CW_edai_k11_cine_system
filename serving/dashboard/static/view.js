/** Map payload FastAPI sang model hiển thị. Không tính metric mới. */

export function present(input) {
  const errors = [];
  if (input.transport) {
    errors.push({ section: "FastAPI", status: input.transport.status || 0, detail: input.transport.detail });
  }
  for (const [section, error] of [
    ["overview", input.overviewError],
    ["top-rated", input.topRatedError],
    ["most-watched", input.mostWatchedError],
    ["trending", input.trendingError],
    ["status", input.statusError],
  ]) {
    if (error) errors.push({ section, status: error.status || 0, detail: error.detail });
  }

  const overview = input.overviewError ? null : input.overview || null;
  const trending = input.trendingError ? null : input.trending || null;
  const status = input.statusError ? null : input.status || null;
  const held = Boolean(input.hold);
  const live = Boolean(
    !held &&
      !input.transport &&
      trending &&
      trending.live === true &&
      trending.freshness === "live"
  );

  return {
    live,
    held,
    freshness: trending ? trending.freshness : null,
    banner: banner(errors, held),
    errors,
    database: status ? status.database : null,
    batchDatabase: status ? status.batch_database || status.database : null,
    trendingDatabase: (input.trending && input.trending.database) || (status ? status.trending_database || status.database : null),
    batchConsistent: status ? status.consistent === true : null,
    status,
    overview,
    overviewEmpty: Boolean(overview && overview.engagement_snapshot_id == null && overview.ratings_snapshot_id == null),
    topRated: input.topRatedError ? null : input.topRated || null,
    mostWatched: input.mostWatchedError ? null : input.mostWatched || null,
    trending,
  };
}

function banner(errors, held) {
  const parts = [];
  if (held) parts.push("Số bên dưới là lần đọc trước. Không gắn nhãn live.");
  for (const error of errors) {
    const detail = clip(error.detail);
    if (error.status === 409) parts.push(`${error.section}: snapshot đang đổi (409). ${detail}`);
    else if (error.status === 503) parts.push(`${error.section}: HTTP 503. ${detail}`);
    else if (error.status === 0) parts.push(`${error.section}: ${detail}`);
    else parts.push(`${error.section}: HTTP ${error.status}. ${detail}`);
  }
  return parts.length ? parts.join(" ") : null;
}

function clip(detail) {
  const text = String(detail || "");
  return text.length > 180 ? `${text.slice(0, 180)}…` : text;
}

export function detailOf(body) {
  if (body && typeof body === "object" && body.detail != null) return String(body.detail);
  if (typeof body === "string" && body.trim()) return body.trim().slice(0, 300);
  return "FastAPI không trả nội dung";
}
