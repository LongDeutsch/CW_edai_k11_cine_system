/** Đọc JSON FastAPI qua proxy cùng origin. Timeout và retry nằm ở đây. */

import { detailOf } from "./view.js";

const ID_KEYS = "engagement_snapshot_id|ratings_snapshot_id|iceberg_snapshot_id|trending_snapshot_id";

export function parseApi(text) {
  if (!text) return null;
  const safe = text.replace(
    new RegExp(`"(${ID_KEYS})"\\s*:\\s*(-?\\d+)`, "g"),
    '"$1":"$2"'
  );
  return JSON.parse(safe);
}

function tryParse(text) {
  try {
    return parseApi(text);
  } catch {
    return text;
  }
}

export async function getJson(path, options = {}) {
  const fetchImpl = options.fetchImpl || fetch;
  const timeoutMs = options.timeoutMs ?? 20000;
  const retries = options.retries ?? 2;
  const sleep = options.sleep || ((ms) => new Promise((resolve) => setTimeout(resolve, ms)));
  let last;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetchImpl(`/api${path}`, {
        signal: controller.signal,
        headers: { accept: "application/json" },
      });
      const text = await response.text();
      const body = response.ok || text.trim().startsWith("{") || text.trim().startsWith("[") ? tryParse(text) : text;
      if (!response.ok) {
        const error = new Error(detailOf(body));
        error.status = response.status;
        error.detail = detailOf(body);
        if ((response.status === 503 || response.status === 409) && attempt < retries) {
          last = error;
          await sleep(options.retryDelayMs ?? 400 * (attempt + 1));
          continue;
        }
        throw error;
      }
      return body;
    } catch (error) {
      if (error.name === "AbortError") {
        last = new Error("hết thời gian chờ FastAPI");
        last.status = 0;
        last.detail = last.message;
      } else if (!error.status) {
        last = new Error("không tới được FastAPI");
        last.status = 0;
        last.detail = error.message || last.message;
      } else {
        last = error;
      }
      if (attempt < retries && (last.status === 0 || last.status === 503)) {
        await sleep(options.retryDelayMs ?? 400 * (attempt + 1));
        continue;
      }
      throw last;
    } finally {
      clearTimeout(timer);
    }
  }
  throw last;
}
