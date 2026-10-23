/** Catalog, lineage và sức khỏe. Lỗi mạng không được vẽ thành 0 dataset. */

const LAYERS = [
  ["source", "Raw"],
  ["bronze", "Bronze"],
  ["silver", "Silver"],
  ["gold", "Gold"],
  ["serving", "Serving"],
  ["consume", "API"],
];
const LAYER_INDEX = Object.fromEntries(LAYERS.map(([id], index) => [id, index]));
const LAYER_LABEL = Object.fromEntries(LAYERS);
const ROW = 64;
const NODE_H = 46;
const COL_GAP = 72;

export function pageState(payload, fetchError) {
  if (fetchError) {
    return { banner: fetchError, showCatalog: false, countLabel: null };
  }
  if (!payload || !Array.isArray(payload.datasets)) {
    return {
      banner: "API không trả catalog. Không hiển thị 0 dataset.",
      showCatalog: false,
      countLabel: null,
    };
  }
  const iceberg = payload.sources && payload.sources.iceberg && payload.sources.iceberg.status;
  if (payload.datasets.length === 0 && iceberg !== "read") {
    return {
      banner: "Catalog không đọc được và không có inventory. Không coi là 0 dataset.",
      showCatalog: false,
      countLabel: null,
    };
  }
  const banners = [];
  if (iceberg !== "read") {
    banners.push("Iceberg " + (iceberg || "unavailable") + ". Trạng thái vật lý là unknown, không phải danh sách rỗng.");
  }
  if (payload.integration && payload.integration.status === "unavailable") {
    banners.push("Danh sách bảng integration chưa đọc được.");
  }
  if (payload.drift && payload.drift.status === "unavailable") {
    banners.push("Drift unavailable.");
  }
  if (payload.historical_proof_used_as_current_health) {
    banners.push("Payload gắn proof lịch sử vào sức khỏe hiện tại. UI không dùng kết quả PASS cũ.");
  }
  return {
    banner: banners.join(" "),
    showCatalog: true,
    countLabel: String(payload.datasets.length),
  };
}

export function filterDatasets(rows, query = {}) {
  const needle = String(query.q || "").trim().toLowerCase();
  return rows.filter((row) => {
    if (query.environment && row.environment !== query.environment) return false;
    if (query.status && row.status !== query.status) return false;
    if (query.zone && row.zone !== query.zone) return false;
    if (needle && !`${row.id} ${row.name} ${row.location || ""}`.toLowerCase().includes(needle)) return false;
    return true;
  });
}

export function selectionView(datasets, selectedId, query = {}) {
  const filtered = filterDatasets(datasets, query);
  const selected = selectedId ? datasets.find((row) => row.id === selectedId) || null : null;
  if (!selected) {
    return {
      filtered,
      selected: null,
      showDetail: false,
      graph: false,
      notice: "Chọn một dataset trong catalog.",
    };
  }
  if (!filtered.some((row) => row.id === selected.id)) {
    return {
      filtered,
      selected,
      showDetail: false,
      graph: false,
      notice: `Đang chọn ${selected.id}. Dataset này không nằm trong bộ lọc.`,
    };
  }
  return { filtered, selected, showDetail: true, graph: true, notice: null };
}

export function detailModel(dataset) {
  if (!dataset) return null;
  return {
    id: dataset.id,
    environment: dataset.environment || null,
    pipeline: dataset.pipeline || null,
    schemaNames: (dataset.expected_schema || []).map((field) => field.name),
    downstream: (dataset.downstream || []).map((edge) => edge.downstream),
  };
}

export function focusEdges(edges, selectedId, mode) {
  const list = (edges || []).filter((edge) => edge && edge.upstream && edge.downstream);
  if (mode === "upstream") return list.filter((edge) => edge.downstream === selectedId);
  if (mode === "downstream") return list.filter((edge) => edge.upstream === selectedId);
  const outgoing = new Map();
  const incoming = new Map();
  for (const edge of list) {
    if (!outgoing.has(edge.upstream)) outgoing.set(edge.upstream, []);
    outgoing.get(edge.upstream).push(edge);
    if (!incoming.has(edge.downstream)) incoming.set(edge.downstream, []);
    incoming.get(edge.downstream).push(edge);
  }
  const kept = new Map();
  const walk = (nextOf) => {
    const seen = new Set([selectedId]);
    const queue = [selectedId];
    while (queue.length) {
      const current = queue.shift();
      for (const edge of nextOf(current) || []) {
        kept.set(`${edge.upstream}\0${edge.downstream}\0${edge.path}\0${edge.token}`, edge);
        const other = edge.upstream === current ? edge.downstream : edge.upstream;
        if (!seen.has(other)) {
          seen.add(other);
          queue.push(other);
        }
      }
    }
  };
  walk((id) => outgoing.get(id));
  walk((id) => incoming.get(id));
  return [...kept.values()];
}

export function focusLayout(datasets, edges, selectedId, mode = "full") {
  const related = focusEdges(edges, selectedId, mode);
  const directIn = (edges || []).filter((edge) => edge.downstream === selectedId);
  const directOut = (edges || []).filter((edge) => edge.upstream === selectedId);
  const ids = new Set([selectedId, ...related.flatMap((edge) => [edge.upstream, edge.downstream])]);
  const byId = new Map((datasets || []).map((row) => [row.id, row]));
  const nodes = [...ids].filter(Boolean).map((id) => {
    const row = byId.get(id);
    return {
      id,
      name: row && row.name ? row.name : id,
      layer: layerOf(row, id),
      environment: row ? row.environment : "production",
      pipeline: row ? row.pipeline : "not_applicable",
      selected: id === selectedId,
    };
  });
  const columns = LAYERS.map(([layer]) => nodes.filter((node) => node.layer === layer).sort((a, b) => a.name.localeCompare(b.name)));
  const present = columns.map((column, index) => ({ layer: LAYERS[index][0], label: LAYERS[index][1], nodes: column })).filter((column) => column.nodes.length);
  const widthOf = (node) => {
    const lines = labelLines(node.name);
    return Math.max(132, ...lines.map((line) => Math.ceil(line.length * 7.2) + 20));
  };
  let cursor = 20;
  const placed = [];
  present.forEach((column) => {
    const width = Math.max(...column.nodes.map(widthOf));
    column.nodes.forEach((node, index) => {
      placed.push({
        ...node,
        lines: labelLines(node.name),
        layerLabel: column.label,
        x: cursor,
        y: 36 + index * ROW,
        w: width,
        h: NODE_H,
      });
    });
    cursor += width + COL_GAP;
  });
  const byNode = new Map(placed.map((node) => [node.id, node]));
  const links = related.filter((edge) => byNode.has(edge.upstream) && byNode.has(edge.downstream)).map((edge) => {
    const source = byNode.get(edge.upstream);
    const target = byNode.get(edge.downstream);
    const disabled = source.pipeline === "disabled" || target.pipeline === "disabled";
    return {
      upstream: edge.upstream,
      downstream: edge.downstream,
      path: edge.path,
      token: edge.token,
      kind: edge.kind || null,
      direction: "upstream_to_downstream",
      disabled,
      d: route(source, target),
    };
  });
  const height = Math.max(120, 36 + Math.max(1, ...present.map((column) => column.nodes.length)) * ROW);
  const width = placed.length ? Math.max(...placed.map((node) => node.x + node.w)) + 24 : 280;
  let message = null;
  if (!selectedId) message = "Chọn một dataset trong catalog.";
  else if (mode === "upstream" && directIn.length === 0) message = "Không có upstream đã kiểm chứng.";
  else if (mode === "downstream" && directOut.length === 0) message = "Không có downstream đã kiểm chứng.";
  else if (directIn.length === 0 && directOut.length === 0) message = "Không có upstream và downstream đã kiểm chứng.";
  return {
    mode,
    nodes: placed,
    links,
    width,
    height,
    message,
    emptyUpstream: directIn.length === 0,
    emptyDownstream: directOut.length === 0,
  };
}

export function layoutGraph(datasets, edges, selectedId) {
  const layout = focusLayout(datasets, edges, selectedId, "full");
  return { ...layout, links: layout.links.map((link) => ({ ...link, active: true })) };
}

function layerOf(row, id) {
  if (row && LAYER_INDEX[row.layer] != null) return row.layer;
  if (id.startsWith("iceberg.bronze.")) return "bronze";
  if (id.startsWith("iceberg.silver.")) return "silver";
  if (id.startsWith("iceberg.gold.")) return "gold";
  if (id.startsWith("clickhouse.") || id === "minio.orchestration_marker") return "serving";
  if (id.startsWith("minio.") || id.startsWith("kafka.")) return "source";
  return "consume";
}

function labelLines(name) {
  const max = 28;
  if (name.length <= max) return [name];
  const breakAt = name.lastIndexOf("_", max);
  const cut = breakAt > 8 ? breakAt : max;
  const rest = name.slice(cut).replace(/^_/, "");
  return [name.slice(0, cut), rest.length > max ? `${rest.slice(0, max - 1)}…` : rest];
}

function route(source, target) {
  const y1 = source.y + source.h / 2;
  const y2 = target.y + target.h / 2;
  if (source.x === target.x) {
    const jog = source.x + source.w + 22;
    return `M ${source.x + source.w} ${y1} H ${jog} V ${y2} H ${target.x + target.w}`;
  }
  const downstreamIsRight = source.x <= target.x;
  const x1 = downstreamIsRight ? source.x + source.w : source.x;
  const x2 = downstreamIsRight ? target.x : target.x + target.w;
  const mid = x1 + (x2 - x1) / 2;
  return `M ${x1} ${y1} H ${mid} V ${y2} H ${x2}`;
}

export function arrowPointsDownstream(d, source, target) {
  const tokens = String(d).match(/[MLHV]|-?\d+(?:\.\d+)?/g) || [];
  let x = 0;
  let y = 0;
  let start = null;
  for (let index = 0; index < tokens.length; index += 1) {
    const token = tokens[index];
    if (token === "M" || token === "L") {
      x = Number(tokens[index += 1]);
      y = Number(tokens[index += 1]);
      if (!start) start = { x, y };
    } else if (token === "H") {
      x = Number(tokens[index += 1]);
    } else if (token === "V") {
      y = Number(tokens[index += 1]);
    }
  }
  if (!start) return false;
  const onBorder = (point, node) => {
    const onY = point.y >= node.y && point.y <= node.y + node.h;
    return onY && (point.x === node.x || point.x === node.x + node.w);
  };
  return onBorder(start, source) && onBorder({ x, y }, target) && !onBorder(start, target) && !onBorder({ x, y }, source);
}

const EDGE_PAINT = {
  data_flow: { stroke: "#8a3d12", dash: null, marker: "gov-arrow-data" },
  control_dependency: { stroke: "#5e584f", dash: "7 4", marker: "gov-arrow-control" },
  observability: { stroke: "#0f5c62", dash: "2 3", marker: "gov-arrow-observe" },
  api_consumption: { stroke: "#0f5c62", dash: null, marker: "gov-arrow-api" },
};

export function edgePaint(link) {
  const paint = EDGE_PAINT[link.kind] || { stroke: "#a8a29e", dash: "1 3", marker: "gov-arrow-unknown" };
  if (link.disabled && !paint.dash) return { ...paint, dash: "5 4" };
  return paint;
}

const KIND_LABEL = {
  data_flow: "data flow",
  control_dependency: "control",
  observability: "observability",
  api_consumption: "api",
};

const state = { payload: null, selected: null, mode: "full", zoom: 1, layout: null };

export function boot() {
  document.getElementById("refresh").addEventListener("click", () => load(true));
  document.getElementById("filters").addEventListener("submit", (event) => event.preventDefault());
  document.getElementById("filters").addEventListener("input", () => render(null));
  document.querySelectorAll("[data-mode]").forEach((button) => {
    button.addEventListener("click", () => {
      state.mode = button.dataset.mode;
      render(null);
    });
  });
  document.getElementById("zoom-in").addEventListener("click", () => setZoom(state.zoom + 0.2));
  document.getElementById("zoom-out").addEventListener("click", () => setZoom(state.zoom - 0.2));
  document.getElementById("zoom-reset").addEventListener("click", () => setZoom(1));
  load(false);
}

function setZoom(value) {
  state.zoom = Math.min(2.4, Math.max(0.6, Math.round(value * 10) / 10));
  applyZoom();
}

function applyZoom() {
  const svg = document.getElementById("gov-lineage");
  if (!state.layout) return;
  svg.style.width = `${Math.round(state.layout.width * state.zoom)}px`;
  svg.style.height = `${Math.round(state.layout.height * state.zoom)}px`;
}

async function load(refresh) {
  const readState = document.getElementById("read-state");
  readState.textContent = "Đang đọc FastAPI…";
  try {
    const response = await fetch(`/api/governance/snapshot${refresh ? "?refresh=1" : ""}`, {
      headers: { accept: "application/json" },
    });
    const text = await response.text();
    if (!response.ok) throw new Error(text || `HTTP ${response.status}`);
    state.payload = JSON.parse(text);
    if (state.selected && !state.payload.datasets.some((row) => row.id === state.selected)) state.selected = null;
    readState.textContent = "đã đọc " + (state.payload.generated_at || "");
  } catch (error) {
    state.payload = null;
    state.selected = null;
    readState.textContent = "không tới được FastAPI";
    render(error.message || "không tới được FastAPI");
    return;
  }
  render(null);
}

function render(fetchError) {
  const payload = state.payload;
  const view = pageState(payload, fetchError || null);
  const banner = document.getElementById("gov-banner");
  banner.hidden = !view.banner;
  banner.textContent = view.banner || "";
  const count = document.getElementById("gov-count");
  count.textContent = view.countLabel == null ? "không có số lượng" : `${view.countLabel} dataset trong inventory`;
  fillHealth(view.showCatalog ? payload : null);
  fillFilters(view.showCatalog ? payload.datasets : []);
  const selection = view.showCatalog ? selectionView(payload.datasets, state.selected, currentQuery()) : null;
  document.getElementById("selection-note").textContent = selection && selection.notice && state.selected ? selection.notice : "";
  fillCatalog(selection ? selection.filtered : [], view.showCatalog);
  drawGraph(selection && selection.graph ? payload : null, selection);
  fillDetail(selection && selection.showDetail ? selection.selected : null, view.showCatalog ? payload : null, selection ? selection.notice : null);
  document.querySelectorAll("[data-mode]").forEach((button) => {
    button.setAttribute("aria-pressed", button.dataset.mode === state.mode ? "true" : "false");
  });
}

function currentQuery() {
  return {
    q: document.getElementById("q").value,
    environment: document.getElementById("environment").value,
    zone: document.getElementById("zone").value,
    status: document.getElementById("status").value,
  };
}

function fillFilters(rows) {
  fillSelect("zone", [...new Set(rows.map((row) => row.zone))].sort());
  fillSelect("status", [...new Set(rows.map((row) => row.status))].sort());
}

function fillSelect(id, values) {
  const select = document.getElementById(id);
  const current = select.value;
  const first = select.options[0];
  select.replaceChildren(first);
  values.forEach((value) => {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = value;
    select.append(option);
  });
  if ([...select.options].some((option) => option.value === current)) select.value = current;
}

function choose(id) {
  state.selected = id;
  render(null);
}

function fillCatalog(rows, show) {
  const body = document.getElementById("gov-catalog");
  body.replaceChildren();
  if (!show) return;
  if (rows.length === 0) {
    const tr = document.createElement("tr");
    tr.append(cell("0 khớp bộ lọc"));
    body.append(tr);
    return;
  }
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    const selected = row.id === state.selected;
    if (selected) tr.className = "row-selected";
    tr.tabIndex = 0;
    tr.setAttribute("role", "button");
    tr.setAttribute("aria-selected", selected ? "true" : "false");
    tr.addEventListener("click", () => choose(row.id));
    tr.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        choose(row.id);
      }
    });
    tr.append(cell(row.id), cell(row.environment), cell(row.presence), cell(row.status), cell(row.pipeline));
    body.append(tr);
  });
}

function fillHealth(payload) {
  const target = document.getElementById("gov-health");
  const chips = document.getElementById("source-chips");
  const integration = document.getElementById("integration-note");
  chips.replaceChildren();
  if (!payload) {
    target.textContent = "unavailable";
    integration.textContent = "";
    return;
  }
  const health = payload.health || {};
  const freshness = health.freshness || {};
  target.textContent = [
    `health ${health.status || "unknown"}`,
    `batch pipeline ${health.batch_pipeline || "unknown"}`,
    `freshness batch ${freshness.batch || "unavailable"}`,
    `streaming ${freshness.streaming || "unavailable"}`,
    `serving ${health.serving && health.serving.status ? health.serving.status : "unavailable"}`,
  ].join(" · ");
  Object.entries(payload.sources || {}).forEach(([name, source]) => {
    const chip = document.createElement("span");
    chip.className = "chip";
    chip.textContent = `${name} ${source.status || "unknown"}`;
    chips.append(chip);
  });
  const block = payload.integration || {};
  integration.textContent = block.status === "read"
    ? `Bảng integration vật lý: ${block.count}. cleanup_candidate ${block.cleanup_candidate}, keep_referenced ${block.keep_referenced}.`
    : "Bảng integration: unavailable";
}

function drawGraph(payload, selection) {
  const svg = document.getElementById("gov-lineage");
  const note = document.getElementById("lineage-note");
  const evidence = document.getElementById("lineage-evidence");
  svg.replaceChildren();
  evidence.replaceChildren();
  if (!payload || !selection || !selection.graph) {
    state.layout = null;
    note.textContent = selection && selection.notice ? selection.notice : "Chọn một dataset trong catalog.";
    svg.setAttribute("viewBox", "0 0 320 80");
    return;
  }
  const layout = focusLayout(payload.datasets, payload.edges || [], state.selected, state.mode);
  state.layout = layout;
  note.textContent = layout.message || `${layout.nodes.length} dataset, ${layout.links.length} cạnh đã kiểm chứng.`;
  svg.setAttribute("viewBox", `0 0 ${layout.width} ${layout.height}`);
  applyZoom();
  const defs = svgEl("defs");
  const paints = [...Object.values(EDGE_PAINT), { stroke: "#a8a29e", marker: "gov-arrow-unknown" }];
  paints.forEach((paint) => {
    const marker = svgEl("marker");
    marker.setAttribute("id", paint.marker);
    marker.setAttribute("viewBox", "0 0 8 8");
    marker.setAttribute("refX", "8");
    marker.setAttribute("refY", "4");
    marker.setAttribute("markerWidth", "8");
    marker.setAttribute("markerHeight", "8");
    marker.setAttribute("orient", "auto");
    const head = svgEl("path");
    head.setAttribute("d", "M0,0 L8,4 L0,8 Z");
    head.setAttribute("fill", paint.stroke);
    marker.append(head);
    defs.append(marker);
  });
  svg.append(defs);
  const seenLabel = new Set();
  layout.nodes.forEach((node) => {
    if (seenLabel.has(node.layer)) return;
    seenLabel.add(node.layer);
    const label = svgEl("text");
    label.setAttribute("x", node.x);
    label.setAttribute("y", 18);
    label.setAttribute("font-size", "12");
    label.setAttribute("fill", "#5e584f");
    label.textContent = node.layerLabel;
    svg.append(label);
  });
  layout.links.forEach((link) => {
    const path = svgEl("path");
    path.setAttribute("d", link.d);
    path.setAttribute("fill", "none");
    const paint = edgePaint(link);
    path.setAttribute("stroke", paint.stroke);
    path.setAttribute("stroke-width", link.kind === "data_flow" ? "1.8" : "1.4");
    if (paint.dash) path.setAttribute("stroke-dasharray", paint.dash);
    path.setAttribute("marker-end", `url(#${paint.marker})`);
    const title = svgEl("title");
    title.textContent = evidenceText(link);
    path.append(title);
    svg.append(path);
    const item = document.createElement("li");
    item.textContent = evidenceText(link);
    evidence.append(item);
  });
  if (layout.links.length === 0 && layout.message) {
    const item = document.createElement("li");
    item.textContent = layout.message;
    evidence.append(item);
  }
  layout.nodes.forEach((node) => svg.append(drawNode(node)));
}

function drawNode(node) {
  const group = svgEl("g");
  group.style.cursor = "pointer";
  group.setAttribute("data-id", node.id);
  group.addEventListener("click", () => choose(node.id));
  const rect = svgEl("rect");
  rect.setAttribute("x", node.x);
  rect.setAttribute("y", node.y);
  rect.setAttribute("width", node.w);
  rect.setAttribute("height", node.h);
  rect.setAttribute("rx", "8");
  const paint = nodePaint(node);
  rect.setAttribute("fill", paint.fill);
  rect.setAttribute("stroke", paint.stroke);
  rect.setAttribute("stroke-width", node.selected ? "2.4" : "1.4");
  if (paint.dash) rect.setAttribute("stroke-dasharray", paint.dash);
  const title = svgEl("title");
  title.textContent = node.id;
  group.append(rect, title);
  node.lines.forEach((line, index) => {
    const text = svgEl("text");
    text.setAttribute("x", node.x + 10);
    text.setAttribute("y", node.y + 20 + index * 16);
    text.setAttribute("font-size", "12");
    text.setAttribute("fill", "#1c1915");
    text.textContent = line;
    group.append(text);
  });
  return group;
}

function nodePaint(node) {
  if (node.selected) return { fill: "#f8e6e1", stroke: "#9a3412", dash: node.pipeline === "disabled" ? "5 4" : null };
  if (node.pipeline === "disabled") return { fill: "#f6f1e8", stroke: "#8a5a12", dash: "5 4" };
  if (node.environment === "integration") return { fill: "#e7f3f4", stroke: "#0f5c62", dash: null };
  return { fill: "#fff", stroke: "#1c1915", dash: null };
}

function fillDetail(dataset, payload, notice) {
  const detail = document.getElementById("gov-detail");
  const schema = document.getElementById("gov-schema");
  const impact = document.getElementById("gov-impact");
  schema.replaceChildren();
  if (!dataset) {
    detail.textContent = notice || (payload ? "Chọn một dataset trong catalog." : "Chưa có chi tiết vì catalog unavailable.");
    impact.textContent = payload && !notice ? "—" : "";
    return;
  }
  detail.textContent = [
    dataset.id,
    `environment ${dataset.environment}`,
    `presence ${dataset.presence}`,
    `status ${dataset.status}`,
    `pipeline ${dataset.pipeline}`,
    `snapshot ${dataset.snapshot_id || "—"}`,
    `records ${dataset.total_records ?? "—"}`,
    dataset.live_note || "",
  ].filter(Boolean).join(" · ");
  if (dataset.actual_schema == null && dataset.drift_classification === "unavailable") {
    schema.append(schemaRow("schema vật lý", "—", "unknown"));
  } else if (dataset.actual_schema == null && dataset.drift_classification === "not_compared") {
    schema.append(schemaRow("schema vật lý", "—", "không có bảng để so"));
  }
  const actual = new Map((dataset.actual_schema || []).map((field) => [field.name, field]));
  const names = [...new Set([...(dataset.expected_schema || []).map((field) => field.name), ...actual.keys()])];
  names.forEach((name) => {
    const expected = (dataset.expected_schema || []).find((field) => field.name === name);
    const physical = actual.get(name);
    schema.append(schemaRow(name, expected ? expected.type : "—", physical ? `${physical.type}${physical.required ? " required" : ""}` : "—"));
  });
  const upstream = dataset.upstream || [];
  const downstream = dataset.downstream || [];
  const parts = [
    `effect ${dataset.impact ? dataset.impact.effect : "unknown"}`,
    dataset.impact ? dataset.impact.reason : "",
    upstream.length ? `upstream: ${upstream.map(evidenceText).join("; ")}` : "Không có upstream đã kiểm chứng.",
    downstream.length ? `downstream: ${downstream.map(evidenceText).join("; ")}` : "Không có downstream đã kiểm chứng.",
  ];
  impact.textContent = parts.filter(Boolean).join(" · ");
}

function evidenceText(edge) {
  const kind = KIND_LABEL[edge.kind] || "chưa phân loại";
  return `${kind}: ${edge.upstream} → ${edge.downstream} (${edge.path}: ${edge.token})`;
}

function schemaRow(name, expected, physical) {
  const row = document.createElement("tr");
  row.append(cell(name), cell(expected), cell(physical));
  return row;
}

function cell(value) {
  const td = document.createElement("td");
  td.textContent = value == null || value === "" ? "—" : String(value);
  return td;
}

function svgEl(name) {
  return document.createElementNS("http://www.w3.org/2000/svg", name);
}

if (typeof document !== "undefined") boot();
