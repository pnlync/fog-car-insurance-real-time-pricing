# ruff: noqa: E501
from __future__ import annotations

import base64
import json
from typing import Any

from cloud.dashboard.demo_control import start_demo_session, stop_demo_session
from cloud.dashboard.queries import (
    get_demo_session,
    latest_demo_metrics,
    latest_metrics,
    list_vehicles,
    recent_demo_metrics,
    recent_metrics,
)


def _html_page() -> str:
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Fog Car Insurance Dashboard</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    :root {
      --bg: #f4f0e8;
      --surface: rgba(255, 252, 247, 0.92);
      --ink: #1d2a32;
      --accent: #a0432d;
      --accent-2: #2d6a73;
      --muted: #5f6b73;
      --line: rgba(29, 42, 50, 0.1);
      --shadow: 0 20px 50px rgba(29, 42, 50, 0.12);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(160, 67, 45, 0.18), transparent 34%),
        radial-gradient(circle at top right, rgba(45, 106, 115, 0.16), transparent 28%),
        linear-gradient(180deg, #f8f4ec 0%, var(--bg) 100%);
    }
    .page {
      width: min(1180px, calc(100vw - 32px));
      margin: 24px auto 56px;
    }
    .hero, .panel, .chart-card {
      background: var(--surface);
      border: 1px solid var(--line);
      border-radius: 24px;
      box-shadow: var(--shadow);
    }
    .hero {
      padding: 28px;
      display: grid;
      gap: 16px;
    }
    .eyebrow {
      color: var(--accent);
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      font-size: 12px;
    }
    h1 {
      margin: 0;
      font-size: clamp(32px, 5vw, 52px);
      line-height: 0.95;
      letter-spacing: -0.04em;
    }
    .subtitle {
      margin: 0;
      color: var(--muted);
      max-width: 760px;
      font-size: 16px;
    }
    .layout {
      margin-top: 18px;
      display: grid;
      gap: 18px;
    }
    .panel {
      padding: 20px;
    }
    .controls {
      display: grid;
      gap: 14px;
      grid-template-columns: 2fr 1fr 1fr;
      align-items: end;
    }
    label {
      display: block;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
      margin-bottom: 8px;
    }
    select, button {
      width: 100%;
      border-radius: 14px;
      border: 1px solid var(--line);
      font: inherit;
      padding: 12px 14px;
    }
    select {
      background: white;
      color: var(--ink);
    }
    button {
      cursor: pointer;
      background: var(--accent);
      color: white;
      font-weight: 700;
      transition: transform 120ms ease, opacity 120ms ease;
    }
    button.secondary {
      background: var(--accent-2);
    }
    button:hover { transform: translateY(-1px); }
    button:disabled { opacity: 0.5; cursor: not-allowed; transform: none; }
    .status-row {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: center;
      margin-top: 8px;
      color: var(--muted);
    }
    .pill {
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 6px 12px;
      background: rgba(45, 106, 115, 0.1);
      color: var(--accent-2);
      font-size: 13px;
      font-weight: 700;
    }
    .kpis {
      display: grid;
      gap: 14px;
      grid-template-columns: repeat(4, minmax(0, 1fr));
    }
    .kpi {
      padding: 18px;
      border-radius: 18px;
      background: rgba(255, 255, 255, 0.76);
      border: 1px solid var(--line);
    }
    .kpi-label {
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 10px;
    }
    .kpi-value {
      font-size: clamp(24px, 3vw, 36px);
      font-weight: 800;
      letter-spacing: -0.04em;
    }
    .charts {
      display: grid;
      gap: 18px;
      grid-template-columns: 1fr;
    }
    .chart-card { padding: 14px; }
    .chart-title {
      margin: 4px 8px 10px;
      font-size: 15px;
      font-weight: 700;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    .empty {
      padding: 32px;
      text-align: center;
      color: var(--muted);
    }
    @media (max-width: 900px) {
      .controls { grid-template-columns: 1fr; }
      .kpis { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
    @media (max-width: 560px) {
      .page { width: min(100vw - 20px, 1180px); margin-top: 10px; }
      .hero { padding: 22px; border-radius: 20px; }
      .panel, .chart-card { border-radius: 18px; }
      .kpis { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <div class="eyebrow">Fog and Edge Computing</div>
      <h1>Real-Time Car Insurance Pricing</h1>
      <p class="subtitle">
        Local edge and fog nodes generate and aggregate driver telemetry. AWS stores the processed windows and serves this live dashboard.
      </p>
    </section>

    <section class="panel layout">
      <div class="controls">
        <div>
          <label for="vehicle-select">Vehicle</label>
          <select id="vehicle-select"></select>
        </div>
        <div>
          <label>&nbsp;</label>
          <button id="start-demo">Start Demo</button>
        </div>
        <div>
          <label>&nbsp;</label>
          <button id="stop-demo" class="secondary">Stop Demo</button>
        </div>
      </div>
      <div class="status-row">
        <span class="pill" id="mode-pill">Production</span>
        <span id="status-text">Waiting for telemetry...</span>
      </div>
      <div class="kpis" id="kpis"></div>
    </section>

    <section class="charts">
      <div class="chart-card">
        <div class="chart-title">Average Speed</div>
        <div id="speed-chart"></div>
      </div>
      <div class="chart-card">
        <div class="chart-title">Risk Score</div>
        <div id="risk-chart"></div>
      </div>
      <div class="chart-card">
        <div class="chart-title">Brake and Lane Events</div>
        <div id="events-chart"></div>
      </div>
    </section>
  </div>

  <script>
    const state = {
      vehicleId: null,
      demoSessionId: null,
      refreshHandle: null,
    };

    const vehicleSelect = document.getElementById("vehicle-select");
    const modePill = document.getElementById("mode-pill");
    const statusText = document.getElementById("status-text");
    const kpis = document.getElementById("kpis");

    function api(path, options = {}) {
      return fetch(path, {
        headers: { "Content-Type": "application/json" },
        ...options,
      }).then(async (response) => {
        const text = await response.text();
        const data = text ? JSON.parse(text) : {};
        if (!response.ok) {
          throw new Error(data.error || response.statusText);
        }
        return data;
      });
    }

    function formatNumber(value, digits = 2) {
      if (value === null || value === undefined) return "--";
      return Number(value).toFixed(digits);
    }

    function renderKpis(latest) {
      if (!latest) {
        kpis.innerHTML = '<div class="kpi"><div class="kpi-label">Telemetry</div><div class="kpi-value">No data</div></div>';
        return;
      }
      const entries = [
        ["Risk Score", formatNumber(latest.risk_score, 4)],
        ["Premium Multiplier", formatNumber(latest.premium_multiplier, 4)],
        ["Avg Speed (km/h)", formatNumber(latest.avg_speed_kmh, 1)],
        ["Harsh Brakes", formatNumber(latest.harsh_brake_count, 0)],
      ];
      kpis.innerHTML = entries.map(([label, value]) => `
        <div class="kpi">
          <div class="kpi-label">${label}</div>
          <div class="kpi-value">${value}</div>
        </div>
      `).join("");
    }

    function renderEmptyCharts() {
      const layout = {
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        font: { family: "IBM Plex Sans, sans-serif", color: "#5f6b73" },
        xaxis: { visible: false },
        yaxis: { visible: false },
        margin: { l: 30, r: 20, t: 20, b: 30 },
        annotations: [{
          text: "No telemetry available yet",
          showarrow: false,
          xref: "paper",
          yref: "paper",
          x: 0.5,
          y: 0.5
        }]
      };
      Plotly.newPlot("speed-chart", [], layout, {displayModeBar: false, responsive: true});
      Plotly.newPlot("risk-chart", [], layout, {displayModeBar: false, responsive: true});
      Plotly.newPlot("events-chart", [], layout, {displayModeBar: false, responsive: true});
    }

    function renderCharts(history, label) {
      if (!history || history.length === 0) {
        renderEmptyCharts();
        return;
      }
      const times = history.map((item) => item.time);
      const baseLayout = {
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        font: { family: "IBM Plex Sans, sans-serif", color: "#1d2a32" },
        margin: { l: 44, r: 20, t: 18, b: 42 },
        xaxis: { gridcolor: "rgba(29,42,50,0.08)" },
        yaxis: { gridcolor: "rgba(29,42,50,0.08)" },
      };

      Plotly.newPlot("speed-chart", [{
        x: times,
        y: history.map((item) => item.avg_speed_kmh),
        mode: "lines+markers",
        line: { color: "#a0432d", width: 3 },
        marker: { size: 6, color: "#a0432d" },
        name: label
      }], { ...baseLayout, yaxis: { title: "km/h", gridcolor: "rgba(29,42,50,0.08)" } }, {displayModeBar: false, responsive: true});

      Plotly.newPlot("risk-chart", [{
        x: times,
        y: history.map((item) => item.risk_score),
        mode: "lines+markers",
        line: { color: "#2d6a73", width: 3 },
        marker: { size: 6, color: "#2d6a73" },
        name: label
      }], { ...baseLayout, yaxis: { title: "score", range: [0, 1], gridcolor: "rgba(29,42,50,0.08)" } }, {displayModeBar: false, responsive: true});

      Plotly.newPlot("events-chart", [
        {
          x: times,
          y: history.map((item) => item.harsh_brake_count),
          type: "bar",
          name: "Harsh Brakes",
          marker: { color: "#a0432d" }
        },
        {
          x: times,
          y: history.map((item) => item.lane_departure_count),
          type: "bar",
          name: "Lane Departures",
          marker: { color: "#2d6a73" }
        }
      ], { ...baseLayout, barmode: "group", yaxis: { title: "count", gridcolor: "rgba(29,42,50,0.08)" } }, {displayModeBar: false, responsive: true});
    }

    async function loadVehicles() {
      const payload = await api("/api/vehicles");
      const vehicles = payload.vehicles || [];
      vehicleSelect.innerHTML = vehicles.map((vehicleId) => `<option value="${vehicleId}">${vehicleId}</option>`).join("");
      if (!state.vehicleId && vehicles.length) {
        state.vehicleId = vehicles[0];
      }
      if (state.vehicleId) {
        vehicleSelect.value = state.vehicleId;
      }
    }

    async function refreshDashboard() {
      try {
        if (state.demoSessionId) {
          modePill.textContent = "Demo";
          const session = await api(`/api/demo/session?${new URLSearchParams({ demo_session_id: state.demoSessionId })}`);
          statusText.textContent = session.status
            ? `Session ${session.demo_session_id} · ${session.status} · ${session.ticks_completed || 0}/${session.max_ticks || 0} ticks`
            : "Demo session inactive";
          const data = await api(`/api/dashboard?${new URLSearchParams({ demo_session_id: state.demoSessionId })}`);
          renderKpis(data.latest);
          renderCharts(data.history, `Demo ${state.demoSessionId.slice(0, 8)}`);
          if (session.status && ["STOPPED", "COMPLETED", "FAILED"].includes(session.status)) {
            state.demoSessionId = null;
            modePill.textContent = "Production";
          }
          return;
        }

        modePill.textContent = "Production";
        if (!state.vehicleId) {
          await loadVehicles();
          if (!state.vehicleId) {
            statusText.textContent = "No vehicle telemetry available yet.";
            renderKpis(null);
            renderEmptyCharts();
            return;
          }
        }
        statusText.textContent = `Live production telemetry for ${state.vehicleId}`;
        const data = await api(`/api/dashboard?${new URLSearchParams({ vehicle_id: state.vehicleId })}`);
        renderKpis(data.latest);
        renderCharts(data.history, state.vehicleId);
      } catch (error) {
        statusText.textContent = `Error: ${error.message}`;
        renderKpis(null);
        renderEmptyCharts();
      }
    }

    vehicleSelect.addEventListener("change", (event) => {
      state.demoSessionId = null;
      state.vehicleId = event.target.value;
      refreshDashboard();
    });

    document.getElementById("start-demo").addEventListener("click", async () => {
      const session = await api("/api/demo/start", {
        method: "POST",
        body: JSON.stringify({ duration_minutes: 2, vehicle_count: 1 }),
      });
      state.demoSessionId = session.demo_session_id;
      await refreshDashboard();
    });

    document.getElementById("stop-demo").addEventListener("click", async () => {
      if (!state.demoSessionId) return;
      await api("/api/demo/stop", {
        method: "POST",
        body: JSON.stringify({ demo_session_id: state.demoSessionId }),
      });
      state.demoSessionId = null;
      await refreshDashboard();
    });

    renderEmptyCharts();
    loadVehicles().then(refreshDashboard);
    state.refreshHandle = window.setInterval(refreshDashboard, 5000);
  </script>
</body>
</html>
"""


def _headers(content_type: str) -> dict[str, str]:
    return {
        "Content-Type": content_type,
        "Cache-Control": "no-store",
    }


def _json_response(payload: dict[str, Any], status_code: int = 200) -> dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": _headers("application/json"),
        "body": json.dumps(payload),
    }


def _html_response(body: str) -> dict[str, Any]:
    return {
        "statusCode": 200,
        "headers": _headers("text/html; charset=utf-8"),
        "body": body,
    }


def _error_response(message: str, status_code: int = 400) -> dict[str, Any]:
    return _json_response({"error": message}, status_code=status_code)


def _parse_body(event: dict[str, Any]) -> dict[str, Any]:
    body = event.get("body")
    if not body:
        return {}
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")
    return json.loads(body)


def _route(event: dict[str, Any]) -> tuple[str, str]:
    request_context = event.get("requestContext", {})
    http = request_context.get("http", {})
    return http.get("method", "GET"), event.get("rawPath", "/")


def _query_params(event: dict[str, Any]) -> dict[str, str]:
    return event.get("queryStringParameters") or {}


def _dashboard_payload(query_params: dict[str, str]) -> dict[str, Any]:
    demo_session_id = query_params.get("demo_session_id")
    vehicle_id = query_params.get("vehicle_id")
    if demo_session_id:
        return {
            "latest": latest_demo_metrics(demo_session_id),
            "history": recent_demo_metrics(demo_session_id),
        }
    if vehicle_id:
        return {
            "latest": latest_metrics(vehicle_id),
            "history": recent_metrics(vehicle_id),
        }
    raise ValueError("vehicle_id or demo_session_id is required")


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    method, path = _route(event)
    query_params = _query_params(event)

    if method == "OPTIONS":
        return {"statusCode": 204, "headers": _headers("text/plain"), "body": ""}

    try:
        if method == "GET" and path == "/":
            return _html_response(_html_page())
        if method == "GET" and path == "/api/vehicles":
            return _json_response({"vehicles": list_vehicles()})
        if method == "GET" and path == "/api/dashboard":
            return _json_response(_dashboard_payload(query_params))
        if method == "GET" and path == "/api/demo/session":
            demo_session_id = query_params.get("demo_session_id")
            if not demo_session_id:
                return _error_response("demo_session_id is required")
            session = get_demo_session(demo_session_id)
            return _json_response(session or {"status": "NOT_FOUND"})
        if method == "POST" and path == "/api/demo/start":
            payload = _parse_body(event)
            return _json_response(
                start_demo_session(
                    duration_minutes=int(payload.get("duration_minutes", 2)),
                    vehicle_count=int(payload.get("vehicle_count", 1)),
                )
            )
        if method == "POST" and path == "/api/demo/stop":
            payload = _parse_body(event)
            demo_session_id = payload.get("demo_session_id")
            if not demo_session_id:
                return _error_response("demo_session_id is required")
            return _json_response(stop_demo_session(demo_session_id))
        return _error_response(f"Unsupported route: {method} {path}", status_code=404)
    except ValueError as exc:
        return _error_response(str(exc), status_code=400)
    except Exception as exc:  # pragma: no cover - defensive runtime response
        return _error_response(f"Unhandled error: {exc}", status_code=500)
