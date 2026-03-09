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
      --surface: rgba(255, 252, 247, 0.94);
      --ink: #1d2a32;
      --accent: #a0432d;
      --accent-2: #2d6a73;
      --accent-3: #5a7f2b;
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
      width: min(1280px, calc(100vw - 32px));
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
      max-width: 860px;
      font-size: 16px;
    }
    .layout {
      margin-top: 18px;
      display: grid;
      gap: 18px;
    }
    .panel { padding: 20px; }
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
    button.secondary { background: var(--accent-2); }
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
      font-size: clamp(22px, 2.8vw, 32px);
      font-weight: 800;
      letter-spacing: -0.04em;
    }
    .charts {
      display: grid;
      gap: 18px;
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .chart-card {
      padding: 14px;
      min-height: 320px;
    }
    .chart-card-wide { grid-column: 1 / -1; }
    .chart-title {
      margin: 4px 8px 4px;
      font-size: 15px;
      font-weight: 700;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    .chart-note {
      margin: 0 8px 10px;
      color: var(--muted);
      font-size: 13px;
    }
    @media (max-width: 980px) {
      .controls { grid-template-columns: 1fr; }
      .kpis { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .charts { grid-template-columns: 1fr; }
      .chart-card-wide { grid-column: auto; }
    }
    @media (max-width: 560px) {
      .page { width: min(100vw - 20px, 1280px); margin-top: 10px; }
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
        This dashboard exposes the five simulated sensor types required by the coursework: speed, acceleration, brake intensity, steering variability, and lane deviation. The fog node aggregates raw events into five-second windows and AWS serves the latest sensor and risk metrics.
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
        <div class="chart-title">Speed Sensor</div>
        <div class="chart-note">Fog window average speed from the speed sensor.</div>
        <div id="speed-chart"></div>
      </div>
      <div class="chart-card">
        <div class="chart-title">Acceleration Sensor</div>
        <div class="chart-note">Average and peak acceleration retained after fog aggregation.</div>
        <div id="acceleration-chart"></div>
      </div>
      <div class="chart-card">
        <div class="chart-title">Brake Intensity Sensor</div>
        <div class="chart-note">Normalized brake intensity derived from the dataset brake signal.</div>
        <div id="brake-chart"></div>
      </div>
      <div class="chart-card">
        <div class="chart-title">Steering Variability Sensor</div>
        <div class="chart-note">Average steering movement and the fog window standard deviation.</div>
        <div id="steering-chart"></div>
      </div>
      <div class="chart-card">
        <div class="chart-title">Lane Deviation Sensor</div>
        <div class="chart-note">Average lane offset retained in each fog window.</div>
        <div id="lane-chart"></div>
      </div>
      <div class="chart-card">
        <div class="chart-title">Risk and Premium</div>
        <div class="chart-note">Fog risk score and pricing multiplier generated from the five sensor streams.</div>
        <div id="risk-chart"></div>
      </div>
      <div class="chart-card chart-card-wide">
        <div class="chart-title">Event Counts</div>
        <div class="chart-note">Discrete events derived by the fog node from brake and lane signals.</div>
        <div id="events-chart"></div>
      </div>
    </section>
  </div>

  <script>
    const CHART_IDS = [
      "speed-chart",
      "acceleration-chart",
      "brake-chart",
      "steering-chart",
      "lane-chart",
      "risk-chart",
      "events-chart"
    ];
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
        ["Avg Speed (km/h)", formatNumber(latest.avg_speed_kmh, 1)],
        ["Avg Accel (m/s2)", formatNumber(latest.avg_acceleration_ms2, 2)],
        ["Brake Intensity", formatNumber(latest.avg_brake_intensity, 2)],
        ["Steering Variability", formatNumber(latest.avg_steering_variability, 2)],
        ["Lane Deviation (m)", formatNumber(latest.avg_lane_deviation_m, 2)],
        ["Risk Score", formatNumber(latest.risk_score, 4)],
        ["Premium Multiplier", formatNumber(latest.premium_multiplier, 4)],
        ["Behavior Class", latest.behavior_class || "--"],
      ];
      kpis.innerHTML = entries.map(([label, value]) => `
        <div class="kpi">
          <div class="kpi-label">${label}</div>
          <div class="kpi-value">${value}</div>
        </div>
      `).join("");
    }

    function emptyLayout() {
      return {
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
    }

    function renderEmptyCharts() {
      const layout = emptyLayout();
      const config = { displayModeBar: false, responsive: true };
      CHART_IDS.forEach((chartId) => Plotly.newPlot(chartId, [], layout, config));
    }

    function sanitizeHistory(history) {
      if (!history || history.length === 0) {
        return [];
      }
      return history
        .filter((item) => item.time && Number(item.avg_speed_kmh) > 0)
        .sort((left, right) => new Date(left.time) - new Date(right.time));
    }

    function buildSeries(history, key, maxGapMs = 15000) {
      const points = [];
      history.forEach((item, index) => {
        const currentTime = new Date(item.time);
        if (index > 0) {
          const previousTime = new Date(history[index - 1].time);
          if ((currentTime - previousTime) > maxGapMs) {
            points.push({ x: history[index - 1].time, y: null });
          }
        }
        points.push({
          x: item.time,
          y: item[key] === undefined ? null : item[key],
        });
      });
      return {
        x: points.map((point) => point.x),
        y: points.map((point) => point.y),
      };
    }

    function baseLayout() {
      return {
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        font: { family: "IBM Plex Sans, sans-serif", color: "#1d2a32" },
        margin: { l: 46, r: 22, t: 18, b: 42 },
        xaxis: { gridcolor: "rgba(29,42,50,0.08)" },
        yaxis: { gridcolor: "rgba(29,42,50,0.08)" },
        hovermode: "x unified",
        legend: { orientation: "h", y: 1.12, x: 0 },
      };
    }

    function renderLineChart(chartId, traces, yaxisTitle) {
      Plotly.newPlot(chartId, traces, {
        ...baseLayout(),
        yaxis: { title: yaxisTitle, gridcolor: "rgba(29,42,50,0.08)" },
      }, { displayModeBar: false, responsive: true });
    }

    function renderCharts(rawHistory, label) {
      const history = sanitizeHistory(rawHistory);
      if (history.length === 0) {
        renderEmptyCharts();
        return;
      }

      const speedSeries = buildSeries(history, "avg_speed_kmh");
      const avgAccelerationSeries = buildSeries(history, "avg_acceleration_ms2");
      const maxAccelerationSeries = buildSeries(history, "max_acceleration_ms2");
      const brakeSeries = buildSeries(history, "avg_brake_intensity");
      const steeringSeries = buildSeries(history, "avg_steering_variability");
      const steeringStddevSeries = buildSeries(history, "steering_stddev");
      const laneSeries = buildSeries(history, "avg_lane_deviation_m");
      const riskSeries = buildSeries(history, "risk_score");
      const premiumSeries = buildSeries(history, "premium_multiplier");
      const harshBrakeSeries = buildSeries(history, "harsh_brake_count");
      const laneDepartureSeries = buildSeries(history, "lane_departure_count");

      renderLineChart("speed-chart", [{
        x: speedSeries.x,
        y: speedSeries.y,
        mode: "lines+markers",
        connectgaps: false,
        line: { color: "#a0432d", width: 4, shape: "spline", smoothing: 0.55 },
        marker: { size: 5, color: "#a0432d" },
        name: `${label} speed`
      }], "km/h");

      renderLineChart("acceleration-chart", [
        {
          x: avgAccelerationSeries.x,
          y: avgAccelerationSeries.y,
          mode: "lines+markers",
          connectgaps: false,
          line: { color: "#2d6a73", width: 4, shape: "spline", smoothing: 0.45 },
          marker: { size: 5, color: "#2d6a73" },
          name: "Average acceleration"
        },
        {
          x: maxAccelerationSeries.x,
          y: maxAccelerationSeries.y,
          mode: "lines",
          connectgaps: false,
          line: { color: "#5a7f2b", width: 3, dash: "dot" },
          name: "Peak acceleration"
        }
      ], "m/s2");

      renderLineChart("brake-chart", [{
        x: brakeSeries.x,
        y: brakeSeries.y,
        mode: "lines+markers",
        connectgaps: false,
        line: { color: "#8f3d1f", width: 4, shape: "spline", smoothing: 0.4 },
        marker: { size: 5, color: "#8f3d1f" },
        name: "Brake intensity"
      }], "ratio");

      renderLineChart("steering-chart", [
        {
          x: steeringSeries.x,
          y: steeringSeries.y,
          mode: "lines+markers",
          connectgaps: false,
          line: { color: "#41644a", width: 4, shape: "spline", smoothing: 0.4 },
          marker: { size: 5, color: "#41644a" },
          name: "Average steering"
        },
        {
          x: steeringStddevSeries.x,
          y: steeringStddevSeries.y,
          mode: "lines",
          connectgaps: false,
          line: { color: "#2d6a73", width: 3, dash: "dot" },
          name: "Steering stddev"
        }
      ], "ratio");

      renderLineChart("lane-chart", [{
        x: laneSeries.x,
        y: laneSeries.y,
        mode: "lines+markers",
        connectgaps: false,
        line: { color: "#6f7f1e", width: 4, shape: "spline", smoothing: 0.4 },
        marker: { size: 5, color: "#6f7f1e" },
        name: "Lane deviation"
      }], "m");

      Plotly.newPlot("risk-chart", [
        {
          x: riskSeries.x,
          y: riskSeries.y,
          mode: "lines+markers",
          connectgaps: false,
          line: { color: "#2d6a73", width: 4, shape: "spline", smoothing: 0.45 },
          marker: { size: 5, color: "#2d6a73" },
          name: "Risk score",
          yaxis: "y1"
        },
        {
          x: premiumSeries.x,
          y: premiumSeries.y,
          mode: "lines",
          connectgaps: false,
          line: { color: "#a0432d", width: 3, dash: "dot" },
          name: "Premium multiplier",
          yaxis: "y2"
        }
      ], {
        ...baseLayout(),
        yaxis: { title: "risk score", range: [0, 1], gridcolor: "rgba(29,42,50,0.08)" },
        yaxis2: {
          title: "premium",
          overlaying: "y",
          side: "right",
          gridcolor: "rgba(0,0,0,0)"
        }
      }, { displayModeBar: false, responsive: true });

      Plotly.newPlot("events-chart", [
        {
          x: harshBrakeSeries.x,
          y: harshBrakeSeries.y,
          type: "bar",
          name: "Harsh brakes",
          marker: { color: "#a0432d" }
        },
        {
          x: laneDepartureSeries.x,
          y: laneDepartureSeries.y,
          type: "bar",
          name: "Lane departures",
          marker: { color: "#2d6a73" }
        }
      ], {
        ...baseLayout(),
        barmode: "group",
        yaxis: { title: "count", gridcolor: "rgba(29,42,50,0.08)" }
      }, { displayModeBar: false, responsive: true });
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
