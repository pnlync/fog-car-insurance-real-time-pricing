from __future__ import annotations

import os

import pandas as pd
import plotly.express as px
from dash import Dash, Input, Output, State, ctx, dcc, html

from .demo_control import start_demo_session, stop_demo_session
from .queries import (
    get_demo_session,
    latest_demo_metrics,
    latest_metrics,
    list_vehicles,
    recent_demo_metrics,
    recent_metrics,
)

app = Dash(__name__)
server = app.server


def placeholder_figure(title: str):
    return px.line(title=title)


def build_kpi_cards(latest: dict | None) -> list:
    if not latest:
        return [html.Div("No telemetry available yet.", className="kpi-card")]

    cards = [
        ("Avg Speed", latest["avg_speed_kmh"]),
        ("Avg Acceleration", latest["avg_acceleration_ms2"]),
        ("Brake Intensity", latest["avg_brake_intensity"]),
        ("Steering Variability", latest["avg_steering_variability"]),
        ("Lane Deviation", latest["avg_lane_deviation_m"]),
        ("Risk Score", latest["risk_score"]),
        ("Premium Multiplier", latest["premium_multiplier"]),
        ("Behavior Class", latest["behavior_class"]),
    ]
    return [
        html.Div(
            [
                html.Div(label, className="kpi-label"),
                html.Div(value, className="kpi-value"),
            ],
            className="kpi-card",
        )
        for label, value in cards
    ]


def build_demo_status(session: dict | None):
    if not session:
        return "Demo session inactive."
    if session.get("status") in {"UNCONFIGURED", "ERROR", "NOT_FOUND"}:
        return html.Div(
            session.get("message", f"Demo error: {session.get('status', 'UNKNOWN')}"),
            className="status-error",
        )

    return html.Div(
        [
            html.Span(
                f"Session: {session.get('demo_session_id', '-')}",
                className="status-id",
            ),
            html.Span(f"Status: {session.get('status', '-')}", className="status-pill"),
            html.Span(
                (
                    f"Ticks: {session.get('ticks_completed', '0')}/"
                    f"{session.get('max_ticks', '0')}"
                ),
                className="status-meta",
            ),
        ],
        className="status-row",
    )


app.layout = html.Div(
    className="page",
    children=[
        html.H1("Real-Time Car Insurance Pricing Dashboard"),
        html.P(
            "Fog-processed telemetry dashboard for speed, acceleration, brake, "
            "steering, lane, and pricing metrics."
        ),
        dcc.Store(id="demo-session-store", storage_type="session"),
        html.Div(
            className="demo-panel",
            children=[
                html.Div(
                    className="demo-actions",
                    children=[
                        html.Button("Start Demo", id="start-demo", n_clicks=0),
                        html.Button("Stop Demo", id="stop-demo", n_clicks=0),
                    ],
                ),
                html.Div(id="demo-status", children=build_demo_status(None)),
            ],
        ),
        html.Div(
            className="controls",
            children=[
                dcc.Dropdown(id="vehicle-id", placeholder="Select a vehicle"),
                dcc.Interval(id="refresh", interval=5000, n_intervals=0),
            ],
        ),
        html.Div(id="kpi-cards", className="kpi-grid"),
        dcc.Graph(id="speed-chart"),
        dcc.Graph(id="acceleration-chart"),
        dcc.Graph(id="brake-chart"),
        dcc.Graph(id="steering-chart"),
        dcc.Graph(id="lane-chart"),
        dcc.Graph(id="risk-chart"),
        dcc.Graph(id="events-chart"),
    ],
)


@app.callback(
    Output("demo-session-store", "data"),
    Output("demo-status", "children"),
    Input("start-demo", "n_clicks"),
    Input("stop-demo", "n_clicks"),
    Input("refresh", "n_intervals"),
    State("demo-session-store", "data"),
)
def manage_demo_session(
    _start_clicks: int,
    _stop_clicks: int,
    _refresh_tick: int,
    current_session: dict | None,
):
    triggered = ctx.triggered_id

    if triggered == "start-demo":
        if current_session and current_session.get("demo_session_id"):
            stop_demo_session(current_session["demo_session_id"])
        session = start_demo_session(duration_minutes=2, vehicle_count=1)
        return session, build_demo_status(session)

    if triggered == "stop-demo" and current_session and current_session.get(
        "demo_session_id"
    ):
        session = stop_demo_session(current_session["demo_session_id"])
        return session, build_demo_status(session)

    if current_session and current_session.get("demo_session_id"):
        session = (
            get_demo_session(current_session["demo_session_id"]) or current_session
        )
        return session, build_demo_status(session)

    return None, build_demo_status(None)


@app.callback(
    Output("vehicle-id", "options"),
    Output("vehicle-id", "value"),
    Input("refresh", "n_intervals"),
    Input("vehicle-id", "value"),
)
def refresh_vehicle_options(_tick: int, current_value: str | None):
    vehicles = list_vehicles()
    options = [{"label": vehicle_id, "value": vehicle_id} for vehicle_id in vehicles]
    selected = (
        current_value
        if current_value in vehicles
        else (vehicles[0] if vehicles else None)
    )
    return options, selected


@app.callback(
    Output("kpi-cards", "children"),
    Output("speed-chart", "figure"),
    Output("acceleration-chart", "figure"),
    Output("brake-chart", "figure"),
    Output("steering-chart", "figure"),
    Output("lane-chart", "figure"),
    Output("risk-chart", "figure"),
    Output("events-chart", "figure"),
    Input("refresh", "n_intervals"),
    Input("vehicle-id", "value"),
    Input("demo-session-store", "data"),
)
def refresh_dashboard(
    _tick: int, vehicle_id: str | None, demo_session: dict | None
):
    if demo_session and demo_session.get("demo_session_id"):
        session_id = demo_session["demo_session_id"]
        latest = latest_demo_metrics(session_id)
        history = pd.DataFrame(recent_demo_metrics(session_id))
        label = f"Demo Session {session_id[:8]}"
    elif vehicle_id:
        latest = latest_metrics(vehicle_id)
        history = pd.DataFrame(recent_metrics(vehicle_id))
        label = vehicle_id
    else:
        return (
            build_kpi_cards(None),
            placeholder_figure("Speed over time"),
            placeholder_figure("Acceleration over time"),
            placeholder_figure("Brake intensity over time"),
            placeholder_figure("Steering variability over time"),
            placeholder_figure("Lane deviation over time"),
            placeholder_figure("Risk score over time"),
            placeholder_figure("Event counts"),
        )

    if history.empty:
        return (
            build_kpi_cards(latest),
            placeholder_figure("Speed over time"),
            placeholder_figure("Acceleration over time"),
            placeholder_figure("Brake intensity over time"),
            placeholder_figure("Steering variability over time"),
            placeholder_figure("Lane deviation over time"),
            placeholder_figure("Risk score over time"),
            placeholder_figure("Event counts"),
        )

    acceleration_fig = px.line(
        history,
        x="time",
        y=["avg_acceleration_ms2", "max_acceleration_ms2"],
        title=f"Acceleration Sensor ({label})",
    )
    brake_fig = px.line(
        history,
        x="time",
        y="avg_brake_intensity",
        title=f"Brake Intensity Sensor ({label})",
    )
    steering_fig = px.line(
        history,
        x="time",
        y=["avg_steering_variability", "steering_stddev"],
        title=f"Steering Sensor ({label})",
    )
    lane_fig = px.line(
        history,
        x="time",
        y="avg_lane_deviation_m",
        title=f"Lane Deviation Sensor ({label})",
    )
    speed_fig = px.line(
        history,
        x="time",
        y="avg_speed_kmh",
        title=f"Speed Sensor ({label})",
    )
    risk_fig = px.line(
        history,
        x="time",
        y=["risk_score", "premium_multiplier"],
        title=f"Risk and Premium ({label})",
    )
    events_df = history.melt(
        id_vars=["time"],
        value_vars=["harsh_brake_count", "lane_departure_count"],
        var_name="metric",
        value_name="count",
    )
    events_fig = px.bar(
        events_df,
        x="time",
        y="count",
        color="metric",
        barmode="group",
        title=f"Brake and Lane Events ({label})",
    )
    return (
        build_kpi_cards(latest),
        speed_fig,
        acceleration_fig,
        brake_fig,
        steering_fig,
        lane_fig,
        risk_fig,
        events_fig,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")), debug=False)
