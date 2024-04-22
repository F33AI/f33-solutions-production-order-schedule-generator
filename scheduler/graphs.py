from collections import defaultdict
import plotly.graph_objects as go
import plotly.express as px
from scheduler.datastructures import Experiment, Scenario, JobStatus


def render_gantt_chart(scenario: Scenario):
    fig = px.timeline(scenario.results, x_start="Start", x_end="Finish", y="Task",
                      color="Resource", title="Gantt Chart", hover_data="Hoverdata")
    fig.update_yaxes(categoryorder="category descending")
    return fig


def render_radar_plot(experiment: Experiment):

    if len(experiment.scenarios) == 0:
        return

    all_metrics = list(experiment.scenarios[0].metrics.keys())
    normalization = defaultdict(lambda: 1)

    for scenario in experiment.scenarios:
        if scenario.status != JobStatus.State.SUCCEEDED or len(scenario.metrics) == 0:
            continue
        for metric in all_metrics:
            normalization[metric] = max(scenario.metrics[metric], normalization[metric])

    fig = go.Figure()
    for scenario in experiment.scenarios:

        if scenario.status != JobStatus.State.SUCCEEDED or len(scenario.metrics) == 0:
            continue

        points = [round(scenario.metrics[metric] / normalization[metric], 2) \
                  for metric in all_metrics]
        plot = go.Scatterpolar(r=points, theta=all_metrics, fill="toself",
                               name=scenario.scenario_name)
        fig.add_trace(plot)

    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 1]
            )
        ),
        showlegend=True
    )

    return fig