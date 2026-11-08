"""
Plotly Theme Configuration for CUMTA TransitPulse.
Provides a shared, clean, accessible chart styling template.
"""

import plotly.graph_objects as go
import plotly.io as pio

# Clean Color Tokens
COLOR_PAGE = "#F5F7FA"
COLOR_CARD = "#FFFFFF"
COLOR_BORDER = "#E2E8F0"
COLOR_TEXT = "#1F2937"
COLOR_MUTED = "#64748B"
COLOR_ACCENT = "#1D4ED8"
COLOR_PROBLEM = "#DC2626"
COLOR_GOOD = "#16A34A"
COLOR_CAUTION = "#D97706"
COLOR_INFO = "#2563EB"

# Series Semantic Aliases
COLOR_SEARCHES = "#2563EB"
COLOR_TICKETS = "#16A34A"
COLOR_LOST = "#DC2626"
COLOR_CONVERSION = "#D97706"

FONT_FAMILY = "Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif"

clean_template = go.layout.Template(
    layout=go.Layout(
        font=dict(family=FONT_FAMILY, color=COLOR_TEXT, size=12),
        paper_bgcolor=COLOR_CARD,
        plot_bgcolor=COLOR_CARD,
        margin=dict(l=48, r=48, t=24, b=56),
        xaxis=dict(
            showgrid=True,
            gridcolor=COLOR_BORDER,
            gridwidth=1,
            linecolor=COLOR_BORDER,
            tickfont=dict(family=FONT_FAMILY, color=COLOR_MUTED, size=11),
            title_font=dict(family=FONT_FAMILY, color=COLOR_TEXT, size=12),
            zeroline=False,
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor=COLOR_BORDER,
            gridwidth=1,
            linecolor=COLOR_BORDER,
            tickfont=dict(family=FONT_FAMILY, color=COLOR_MUTED, size=11),
            title_font=dict(family=FONT_FAMILY, color=COLOR_TEXT, size=12),
            zeroline=False,
        ),
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.22,
            xanchor="center",
            x=0.5,
            font=dict(family=FONT_FAMILY, color=COLOR_TEXT, size=11),
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor=COLOR_BORDER,
            borderwidth=1,
        ),
        hoverlabel=dict(
            bgcolor=COLOR_CARD,
            bordercolor=COLOR_BORDER,
            font=dict(family=FONT_FAMILY, color=COLOR_TEXT, size=12),
        ),
    )
)

pio.templates["clean_transitpulse"] = clean_template
pio.templates.default = "clean_transitpulse"


def apply_clean_layout(fig, height=360, legend_y=-0.22):
    """Applies clean unified layout settings to any Plotly figure."""
    fig.update_layout(
        template="clean_transitpulse",
        height=height,
        paper_bgcolor=COLOR_CARD,
        plot_bgcolor=COLOR_CARD,
        legend=dict(
            orientation="h",
            yanchor="top",
            y=legend_y,
            xanchor="center",
            x=0.5,
            font=dict(family=FONT_FAMILY, color=COLOR_TEXT, size=11),
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor=COLOR_BORDER,
            borderwidth=1,
        ),
        margin=dict(l=48, r=48, t=16, b=64),
    )
    return fig
