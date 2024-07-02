from osgeo.gdalnumeric import *
from dash import Dash, html, dcc, Output, Input
import plotly.express as px
from plotly.subplots import make_subplots
import numpy as np
import os


app = Dash(__name__)


@app.callback(
    [Output(component_id='rgb', component_property='figure'),
     Output(component_id='mean_ndvi', component_property='figure')],
    Input('none', 'children')
)
def update_images(none):
    years = os.listdir("spark-output")
    years = [1999, 2005, 2008, 2015, 2021]

    figs_rgb = []
    figs_ndvi = []
    plot_titles = []
    mean_ndvi_list = []

    for year in years:
        ndvi = np.load(f"spark-output/{year}/ndvi_min.npy")
        if int(year) < 2019:
            rgb = np.array([
                np.load(f"first-stage-output/{year}/green_band.npy"),
                np.load(f"first-stage-output/{year}/red_band.npy"),
                np.load(f"first-stage-output/{year}/blue_band.npy")
            ])
        else:
            rgb = np.array([
                np.load(f"first-stage-output/{year}/blue_band.npy"),
                np.load(f"first-stage-output/{year}/green_band.npy"),
                np.load(f"first-stage-output/{year}/red_band.npy")
            ])
        rgb = np.swapaxes(rgb, 0, 2)
        rgb = np.rot90(rgb)
        with open(f"spark-output/{year}/mean_ndvi.txt") as f:
            mean_ndvi = float(f.read())

        figs_rgb.append(px.imshow(rgb))
        figs_ndvi.append(px.imshow(ndvi))
        plot_titles.append(f"Year {year} - mean NDVI: {mean_ndvi:.3f}")
        mean_ndvi_list.append(mean_ndvi)

    n_images = len(years)
    subplots_rgb_ndvi = make_subplots(rows=2, cols=n_images, horizontal_spacing=0.05, vertical_spacing=0.1,
                                      subplot_titles=plot_titles)
    for i in range(n_images):
        subplots_rgb_ndvi.add_trace((figs_rgb[i].data[0]), row=1, col=i + 1)
        subplots_rgb_ndvi.add_trace((figs_ndvi[i].data[0]), row=2, col=i + 1)
    subplots_rgb_ndvi.update_layout(width=400 * n_images, height=800, coloraxis={"colorscale": "Greens"})
    subplots_rgb_ndvi.update_xaxes(visible=False)
    subplots_rgb_ndvi.update_yaxes(visible=False)

    scatter_mean_ndvi = px.scatter(x=years, y=mean_ndvi_list, trendline="lowess")
    scatter_mean_ndvi.update_layout(xaxis_title_text="Year", yaxis_title_text="Mean NDVI")
    scatter_mean_ndvi.update_xaxes(dtick=1)
    return subplots_rgb_ndvi, scatter_mean_ndvi


app.layout = html.Div(children=[
    html.H1(children='NDVI analysis', style={'textAlign': 'center'}),
    dcc.Graph(
        id='rgb'
        # figure=subplots_rgb
    ),
    dcc.Graph(
        id='mean_ndvi'
    ),
    html.Div(id='none', children=[], style={'display': 'none'}),
])

app.run_server(debug=False)


# subplots_ndvi = make_subplots(cols=n_images, horizontal_spacing=0.05, vertical_spacing=0.1,
#                               subplot_titles=plot_titles)
# for i in range(n_images):
#     subplots_ndvi.add_trace((figs_ndvi[i].data[0]), row=1, col=i + 1)
# subplots_ndvi.update_layout(title={'text': "NDVI", 'x': 0.5, 'xanchor': 'center', 'yanchor': 'top'},
#                             width=470 * n_images, height=500, coloraxis={"colorscale": "Greens"})
# subplots_ndvi.update_xaxes(visible=False)
# subplots_ndvi.update_yaxes(visible=False)
# fig2.update_layout(coloraxis={"colorscale": "Greens"})
