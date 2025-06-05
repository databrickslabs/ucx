import pandas as pd
from dash import Dash, dcc, html, Input, Output
import dash_bootstrap_components as dbc
import subprocess

# Initialize the Dash app with Bootstrap styling
dash_app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Define the app layout
dash_app.layout = dbc.Container([
    dbc.Button("Install UCX App", id="install-button", color="primary", className="mt-3")
], fluid=True)

@dash_app.callback(
    Output("install-button", "children"),
    Input("install-button", "n_clicks")
)
def install_ucx_app(n_clicks):
    if n_clicks:
        script_path = os.path.join("src", "databricks", "ucx", "install.py")
        subprocess.run(["python", script_path])
        return "UCX App Installed"
    return "Install UCX App"

if __name__ == '__main__':
    dash_app.run_server(debug=True)
