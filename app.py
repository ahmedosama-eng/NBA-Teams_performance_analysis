import dash
from dash import dcc
from dash import html,dash_table
from ETL_pipline import seasons_url,Extract_data_frame,transform_data
import plotly.graph_objects as go
from dash.dependencies import Input, Output
import pandas as pd 


teams_final_data=pd.read_csv(r'F:\studying machine\airflow Dag using docker\data\data.csv')
teams_final_data.set_index(['Team', 'Years'], inplace=True)


app = dash.Dash()
app.layout = html.Div(
    style={
        'fontFamily': 'Arial, sans-serif',
        'backgroundImage': 'linear-gradient(rgba(0, 0, 0, 0.5), rgba(0, 0, 0, 0.5)),url("https://cdn.nba.com/manage/2020/02/AS20-Starters-T1-FrostyChiTowngraphic.jpg")',
        'backgroundSize': 'cover',
        'backgroundRepeat': 'no-repeat',
        'backgroundAttachment': 'fixed',
        'textAlign': 'center',
        'padding': '20px'
    },
    children=[
        html.Div(
            [
                html.H1("NBA Team Performance Across Last 5 Seasons")
            ],
            style={
                'color': 'white'
            }
        ),
        html.Div(
            [
                html.Label("Select Team:", style={'color': 'black'}),
                dcc.Dropdown(
                    id='team-dropdown',
                    options=[{'label': team, 'value': team} for team in list(teams_final_data.index.get_level_values('Team').unique())],
                    value='Boston Celtics'
                ),
                html.Label("Select Feature:", style={'color': 'black', 'marginTop': '10px'}),
                dcc.Dropdown(
                    id='feature-dropdown',
                    options=[{'label': col, 'value': col} for col in list(teams_final_data.columns)],
                    value='Rk'
                )
            ],
            style={
                'width': '25%',
                'margin': '0 auto',
                'padding': '10px',
                'background': 'rgba(255, 255, 255, 0.8)',
                'borderRadius': '10px',
                'marginBottom': '20px'
            },
        ),
        html.Div(
            [
                html.Div(
                    [
                        dcc.Graph(id='scatter_graph')
                    ],
                    style={
                        'width': '45%',
                        'padding': '10px',
                        'background': 'rgba(255, 255, 255, 0.8)',
                        'borderRadius': '10px',
                        'marginBottom': '20px',
                        'color': 'white'
                    },
                ),
                html.Div(
                    [
                        dash_table.DataTable(
                            id='data-table',
                            columns=[
                                {"name": "Year", "id": "Year"},
                                {"name": "Value", "id": "Value"}
                            ],
                            style_table={'width': '100%'}
                        )
                    ],
                    style={
                        'width': '45%',
                        'padding': '10px',
                        'background': 'rgba(255, 255, 255, 0.8)',
                        'borderRadius': '10px',
                        'marginBottom': '20px',
                        'color': 'black'
                    },
                )
            ],
            style={
                'display': 'flex',
                'justifyContent': 'space-between',
                'alignItems': 'center',
                'width': '90%',
                'margin': '0 auto'
            }
        )
    ]
)

@app.callback(
    [Output('scatter_graph', 'figure'),
     Output('data-table', 'data')],
    [Input('team-dropdown', 'value'),
     Input('feature-dropdown', 'value')]
)
def update_graphs(selected_team, selected_feature):
    x = list(teams_final_data.loc[selected_team].index)
    if selected_feature == 'Rk':
        y = teams_final_data.loc[selected_team, selected_feature].values.astype(int).flatten().tolist()
    else:
        y = teams_final_data.loc[selected_team, selected_feature].values.astype(float).flatten().tolist()

    y_labels = teams_final_data.loc[selected_team, selected_feature].values.flatten().tolist()

    scatter_fig = go.Figure(data=go.Scatter(x=x, y=y, name='Scatter Plot'))
    scatter_fig.update_layout(
        title=f'{selected_team} - {selected_feature} Over Time',
        xaxis_title='Years',
        yaxis_title=selected_feature,
        xaxis=dict(
            tickvals=[2020, 2021, 2022, 2023, 2024],
            ticktext=['2020', '2021', '2022', '2023', '2024']
        ),
        yaxis=dict(
            tickvals=y,
            ticktext=y_labels,
            autorange='reversed' if selected_feature == 'Rk' else True
        )
    )
    table_data = [{"Year": year, "Value": value} for year, value in zip(x, y)]
    return scatter_fig, table_data

if __name__ == "__main__":
    app.run_server()