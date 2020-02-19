#!/usr/bin/python
# -*- coding: utf-8 -*-

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import datetime
import pandas as pd
from sqlalchemy import create_engine

db_config = {'user': 'my_user',
             'pwd': 'my_user_password',
             'host': 'localhost',
             'port': 5432,
             'db': 'zen'}

connection_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_config['user'],
                                                         db_config['pwd'],
                                                         db_config['host'],
                                                         db_config['port'],
                                                         db_config['db'])

engine = create_engine(connection_string)

visits_query = ''' SELECT *
                FROM dash_visits 
                '''
engagement_query = ''' SELECT *
                FROM dash_engagement 
                '''

dash_visits = pd.io.sql.read_sql(visits_query, con = engine)
dash_engagement = pd.io.sql.read_sql(engagement_query, con = engine)

dash_visits['dt'] = pd.to_datetime(dash_visits['dt'])
dash_engagement['dt'] = pd.to_datetime(dash_engagement['dt'])

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.layout = html.Div([
    html.Div(
        className='app-header',
        children=[
            html.Div(
                html.H1(children='Дэшборд Yandex Zen')
                    ),
            html.Div(
                html.H3(children='Статистика по времени, возрастным категориям и темам')
                    ),
            html.Div(
                html.Br()
                    )
                ]
            ),

    # селекторы и графики
    html.Div(className='row',
             children=[
                 html.Div(className='six columns',
                          children=[

                              # выбор временного периода
                              html.Label('Период:'),
                              dcc.DatePickerRange(
                                  start_date=dash_visits['dt'].dt.date.min(),
                                  end_date=dash_visits['dt'].dt.date.max(),
                                  display_format='YYYY-MM-DD',
                                  id='date_selector'
                              ),

                              # выбор возрастной категории
                              html.Label('Возрастная категория:'),
                              dcc.Dropdown(
                                  options=[{'label': i, 'value': i} for i in list(dash_visits.age_segment.unique())],
                                  value=[list(dash_visits.age_segment.unique())[0]],
                                  multi=True,
                                  id='age_selector'
                              ),

                            html.H5(children='График истории событий по темам источников:'),
                            html.Br(),
                            dcc.Graph(
                                style={'height': '50vw'},
                                id='topics_by_date'
                            )
                          ]
                 ),

                 html.Div(className='six columns',
                          children=[
                              # выбор темы публикации
                              html.Label('Темы источников:'),
                              dcc.Dropdown(
                                  options=[{'label': i, 'value': i} for i in list(dash_engagement.item_topic.unique())],
                                  value=['Россия'],
                                  multi=True,
                                  id='item_topic_selector'
                              ),

                              html.H5(children='Доля событий'),
                              dcc.Graph(
                                  style={'height': '25vw'},
                                  id='sources'
                              ),

                              html.H5(children='Количество событий по типам'),
                              dcc.Graph(
                                  style={'height': '25vw'},
                                  id='engagement'
                              )
                          ]
                 )
             ]
    )
])

@app.callback(
    [
    Output('topics_by_date', 'figure'),
    Output('sources', 'figure'),
    Output('engagement', 'figure')
    ],
    [
    Input('date_selector', 'start_date'),
    Input('date_selector', 'end_date'),
    Input('age_selector', 'value'),
    Input('item_topic_selector', 'value')
    ]
)

def update_figure(start_date, end_date, age, topics):
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    if start_date == end_date:
        end_date += datetime.timedelta(days=1)
        data_visits = dash_visits.query(
            'dt >= @start_date and dt < @end_date and age_segment.isin(@age) and item_topic.isin(@topics)'
        )
        data_engagement = dash_engagement.query(
            'dt >= @start_date and dt < @end_date and age_segment.isin(@age) and item_topic.isin(@topics)'
        )
    else:
        data_visits = dash_visits.query(
            'dt >= @start_date and dt <= @end_date and item_topic.isin(@topic)'
        )
        data_engagement = dash_engagement.query(
            'dt >= @start_date and dt <= @end_date and age_segment.isin(@age) and item_topic.isin(@topics)'
        )

    grouped_item_topic = data_visits.groupby(['dt', 'item_topic']).agg({'visits': 'sum'}).reset_index()
    updated_item_topic = []
    for topic in topics:
        updated_item_topic += [go.Scatter(
            x=grouped_item_topic.query('item_topic == @topic')['dt'],
            y=grouped_item_topic.query('item_topic == @topic')['visits'],
            mode='lines',
            stackgroup='one',
            name=topic)]

    grouped_source_topic = data_visits.groupby(['source_topic']).agg({'visits': 'sum'}).reset_index()
    updated_source_topic = [go.Pie(labels=grouped_source_topic['source_topic'],
                           values=grouped_source_topic['visits'],
                           name='pie')]

    grouped_engagement = data_engagement.groupby(['event']).agg({'unique_users': 'mean'}).reset_index()
    grouped_engagement['prop_prev'] = pd.Series()
    grouped_engagement = data_engagement.groupby(['event']).agg({'unique_users': 'mean'}).reset_index()
    grouped_engagement['prop_prev'] = pd.Series()
    grouped_engagement.loc[grouped_engagement['event'] == 'show', 'prop_prev'] = 100
    grouped_engagement.loc[grouped_engagement['event'] == 'click', 'prop_prev'] = round(
        float(grouped_engagement.loc[grouped_engagement['event'] == 'click', 'unique_users']) /
        float(grouped_engagement.loc[grouped_engagement['event'] == 'show', 'unique_users']) * 100, 2)
    grouped_engagement.loc[grouped_engagement['event'] == 'view', 'prop_prev'] = round(
        float(grouped_engagement.loc[grouped_engagement['event'] == 'view', 'unique_users']) /
        float(grouped_engagement.loc[grouped_engagement['event'] == 'click', 'unique_users']) * 100, 2)
    updated_engagement = [go.Bar(x=grouped_engagement['event'],
                              y=grouped_engagement['prop_prev'])]

    return [
        {
            'data': updated_item_topic,
            'layout': go.Layout(
                xaxis={'title': 'Дата и время'},
                yaxis={'title': 'Количество событий'}
            )
        },
        {
            'data': updated_source_topic,
            'layout': go.Layout()
        },
        {
            'data': updated_engagement,
            'layout': go.Layout(
                xaxis={'title': 'Тип взаимодействия'},
                yaxis={'title': 'Среднее количество пользователей'}
            )
        }
    ]

if __name__ == '__main__':
    app.run_server(debug=True)