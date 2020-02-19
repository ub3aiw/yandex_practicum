#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import getopt
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta

if __name__ == "__main__":

    unixOptions = "s:e"
    gnuOptions = ["start_dt=", "end_dt="]

    fullCmdArguments = sys.argv
    argumentList = fullCmdArguments[1:]

    try:
        arguments, values = getopt.getopt(argumentList, unixOptions, gnuOptions)
    except getopt.error as err:
        print(str(err))
        sys.exit(2)

    start_dt = ''
    end_dt = ''

    for currentArgument, currentValue in arguments:
        if currentArgument in ("-s", "--start_dt"):
            start_dt = currentValue
        elif currentArgument in ("-e", "--end_dt"):
            end_dt = currentValue

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

    query = ''' SELECT event_id, age_segment, event, item_id, item_topic, item_type, source_id, source_topic,
            source_type, user_id, TO_TIMESTAMP(ts / 1000) AT TIME ZONE 'Etc/UTC' AS dt
                FROM log_raw 
                WHERE TO_TIMESTAMP(ts / 1000) AT TIME ZONE 'Etc/UTC' BETWEEN '{}'::TIMESTAMP AND '{}'::TIMESTAMP
            '''.format(start_dt, end_dt)

    raw = pd.io.sql.read_sql(query, con=engine, index_col='event_id')

    raw['dt'] = pd.to_datetime(raw['dt']).dt.round('min')

    dash_engagement = raw.groupby(['dt', 'item_topic', 'event', 'age_segment'])\
        .agg({'user_id': 'nunique'}).reset_index()

    dash_engagement = dash_engagement.rename(columns={'user_id': 'unique_users'})

    dash_visits = raw.groupby(['item_topic', 'source_topic', 'age_segment', 'dt'])\
        .agg({'event': 'count'}).reset_index()

    dash_visits = dash_visits.rename(columns={'event': 'visits'})

    tables = {'dash_engagement': dash_engagement,
              'dash_visits': dash_visits}

    for key, value in tables.items():
        query = '''DELETE FROM {} WHERE dt::TIMESTAMP BETWEEN '{}'::TIMESTAMP AND '{}'::TIMESTAMP
                '''.format(key, start_dt, end_dt)

        engine.execute(query)

        value.to_sql(name=key, con=engine, if_exists='append', index=False)

    print('Success')