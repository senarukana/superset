from datetime import datetime

from superset import utils
from superset.utils import QueryStatus, QueryResult
import pandas
import requests
import getpass
import json
import StringIO
import time

import iqlclient

TIMESTAMP_COLUMN_NAME = '__timestamp'


def get_datasource(datasource):
    column_names = []
    metric_names = []

    columns = get_columns(datasource)
    for column in columns:
        if column['data_type'] == 'metric' or column['type'] == 'Integer':
            metric_names.append(column['name'])
        column_names.append(column['name'])

    return {
        'edit_url': "",
        'filter_select': True,
        'filterable_cols': utils.choicify(column_names),
        'metrics_combo': utils.choicify(metric_names),
        'gb_cols': utils.choicify(column_names),
        'id': 1,
        'name': datasource,
        'type': "table"
    }


def get_column_names(datasource):
    column_names = []
    columns = get_columns(datasource)
    for column in columns:
        column_names.append(column["name"])
    return column_names


def get_columns(datasource):
    iql = "describe " + datasource
    df = iqlclient.dataframe(iql)
    return df.to_dict(orient="records")


def get_query_str(
        datasource,
        from_dttm, to_dttm,
        groupby=None,
        groupby_time=None,
        where=None,
        metrics=None,
        row_limit=1000,
        filter=None):
    iql = "FROM %s '%s' '%s'" % (datasource, from_dttm, to_dttm)
    if where:
        iql += " WHERE " + where
    if groupby_time or groupby:
        iql += " GROUP BY "
        if groupby_time:
            iql += "time(" + groupby_time + ")"
            if groupby:
                iql += ","
        if groupby:
            iql += ",".join(groupby)
    if metrics:
        iql += " SELECT " + ",".join(metrics)
    if row_limit:
        iql += " LIMIT " + str(1000)
    return iql


def query_obj(datasource,
              from_dttm, to_dttm,
              groupby=None,
              groupby_time=None,
              where=None,
              metrics=None,
              row_limit=None,
              filter=None):
    column_names = []
    if groupby_time:
        column_names.append(TIMESTAMP_COLUMN_NAME)
    if groupby:
        column_names.extend(groupby)
    if metrics:
        column_names.extend(metrics)
    iql = get_query_str(datasource, from_dttm, to_dttm,
                        groupby=groupby, groupby_time=groupby_time,
                        where=where, metrics=metrics, row_limit=row_limit, filter=filter)
    return query_with_column_names(iql, column_names, groupby_time is not None)


def strip_query(iql):
    return " ".join(iql.split())


def transfer_timeseries(df):
    for i, row in df.iterrows():
        raw_timestamp_str = row[TIMESTAMP_COLUMN_NAME]
        timestamp_str = raw_timestamp_str[1:raw_timestamp_str.find(',')]
        timestamp = time.mktime(time.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S"))
        df.set_value(i, TIMESTAMP_COLUMN_NAME, timestamp)
    return df


def query_with_column_names(iql, column_names, timeseries=False):
    iql = strip_query(iql)
    print(">>> query iql: " + iql)
    qry_start_dttm = datetime.now()
    status = QueryStatus.SUCCESS
    error_message = None
    df = None
    try:
        df = _query(iql, column_names)
    except Exception as e:
        status = QueryStatus.FAILED
        error_message = str(e)
        print error_message
    if timeseries:
        df = transfer_timeseries(df)

    # print(">> query result: {}".format(df.to_dict(orient="records")))
    return QueryResult(
        status=status,
        df=df,
        duration=datetime.now() - qry_start_dttm,
        query=iql,
        error_message=error_message)


def query(iql):
    iql = strip_query(iql)
    column_names = []


# TODO: zheli
def values_for_column(datasource, column_name, from_dttm, to_dttm):
    return ["us", "jp"]
    # iql = get_query_str(datasource, from_dttm, to_dttm, groupby=[column_name])
    # return query(iql)


def _query(query_string, column_names, url='https://squall.indeed.com/iql/query',
           username=None, client=None, v=""):
    if not username:
        username = getpass.getuser()
    if not client:
        client = 'iqlhackathon'

    params = {'sync': 'sync', 'q': query_string, 'username': username, 'client': client, 'v': v}

    r = requests.post(url, data=params)

    if not r.ok:
        try:
            message = json.loads(r.text)['message']
        except Exception as e:
            message = r.text
        raise Exception(message)
    else:
        return pandas.read_csv(StringIO.StringIO(r.text), sep='\t', quoting=3, header=None, names=column_names,
                               encoding='utf-8')
