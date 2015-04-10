from ceilometer_local_lib import PoolConnection
from psycopg2.extras import Json
import datetime

change = {
    'on_behalf_of': u'f2cc0bbd0b724e41b0b70059ea2b9f92',
    'user_id': u'3d622ea5a70a42d3aae549ddfc1ef355',
    'event_id': '13930c20-4028-4eef-8855-8d7dca4b74b5',
    'timestamp': datetime.datetime(2015, 4, 1, 17, 25, 52, 670853),
    'detail': {
        "alarm_actions": [
            "log://"
        ],
        "user_id": "3d622ea5a70a42d3aae549ddfc1ef355",
        "name": "cpu_high1",
        "state": "insufficient data",
        "timestamp": "2015-04-01T17:25:52.670853",
        "enabled": True,
        "state_timestamp": "2015-04-01T17:25:52.670853",
        "rule": {
            "meter_name": "cpu_util",
            "evaluation_periods": 3,
            "period": 600,
            "statistic": "avg",
            "threshold": 70.0,
            "query": [
                {
                   "field": "resource_id",
                   "type": "",
                   "value": "465e284e-7351-4130-b668-bfa7980969f4",
                   "op": "eq"
                }
            ],
            "comparison_operator": "gt",
            "exclude_outliers": False
        },
        "alarm_id": "341b2124-808e-43c2-9e64-35199ca0f834",
        "time_constraints": [

        ],
        "insufficient_data_actions": [

        ],
        "repeat_actions": False,
        "ok_actions": [

        ],
        "project_id": "",
        "type": "threshold",
        "description": "instance running hot"
    },
    'alarm_id': u'81ac9221-e5e3-4a56-bbc6-7d533e271a58',
    'project_id': u'f2cc0bbd0b724e41b0b70059ea2b9f91',
    'type': 'creation'
}


def record_alarm_change(alarm_change):
    values = [alarm_change['event_id'], alarm_change['type'],
              Json(alarm_change['detail']), alarm_change['timestamp'],
              alarm_change['alarm_id'], alarm_change['project_id'],
              alarm_change['user_id']]
    with PoolConnection() as db:
        db.execute('SELECT id FROM projects WHERE uuid = %s',
                   [alarm_change['on_behalf_of']])
        on_behalf_of_query = db.fetchone().id
    values.insert(1, on_behalf_of_query)
    print values
    sql_query = ('INSERT INTO alarm_change (event_id, alarm_id, on_behalf_of,'
                 ' project_id, user_id, type, detail, timestamp)'
                 ' SELECT %s, alarm.id, %s, projects.id, users.id,'
                 ' %s, %s, %s FROM alarm, projects, users'
                 ' WHERE alarm.alarm_id = %s AND projects.uuid = %s AND'
                 ' users.uuid = %s')
    #with PoolConnection() as db:
        #db.execute(sql_query, values)

record_alarm_change(change)
