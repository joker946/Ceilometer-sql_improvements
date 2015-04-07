from ceilometer_local_lib import PoolConnection
import json
import psycopg2
from psycopg2.extras import Json
import datetime


def write_alarm():
    f = {
        'alarm_actions': [
            u'log: //'
        ],
        'ok_actions': [

        ],
        'description': u'instancerunninghot',
        'state': 'insufficientdata',
        'timestamp': datetime.datetime(2015, 4, 7, 19, 20, 5, 859475),
        'enabled': True,
        'state_timestamp': datetime.datetime(2015, 4, 7, 19, 20, 5, 859475),
        'rule': {
            'meter_name': u'cpu_util',
            'evaluation_periods': 3,
            'period': 600,
            'statistic': 'avg',
            'threshold': 70.0,
            'query': [
                {
                    'field': u'resource_id',
                    'type': u'',
                    'value': u'465e284e-7351-4130-b668-bfa7980969f4',
                    'op': 'eq'
                }
            ],
            'comparison_operator': 'gt',
            'exclude_outliers': False
        },
        'alarm_id': u'81ac9221-e5e3-4a56-bbc6-7d533e271a58',
        'time_constraints': [

        ],
        'insufficient_data_actions': [

        ],
        'repeat_actions': False,
        'user_id': u'3d622ea5a70a42d3aae549ddfc1ef355',
        'project_id': u'',
        'type': 'threshold',
        'name': u'cpu_high1'
    }
    data = f
    user_id = None
    project_id = None
    if data['user_id']:
        user_id_query = 'SELECT id from users WHERE uuid = %s'
        with PoolConnection() as db:
            db.execute(user_id_query, [data['user_id']])
            response = db.fetchone()
            if response:
                user_id = response[0]
            else:
                raise Exception('Could not find required user_id.')

    if data['project_id']:
        project_id_query = 'SELECT id from projects WHERE uuid = %s'
        with PoolConnection() as db:
            db.execute(project_id_query, [data['project_id']])
            response = db.fetchone()
            if response:
                project_id = response[0]
            else:
                raise Exception('Could not find required project_id.')

    values = [data['enabled'], data['name'], data['type'], data['description'],
              data['timestamp'], user_id, project_id, data['state'],
              Json(data['ok_actions']), Json(data['alarm_actions']),
              Json(data['insufficient_data_actions']), data['repeat_actions'],
              Json(data['rule']), Json(data['time_constraints']),
              data['state_timestamp'], data['alarm_id']]
    print values
    query = ('INSERT INTO alarm (enabled, name, type, description,'
             ' timestamp, user_id, project_id, state, ok_actions,'
             ' alarm_actions, insufficient_data_actions,'
             ' repeat_actions, rule,'
             ' time_constraints, state_timestamp, alarm_id) VALUES (%s, %s,'
             ' %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
             ' RETURNING enabled, name, type, description,'
             ' timestamp, user_id, project_id, state, ok_actions,'
             ' alarm_actions, insufficient_data_actions,'
             ' repeat_actions, rule,'
             ' time_constraints, state_timestamp, alarm_id;')
    with PoolConnection() as db:
        db.execute(query, values)
        res = db.fetchall()
        print res

write_alarm()
