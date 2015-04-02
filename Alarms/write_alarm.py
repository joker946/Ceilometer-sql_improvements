from ceilometer_local_lib import PoolConnection
import json
import psycopg2
from psycopg2.extras import Json
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)


def write_alarm():
    f = open('alarm_example_json.json')
    data = json.load(f)
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
              data['ok_actions'], Json(data['alarm_actions']),
              data['insufficient_data_actions'], data['repeat_actions'],
              data['rule'], data['time_constraints'], data['state_timestamp'],
              data['alarm_id']]
    print values
    query = ('INSERT INTO alarm (enabled, name, type, description,'
             ' timestamp, user_id, project_id, state, ok_actions,'
             ' alarm_actions, insufficient_data_actions,'
             ' repeat_actions, rule,'
             ' time_constraints, state_timestamp, alarm_id) VALUES (%s, %s,'
             ' %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);')
    with PoolConnection() as db:
        db.execute(query, values)

write_alarm()
