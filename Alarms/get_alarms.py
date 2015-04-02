from ceilometer_local_lib import PoolConnection


def _row_to_alarm_model(row):
    return alarm_api_models.Alarm(alarm_id=row.alarm_id,
                                  enabled=row.enabled,
                                  type=row.type,
                                  name=row.name,
                                  description=row.description,
                                  timestamp=row.timestamp,
                                  user_id=row.user_id,
                                  project_id=row.project_id,
                                  state=row.state,
                                  state_timestamp=row.state_timestamp,
                                  ok_actions=row.ok_actions,
                                  alarm_actions=row.alarm_actions,
                                  insufficient_data_actions=(
                                      row.insufficient_data_actions),
                                  rule=row.rule,
                                  time_constraints=row.time_constraints,
                                  repeat_actions=row.repeat_actions)


def get_alarms(name=None, user=None, state=None, meter=None,
               project=None, enabled=None, alarm_id=None, pagination=None):
    sql_query = 'SELECT * FROM alarm'
    values = []
    if name:
        sql_query += ' AND name = %s'
        values.append(name)
    if enabled:
        sql_query += ' AND enabled = %s'
        values.append(enabled)
    if user:
        subq = 'SELECT id FROM users WHERE uuid = %s'
        with PoolConnection() as db:
            db.execute(subq, [user])
            res = db.fetchone()
        if res:
            values.append(res[0])
        else:
            raise Exception('Could not find any users with requested uuid')
        sql_query += ' AND user_id = %s'
    if project:
        subq = 'SELECT id FROM projects WHERE uuid = %s'
        with PoolConnection() as db:
            db.execute(subq, [project])
            res = db.fetchone()
        if res:
            values.append(res[0])
        else:
            raise Exception('Could not find any projects with requested uuid')
        sql_query += ' AND project_id = %s'
    if alarm_id:
        sql_query += ' AND alarm_id = %s'
        values.append(alarm_id)
    if state:
        sql_query += ' AND state = %s'
        values.append(state)

    sql_query = sql_query.replace(' AND', ' WHERE', 1)
    with PoolConnection() as db:
        db.execute(sql_query, values)
        res = db.fetchall()
    return (_row_to_alarm_model(x) for x in res)

print get_alarms(name='cpu_high', user='3d622ea5-a70a-42d3-aae5-49ddfc1ef355')
