from ceilometer_local_lib import PoolConnection


def _row_to_alarm_change_model(row):
    return alarm_api_models.AlarmChange(event_id=row.event_id,
                                        alarm_id=row.alarm_id,
                                        type=row.type,
                                        detail=row.detail,
                                        user_id=row.user_id,
                                        project_id=row.project_id,
                                        on_behalf_of=row.on_behalf_of,
                                        timestamp=row.timestamp)


def get_alarm_changes(alarm_id, on_behalf_of,
                      user=None, project=None, type=None,
                      start_timestamp=None, start_timestamp_op=None,
                      end_timestamp=None, end_timestamp_op=None):
    values = []
    sql_query = ('SELECT alarm_change.event_id,'
                 ' alarm.alarm_id, alarm_change.type,'
                 ' alarm_change.detail, users.uuid as user_id,'
                 ' p1.uuid as project_id, p2.uuid as on_behalf_of,'
                 ' alarm_change.timestamp FROM alarm_change'
                 ' JOIN alarm ON alarm_change.alarm_id = alarm.id'
                 ' JOIN users ON alarm_change.user_id = users.id'
                 ' JOIN projects p1 ON alarm_change.project_id = p1.id'
                 ' JOIN projects p2 ON alarm_change.on_behalf_of = p2.id')
    sql_query += ' WHERE alarm.alarm_id = %s'
    values.append(alarm_id)
    if on_behalf_of is not None:
        sql_query += ' AND p2.uuid = %s'
        values.append(on_behalf_of)
    if project is not None:
        sql_query += ' AND p1.uuid = %s'
        values.append(project)
    if type is not None:
        sql_query += ' AND alarm_change.type = %s'
        values.append(type)
    if start_timestamp:
        if start_timestamp_op == 'gt':
            sql_query += ' AND alarm_change.timestamp > %s'
        else:
            sql_query += ' AND alarm_change.timestamp >= %s'
        values.append(start_timestamp)
    if end_timestamp:
        if end_timestamp_op == 'lt':
            sql_query += ' AND alarm_change.timestamp < %s'
        else:
            sql_query += ' AND alarm_change.timestamp <= %s'
    sql_query += ' ORDER BY timestamp DESC;'
    print sql_query
    with PoolConnection() as db:
        db.execute(sql_query, values)
        res = db.fetchall()
    return (_row_to_alarm_change_model(x) for x in res)
get_alarm_changes('9b50ae2f-01fa-4ffc-819d-eb30d4110fa0',
                  '128aef99-3efc-4f15-a93a-1d8e6daed4f0',
                  project='f2cc0bbd-0b72-4e41-b0b7-0059ea2b9f91',
                  start_timestamp='2015-04-01 17:25:52.670853',
                  start_timestamp_op='ge',
                  type='creation',
                  )
