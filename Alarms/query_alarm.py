from ceilometer_local_lib import make_sql_query_from_filter
from ceilometer_local_lib import PoolConnection
from ceilometer_local_lib import Object
from ceilometer_local_lib import transform_filter
from ceilometer_local_lib import transform_orderby

obj = {
    'and': [{'=': {'enabled': 't'}}]}


def query_alarms(filter_expr=None, orderby=None, limit=None):
    if limit == 0:
        return []
    sql_query = 'SELECT * FROM alarm'
    values = []
    if filter_expr:
        sql_where_body, values = transform_filter(filter_expr)
        sql_query += sql_where_body
    if orderby:
        sql_query += transform_orderby(orderby)
    if limit:
        sql_query += ' LIMIT %s'
        values.append(limit)
    with PoolConnection() as db:
        db.execute(sql_query, values)
        res = db.fetchall()
        for s in res:
            print s
            """
            yield alarm_api_models.Alarm(alarm_id=row.alarm_id,
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
            """


query_alarms(filter_expr=None)
