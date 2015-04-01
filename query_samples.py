from ceilometer_local_lib import make_sql_query_from_filter
from ceilometer_local_lib import PoolConnection
from ceilometer_local_lib import Object
from ceilometer_local_lib import transform_filter
from ceilometer_local_lib import transform_orderby
obj = {
    'and': [{'=': {'counter_name': 'cpu_util'}}, {'>': {'counter_volume': 5}},
            {'=': {'resource_metadata.image.name': 'cirros-0.3.3-x86_64'}}]}

obj_complex = {"and":
               [{"and":
                 [{"=": {"counter_name": "cpu_util"}},
                  {">": {"counter_volume": 0.23}},
                  {"<": {"counter_volume": 0.26}}]},
                {"or":
                 [{"and":
                   [{">": {"timestamp": "2013-12-01T18:00:00"}},
                    {"<": {"timestamp": "2013-12-01T18:15:00"}}]},
                  {"and":
                   [{">": {"timestamp": "2013-12-01T18:30:00"}},
                    {"<": {"timestamp": "2013-12-01T18:45:00"}}]}]}]}

orderby = [{"counter_volume": "DESC"}]


def query_samples(filter_expr=None, orderby=None, limit=None):
    sql_query = ('SELECT * FROM ('
                 'SELECT samples.id as sam_id, meters.name as counter_name,'
                 ' meters.type as counter_type, meters.unit as counter_unit,'
                 ' samples.volume as counter_volume, resources.resource_id,'
                 ' sources.name as source_id, users.uuid as user_id,'
                 ' projects.uuid as project_id, samples.metadata as metadata,'
                 ' resources.id, samples.timestamp, samples.message_id,'
                 ' samples.message_signature, samples.recorded_at'
                 ' FROM samples'
                 ' JOIN meters ON samples.meter_id = meters.id'
                 ' JOIN resources ON samples.resource_id = resources.id'
                 ' JOIN users ON samples.user_id = users.id'
                 ' JOIN projects ON samples.project_id = projects.id'
                 ' JOIN sources ON samples.source_id = sources.id) as c')
    values = []
    if filter_expr:
        sql_where_body, values = transform_filter(filter_expr)
        sql_query += sql_where_body
    if orderby:
        sql_query += transform_orderby(orderby)
    if limit:
        sql_query += ' LIMIT %s'
        values.append(limit)
    print sql_query
    print values
    with PoolConnection() as db:
        db.execute(sql_query, values)
        res = db.fetchall()
        for s in res:
            print s
            """
            yield api_models.Sample(
                source=s.source_id,
                counter_name=s.counter_name,
                counter_type=s.counter_type,
                counter_unit=s.counter_unit,
                counter_volume=s.counter_volume,
                user_id=s.user_id,
                project_id=s.project_id,
                resource_id=s.resource_id,
                timestamp=s.timestamp,
                recorded_at=s.recorded_at,
                resource_metadata=s.metadata,
                message_id=s.message_id,
                message_signature=s.message_signature,
                )
            """

query_samples(filter_expr=obj, orderby=orderby, limit=2)
