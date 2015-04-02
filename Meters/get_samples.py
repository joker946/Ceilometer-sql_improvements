from ceilometer_local_lib import make_sql_query_from_filter
from ceilometer_local_lib import PoolConnection
from ceilometer_local_lib import Object
from dateutil import parser


def get_samples(sample_filter, limit=None):
    """Return an iterable of model.Sample instances.

   :param sample_filter: Filter.
   :param limit: Maximum number of results to return.
   """
    if limit == 0:
        return []
    query = ("SELECT sources.name as source_id, meters.name as counter_name,"
             " meters.type as counter_type, meters.unit as counter_unit,"
             " samples.volume as counter_volume,"
             " users.uuid as user_id, projects.uuid as project_id,"
             " resources.resource_id, samples.message_id,"
             " samples.message_signature, samples.recorded_at,"
             " samples.metadata, samples.timestamp"
             " FROM samples"
             " JOIN meters ON samples.meter_id = meters.id"
             " JOIN users ON samples.user_id = users.id"
             " JOIN projects ON samples.project_id = projects.id"
             " JOIN resources ON samples.resource_id = resources.id"
             " JOIN sources ON samples.source_id = sources.id")
    query, values = make_sql_query_from_filter(query, sample_filter, limit)
    query += ";"
    with PoolConnection() as cur:
        cur.execute(query, values)
        resp = cur.fetchall()
    for s in resp:
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
            resource_metadata=s.resource_metadata,
            message_id=s.message_id,
            message_signature=s.message_signature,
        )
        """
    return resp


sample_filter = Object
dt = parser.parse("2015-03-16 12:51:51")
dt1 = parser.parse("2015-03-16 13:51:51")
sample_filter.meter = 'cpu'
sample_filter.source = 'openstack'
sample_filter.start = dt
sample_filter.start_timestamp_op = 'ge'
sample_filter.end = None
sample_filter.end_timestamp_op = 'lt'
sample_filter.user = None
sample_filter.project = None
sample_filter.resource = None
sample_filter.message_id = None
sample_filter.metaquery = {'metadata.status': 'active'}

print get_samples(sample_filter)
