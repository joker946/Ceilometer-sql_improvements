import ceilometer_local_lib
from psycopg2.extras import DictCursor


def get_meters(user=None, project=None, resource=None, source=None,
               metaquery=None, pagination=None):
    """Return an iterable of api_models.Meter instances

    :param user: Optional ID for user that owns the resource.
    :param project: Optional ID for project that owns the resource.
    :param resource: Optional ID of the resource.
    :param source: Optional source filter.
    :param metaquery: Optional dict with metadata to match on.
    :param pagination: Optional pagination query.
    """
    if pagination:
        raise Exception('Pagination not implemented')
        # raise ceilometer.NotImplementedError('Pagination not implemented')
    """
    s_filter = storage.SampleFilter(user=user,
                                    project=project,
                                    source=source,
                                    metaquery=metaquery,
                                    resource=resource)
    """
    s_filter = ceilometer_local_lib.Object()
    s_filter.user = user
    s_filter.project = project
    s_filter.source = source
    s_filter.metaquery = metaquery
    s_filter.resource = resource
    s_filter.meter = None
    s_filter.start = None
    s_filter.start_timestamp_op = None
    s_filter.end = None
    s_filter.end_timestamp_op = None
    s_filter.message_id = None
    s_filter.metaquery = None
    subq = ("SELECT max(samples.id) as id"
            " FROM samples"
            " JOIN resources ON samples.resource_id = resources.id")
    if resource:
        subq += " WHERE resources.resource_id = %s"
    subq += " GROUP BY samples.meter_id, resources.resource_id"

    query = ("SELECT samples.meter_id, meters.name, meters.type, meters.unit,"
             " resources.resource_id, projects.uuid as project_id,"
             " sources.name as source_id, users.uuid as user_id"
             " FROM meters"
             " JOIN samples ON meters.id = samples.meter_id"
             " JOIN ({0}) as a ON samples.id = a.id"
             " JOIN resources ON samples.resource_id = resources.id"
             " LEFT JOIN users ON samples.user_id = users.id"
             " JOIN sources ON samples.source_id = sources.id"
             " JOIN projects ON samples.project_id = projects.id")
    query, values = ceilometer_local_lib.make_sql_query_from_filter(query, s_filter,
                                                                    require_meter=False)
    if resource:
        values = [resource] + values
    query = query.format(subq)
    query += " ORDER BY meter_id;"
    print query
    with ceilometer_local_lib.PoolConnection(cursor_factory=DictCursor) as cur:
        cur.execute(query, values)
        res = cur.fetchall()
    res[0]['user_id'] = 1
    print res
    return res


get_meters()
