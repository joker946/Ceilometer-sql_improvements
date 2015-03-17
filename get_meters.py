import psycopg2


class Object(object):
    pass


class PoolConnection(object):

    def __init__(self, readonly=False):
        self._conn = psycopg2.connect("dbname=ceilometer user=alexchadin")
        self._readonly = readonly

    def __enter__(self):
        self._cur = self._conn.cursor()
        if self._readonly:
            self._conn.autocommit = True
        return self._cur

    def __exit__(self, ex_type, ex_value, ex_traceback):
        self._cur.close()
        if self._readonly:
            self._conn.autocommit = False
        elif ex_type is None:
            self._conn.commit()
        else:
            self._conn.rollback()


def concat_query(query, line, keyword):
    if keyword == 'WHERE' and keyword in query:
        query += " AND {}".format(line)
    elif keyword == 'WHERE' and keyword not in query:
        query += " WHERE {}".format(line)
    return query


def _make_sql_query_from_filter(query, sample_filter,
                                limit=None, require_meter=True):
    values = []
    if sample_filter.meter:
        query = concat_query(query, "meters.name = %s", 'WHERE')
        values.append(sample_filter.meter)
    elif require_meter:
        raise RuntimeError('Missing required meter specifier')
    if sample_filter.source:
        query = concat_query(query, "sources.name = %s", 'WHERE')
        values.append(sample_filter.source)
    if sample_filter.start:
        ts_start = sample_filter.start
        if sample_filter.start_timestamp_op == 'gt':
            query = concat_query(query, "samples.timestamp > %s", 'WHERE')
        else:
            query = concat_query(query, "samples.timestamp >= %s", 'WHERE')
        values.append(ts_start)
    if sample_filter.end:
        ts_end = sample_filter.end
        if sample_filter.end_timestamp_op == 'le':
            query = concat_query(query, "samples.timestamp <= %s", 'WHERE')
        else:
            query = concat_query(query, "samples.timestamp < %s", 'WHERE')
        values.append(ts_end)
    if sample_filter.user:
        query = concat_query(query, "user_id = %s", 'WHERE')
        values.append(sample_filter.user)
    if sample_filter.project:
        query = concat_query(query, "projects_id = %s", 'WHERE')
        values.append(sample_filter.project)
    if sample_filter.resource:
        query = concat_query(query, "resources.resource_id = %s", 'WHERE')
        values.append(sample_filter.resource)
    if sample_filter.message_id:
        query = concat_query(query, "samples.message_id = %s", 'WHERE')
        values.append(sample_filter.message_id)
    if sample_filter.metaquery:
        # Note (alexchadin): This section needs to be implemented.
        pass
    if limit:
        query += " LIMIT %s"
        values.append(limit)
    return query, values


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
    s_filter = Object()
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
             " JOIN users ON samples.user_id = users.id"
             " JOIN sources ON samples.source_id = sources.id"
             " JOIN projects ON samples.project_id = projects.id")
    query, values = _make_sql_query_from_filter(query, s_filter,
                                                require_meter=False)
    if resource:
        values = [resource] + values
    query = query.format(subq)
    query += " ORDER BY meter_id;"
    print query
    with PoolConnection() as cur:
        cur.execute(query, values)
        res = cur.fetchall()
    print res


get_meters()
