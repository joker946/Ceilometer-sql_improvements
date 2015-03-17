import psycopg2


def concat_query(query, line, keyword):
    if keyword == 'WHERE' and keyword in query:
        query += " AND {}".format(line)
    elif keyword == 'WHERE' and keyword not in query:
        query += " WHERE {}".format(line)
    return query


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


def make_sql_query_from_filter(query, sample_filter,
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


def make_list(resp):
    result = []
    for r in resp:
        result.append(r[0])
    return result


def get_samples(sample_filter, limit=None):
    """Return an iterable of model.Sample instances.

   :param sample_filter: Filter.
   :param limit: Maximum number of results to return.
   """
    conn = psycopg2.connect("dbname=ceilometer user=alexchadin")
    cur = conn.cursor()
    if limit == 0:
        return []
    query = ("SELECT sources.name as source_name, meters.name as meter_name,"
             " meters.type, meters.unit, samples.volume,"
             " users.uuid as user_id, projects.uuid as projects_id,"
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
    # print query
    # print values
    with PoolConnection() as cur:
        cur.execute(query, values)
        resp = cur.fetchall()
    print resp


class Object(object):
    pass
sample_filter = Object
sample_filter.meter = 'cpu'
sample_filter.source = 'openstack'
sample_filter.start = None
sample_filter.start_timestamp_op = None
sample_filter.end = None
sample_filter.end_timestamp_op = None
sample_filter.user = None
sample_filter.project = None
sample_filter.resource = None
sample_filter.message_id = None
sample_filter.metaquery = None

get_samples(sample_filter)
