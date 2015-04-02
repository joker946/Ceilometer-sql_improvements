from ceilometer_local_lib import make_sql_query_from_filter
from ceilometer_local_lib import apply_metaquery_filter
from ceilometer_local_lib import PoolConnection
from ceilometer_local_lib import Object
from dateutil import parser


def get_resources(user=None, project=None, source=None,
                  start_timestamp=None, start_timestamp_op=None,
                  end_timestamp=None, end_timestamp_op=None,
                  metaquery={}, resource=None, pagination=None):
    if pagination:
        # IGNORE THIS FOR TEST PURPOSES
        # raise ceilometer.NotImplementedError('Pagination not implemented')
        raise Exception('Pagination not implemented')

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
    s_filter.resource = resource
    s_filter.meter = None
    s_filter.start = start_timestamp
    s_filter.start_timestamp_op = start_timestamp_op
    s_filter.end = end_timestamp
    s_filter.end_timestamp_op = end_timestamp_op
    s_filter.message_id = None
    s_filter.metaquery = metaquery

    resource_id = None
    user_id = None
    project_id = None
    source_id = None

    subq_values = []
    samples_subq = ("SELECT resource_id, source_id, user_id, project_id,"
                    " max(timestamp) as max_ts, min(timestamp) as min_ts"
                    " FROM samples")

    if s_filter.resource:
        resource_id_q = ("SELECT id FROM resources"
                         " WHERE resource_id = %s")
        with PoolConnection() as cur:
            cur.execute(resource_id_q, (s_filter.resource,))
            resource_resp = cur.fetchone()
        if resource_resp:
            resource_id = resource_resp[0]
            samples_subq += " AND resource_id = %s"
            subq_values.append(resource_id)
        else:
            return tuple()

    if s_filter.user:
        user_id_q = ("SELECT id FROM users"
                     " WHERE uuid = %s")
        with PoolConnection() as cur:
            cur.execute(user_id_q, (s_filter.user,))
            user_resp = cur.fetchone()
        if user_resp:
            user_id = user_resp[0]
            samples_subq += " AND user_id = %s"
            subq_values.append(user_id)
        else:
            return tuple()

    if s_filter.project:
        project_id_q = ("SELECT id FROM projects"
                        " WHERE uuid = %s")
        with PoolConnection() as cur:
            cur.execute(project_id_q, (s_filter.project,))
            project_resp = cur.fetchone()
        if project_resp:
            project_id = project_resp[0]
            samples_subq += " AND project_id = %s"
            subq_values.append(project_id)
        else:
            return tuple()

    if s_filter.source:
        source_name_q = ("SELECT id FROM sources"
                         " WHERE name = %s")
        with PoolConnection() as cur:
            cur.execute(source_name_q, (s_filter.source,))
            source_resp = cur.fetchone()
        if source_resp:
            source_id = source_resp[0]
            samples_subq += " AND source_id = %s"
            subq_values.append(source_id)
        else:
            return tuple()

    if s_filter.start:
        ts_start = s_filter.start
        if s_filter.start_timestamp_op == "gt":
            samples_subq += " AND timestamp > %s"
        else:
            samples_subq += " AND timestamp >= %s"
        subq_values.append(ts_start)

    if s_filter.end:
        ts_end = s_filter.end
        if s_filter.end_timestamp_op == 'le':
            samples_subq += " AND timestamp <= %s"
        else:
            samples_subq += " AND timestamp < %s"
        subq_values.append(ts_end)

    if s_filter.metaquery:
        q, v = apply_metaquery_filter(s_filter.metaquery)
        samples_subq += " AND {}".format(q)
        subq_values.append(v)

    samples_subq += " GROUP BY resource_id, source_id, user_id, project_id"

    samples_subq = samples_subq.replace(" AND", " WHERE", 1)

    query = ("SELECT resources.resource_id as id, sources.name as source_name,"
             " users.uuid as user_id, projects.uuid as project_id,"
             " max_ts, min_ts"
             " FROM ({}) as samples"
             " JOIN resources ON samples.resource_id = resources.id"
             " JOIN users ON samples.user_id = users.id"
             " JOIN projects ON samples.project_id = projects.id"
             " JOIN sources ON samples.source_id = sources.id")
    query = query.format(samples_subq)

    with PoolConnection() as cur:
        cur.execute(query, subq_values)
        resp = cur.fetchall()

    print "----------------------"
    print("QUERY\n")
    print query
    print "----------------------"
    print("VALUES\n")
    print subq_values
    print "----------------------"
    print("RESPONCE\n")
    print resp
    print "----------------------"
    i = 0
    for res in resp:
        i += 1
        print "\nRecord #{}:".format(i)
        print res

metaquery = {'metadata.status': 'active',
             'metadata.memory_mb': 512,
             'metadata.image.name': 'cirros-0.3.3-x86_64'
             }

user_id_1 = "0c76126d-a5e3-4503-be44-4d9bed645cf7"
user_id_2 = "3d622ea5-a70a-42d3-aae5-49ddfc1ef355"
dt_start = parser.parse("2015-03-16 12:51:50")
dt_end = parser.parse("2015-03-16 14:41:52")
get_resources(user=user_id_2,
              metaquery=metaquery)
print "----------------------"
