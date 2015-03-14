import psycopg2


STANDARD_AGGREGATES = dict(
    avg='avg(samples.volume)',
    sum='sum(samples.volume)',
    min='min(samples.volume)',
    max='max(samples.volume)',
    count='count(samples.volume)'
)
# IGNORE THIS FOR TEST PURPOSES
"""UNPARAMETERIZED_AGGREGATES = dict(
    stddev=func.stddev_pop(models.Sample.volume).label('stddev')
)

PARAMETERIZED_AGGREGATES = dict(
    validate=dict(
        cardinality=lambda p: p in ['resource_id', 'user_id', 'project_id']
    ),
    compute=dict(
        cardinality=lambda p: func.count(
            distinct(getattr(models.Resource, p))
        ).label('cardinality/%s' % p)
    )
)"""
ID_UUID_NAME_CONFORMITY = {
    'source_id': ', sources.name',
    'project_id': ', projects.uuid',
    'user_id': ', users.uuid',
    'resource_id': ', resources.resource_id'
}


class Object(object):
    pass


def _get_aggregate_functions(aggregate):
    if not aggregate:
        return [f for f in STANDARD_AGGREGATES.values()]

    functions = []
    for a in aggregate:
        if a.func in STANDARD_AGGREGATES:
            functions.append(STANDARD_AGGREGATES[a.func])
        # IGNORE THIS FOR TEST PURPOSES
        # elif a.func in UNPARAMETERIZED_AGGREGATES:
        #    functions.append(UNPARAMETERIZED_AGGREGATES[a.func])
        # elif a.func in PARAMETERIZED_AGGREGATES['compute']:
        #    validate = PARAMETERIZED_AGGREGATES['validate'].get(a.func)
        #    if not (validate and validate(a.param)):
        #        raise storage.StorageBadAggregate('Bad aggregate: %s.%s'
        #                                          % (a.func, a.param))
        #    compute = PARAMETERIZED_AGGREGATES['compute'][a.func]
        #    functions.append(compute(a.param))
        else:
            pass
            # raise ceilometer.NotImplementedError(
            #    'Selectable aggregate function %s'
            #    ' is not supported' % a.func)

    return functions


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


def _make_stats_query(sample_filter, groupby, aggregate):
    # TODO make _get_aggregate_functions call
    sql_select = ("SELECT min(samples.timestamp), max(samples.timestamp),"
                  " meters.unit")
    aggr = _get_aggregate_functions(aggregate)
    for a in aggr:
        sql_select += ", {}".format(a)
    if groupby:
        # IGNORE THIS FOR TEST PURPOSES
        #group_attributes = [getattr(models.Resource, g) for g in groupby]
        group_attributes = [g for g in groupby]
        for g in group_attributes:
            sql_select += ID_UUID_NAME_CONFORMITY[g]
    sql_select += (" FROM samples"
                   " JOIN resources ON samples.resource_id = resources.id"
                   " JOIN meters ON samples.meter_id = meters.id"
                   " JOIN sources ON samples.source_id = sources.id"
                   " JOIN projects ON samples.project_id = projects.id"
                   " JOIN users ON samples.user_id = users.id")
    sql_select, values = _make_sql_query_from_filter(sql_select, sample_filter)
    sql_select += " GROUP BY meters.unit"
    if groupby:
        for g in group_attributes:
            sql_select += ID_UUID_NAME_CONFORMITY[g]
    sql_select += ";"
    print sql_select
    return sql_select, values


def _stats_result_aggregates(result, aggregate):
    stats_args = {}
    for attr in ['min', 'max', 'sum', 'avg', 'count']:
        if hasattr(result, attr):
            stats_args[attr] = getattr(result, attr)
    if aggregate:
        stats_args['aggregate'] = {}
        for a in aggregate:
            key = '%s%s' % (a.func, '/%s' % a.param if a.param else '')
            stats_args['aggregate'][key] = getattr(result, key)
    return stats_args


def _stats_result_to_model(result, period, period_start,
                           period_end, groupby, aggregate):
    stats_args = _stats_result_aggregates(result, aggregate)
    stats_args['unit'] = result.unit
    duration = ((result.tsmax - result.tsmin).seconds
                if result.tsmin is not None and result.tsmax is not None
                else None)
    stats_args['duration'] = duration
    stats_args['duration_start'] = result.tsmin
    stats_args['duration_end'] = result.tsmax
    stats_args['period'] = period
    stats_args['period_start'] = period_start
    stats_args['period_end'] = period_end
    stats_args['groupby'] = (dict(
        (g, getattr(result, g)) for g in groupby) if groupby else None)
    # IGNORE THIS FOR TEST PURPOSES
    # return api_models.Statistics(**stats_args)
    return stats_args


def _make_object_from_tuple(res, groupby, aggregate):
    keys = ['tsmin', 'tsmax', 'unit']
    if aggregate:
        keys += [a.func for a in aggregate]
    else:
        keys += [f for f in STANDARD_AGGREGATES.keys()]
    if groupby:
        keys += groupby
    obj = Object()
    for k, i in zip(keys, res):
        setattr(obj, k, i)
    return obj


def get_meter_statistics(sample_filter, period=None, groupby=None,
                         aggregate=None):
    """Return an iterable of api_models.Statistics instances.

    Items are containing meter statistics described by the query
    parameters. The filter must have a meter value set.
    """
    conn = psycopg2.connect("dbname=ceilometer user=alexchadin")
    cur = conn.cursor()
    if groupby:
        for group in groupby:
            if group not in ['user_id', 'project_id', 'resource_id']:
                # IGNORE THIS FOR TEST PURPOSES
                # raise ceilometer.NotImplementedError('Unable to group by '
                                                     #'these fields')
                raise Exception("Unable to group by these fields")
    if not period:
        q, v = _make_stats_query(sample_filter, groupby, aggregate)
        cur.execute(q, v)
        result = cur.fetchall()
        if result:
            for res in result:
                res = _make_object_from_tuple(res, groupby, aggregate)
                print _stats_result_to_model(res, 0,
                                             res.tsmin, res.tsmax,
                                             groupby,
                                             aggregate)
        cur.close()
        return
    if not sample_filter.start or not sample_filter.end:
        res = _make_stats_query(sample_filter, None, aggregate).first()
        if not res:
                # NOTE(liusheng):The 'res' may be NoneType, because no
                # sample has found with sample filter(s).
            return
    query = _make_stats_query(sample_filter, groupby, aggregate)

# MOCK OBJECTS
sample_filter = Object()
sample_filter.meter = 'disk.ephemeral.size'
sample_filter.source = 'openstack'
sample_filter.start = '2014-08-20 10:02:35'
sample_filter.start_timestamp_op = 'ge'
sample_filter.end = '2014-08-20 10:02:50.503009'
sample_filter.end_timestamp_op = 'le'
sample_filter.user = None
sample_filter.project = None
sample_filter.resource = None
sample_filter.message_id = None
sample_filter.metaquery = None
group = ['resource_id', 'project_id']
aggr1 = Object()
aggr1.func = 'max'
aggr1.param = None
aggr2 = Object()
aggr2.func = 'avg'
aggr2.param = None
aggregate_list = [aggr1, aggr2]

get_meter_statistics(sample_filter, groupby=group, aggregate=aggregate_list)
