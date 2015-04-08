from ceilometer_local_lib import make_sql_query_from_filter
from ceilometer_local_lib import PoolConnection
from ceilometer_local_lib import Object
import datetime
from six import moves
import math
from oslo.utils import timeutils
from dateutil import parser
# from sqlalchemy import func
# from sqlalchemy import distinct
# import ceilometer
# from ceilometer import storage
# from ceilometer.storage import base
# from ceilometer.storage.sqlalchemy import models
import pprint

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
    'source_id': 'sources.name',
    'project_id': 'projects.uuid',
    'user_id': 'users.uuid',
    'resource_id': 'resources.resource_id'
}


def iter_period(start, end, period):
    """Split a time from start to end in periods of a number of seconds.

    This function yields the (start, end) time for each period composing the
    time passed as argument.

    :param start: When the period set start.
    :param end: When the period end starts.
    :param period: The duration of the period.
    """
    period_start = start
    increment = datetime.timedelta(seconds=period)
    for i in moves.xrange(int(math.ceil(
            timeutils.delta_seconds(start, end)
            / float(period)))):
        next_start = period_start + increment
        yield (period_start, next_start)
        period_start = next_start


def _get_aggregate_functions(aggregate):
    if not aggregate:
        return [f for f in STANDARD_AGGREGATES.values()]

    functions = []
    for a in aggregate:
        if a.func in STANDARD_AGGREGATES:
            functions.append(STANDARD_AGGREGATES[a.func])
        elif a.func in UNPARAMETERIZED_AGGREGATES:
            functions.append(UNPARAMETERIZED_AGGREGATES[a.func])
        elif a.func in PARAMETERIZED_AGGREGATES['compute']:
            validate = PARAMETERIZED_AGGREGATES['validate'].get(a.func)
            if not (validate and validate(a.param)):
                raise storage.StorageBadAggregate('Bad aggregate: %s.%s'
                                                  % (a.func, a.param))
            compute = PARAMETERIZED_AGGREGATES['compute'][a.func]
            functions.append(compute(a.param))
        else:
            raise ceilometer.NotImplementedError(
                'Selectable aggregate function %s'
                ' is not supported' % a.func)

    return functions


def _make_stats_query(sample_filter, groupby, aggregate):
    sql_select = ("SELECT min(samples.timestamp) as tsmin,"
                  " max(samples.timestamp) as tsmax,"
                  " meters.unit as unit")
    aggr = _get_aggregate_functions(aggregate)
    for a in aggr:
        sql_select += ", {}".format(a)
    if groupby:
        # IGNORE THIS FOR TEST PURPOSES
        #group_attributes = [getattr(models.Resource, g) for g in groupby]
        group_attributes = ', '.join(['{} as {}'.format(
            ID_UUID_NAME_CONFORMITY[g], g)
            for g in groupby])
        sql_select += ', {}'.format(group_attributes)
    sql_select += (" FROM samples"
                   " JOIN resources ON samples.resource_id = resources.id"
                   " JOIN meters ON samples.meter_id = meters.id"
                   " JOIN sources ON samples.source_id = sources.id"
                   " JOIN projects ON samples.project_id = projects.id"
                   " JOIN users ON samples.user_id = users.id")
    sql_select, values = make_sql_query_from_filter(sql_select, sample_filter)
    sql_select += " GROUP BY meters.unit"
    if groupby:
        group_attributes = ', '.join([ID_UUID_NAME_CONFORMITY[g]
                                      for g in groupby])
        sql_select += ', {}'.format(group_attributes)
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


def get_meter_statistics(sample_filter, period=None, groupby=None,
                         aggregate=None):
    """Return an iterable of api_models.Statistics instances.

    Items are containing meter statistics described by the query
    parameters. The filter must have a meter value set.
    """
    if groupby:
        for group in groupby:
            if group not in ['user_id', 'project_id', 'resource_id']:
                # IGNORE THIS FOR TEST PURPOSES
                # raise ceilometer.NotImplementedError('Unable to group by '
                                                     #'these fields')
                raise Exception("Unable to group by these fields")
    if not period:
        q, v = _make_stats_query(sample_filter, groupby, aggregate)
        with PoolConnection() as cur:
            cur.execute(q, v)
            result = cur.fetchall()
        if result:
            for res in result:
                yield _stats_result_to_model(res, 0,
                                             res.tsmin, res.tsmax,
                                             groupby,
                                             aggregate)
        cur.close()
        return

    if not sample_filter.start or not sample_filter.end:
        q, v = _make_stats_query(sample_filter, None, aggregate)
        with PoolConnection() as cur:
            cur.execute(q, v)
            result = cur.fetchone()
        res = result
        if not res:
                # NOTE(liusheng):The 'res' may be NoneType, because no
                # sample has found with sample filter(s).
            return
    query, values = _make_stats_query(sample_filter, groupby, aggregate)
        # HACK(jd) This is an awful method to compute stats by period, but
        # since we're trying to be SQL agnostic we have to write portable
        # code, so here it is, admire! We're going to do one request to get
        # stats by period. We would like to use GROUP BY, but there's no
        # portable way to manipulate timestamp in SQL, so we can't.
    for period_start, period_end in iter_period(
            sample_filter.start or res.tsmin,
            sample_filter.end or res.tsmax,
            period):
        values_to_add = []
        values_to_delete = []
        print '1-----', query
        if query.find(" samples.timestamp > %s") > -1:
            query = query.replace(
                " samples.timestamp > %s", " samples.timestamp >= %s")
        if query.find(" samples.timestamp <= %s"):
            query = query.replace(
                " samples.timestamp <= %s", " samples.timestamp < %s")

        if query.find(" samples.timestamp >= %s") == -1:
            seq = [query[:query.index('GROUP BY') - 1],
                   query[query.index('GROUP BY'):]]
            query = ' AND samples.timestamp >= %s '.join(seq)
        if query.find(" samples.timestamp < %s") == -1:
            seq = [query[:query.index('GROUP BY') - 1],
                   query[query.index('GROUP BY'):]]
            query = ' AND samples.timestamp < %s '.join(seq)

        print '-----', query
        if sample_filter.end and not sample_filter.start:
            query = query.replace('AND samples.timestamp >= %s',
                                  'AND samples.timestamp < %s')
            query = query.replace('AND samples.timestamp < %s',
                                  'AND samples.timestamp >= %s', 1)

        if sample_filter.end and sample_filter.end < period_end:
            period_end = sample_filter.end
        for d in values:
            if isinstance(d, datetime.datetime):
                values_to_delete.append(d)
        for i in values_to_delete:
            values.remove(i)
        values_to_add.extend([period_start, period_end])
        print values + values_to_add
        with PoolConnection() as cur:
            cur.execute(query, values + values_to_add)
            results = cur.fetchall()
        if results:
            for result in results:
                yield _stats_result_to_model(
                    result=result,
                    period=int(timeutils.delta_seconds(period_start,
                                                       period_end)),
                    period_start=period_start,
                    period_end=period_end,
                    groupby=groupby,
                    aggregate=aggregate
                )

# MOCK OBJECTS
sample_filter = Object()
sample_filter.meter = 'cpu_util'
sample_filter.source = 'openstack'
dt = parser.parse("2015-03-16 12:51:51")
dt1 = parser.parse("2015-03-16 14:31:52")
sample_filter.start = None
sample_filter.start_timestamp_op = 'ge'
sample_filter.end = None
sample_filter.end_timestamp_op = 'le'
sample_filter.user = '3d622ea5-a70a-42d3-aae5-49ddfc1ef355'
sample_filter.project = None
sample_filter.resource = None
sample_filter.message_id = None
sample_filter.metaquery = {'metadata.status': 'active',
                           'metadata.memory_mb': 512,
                           'metadata.image.name': 'cirros-0.3.3-x86_64'}
group = ['resource_id']
aggr1 = Object()
aggr1.func = 'max'
aggr1.param = None
aggr2 = Object()
aggr2.func = 'avg'
aggr2.param = None
aggregate_list = [aggr1, aggr2]
for stat in get_meter_statistics(sample_filter, groupby=group,
                                 aggregate=None, period=None):
    pprint.pprint(stat)
