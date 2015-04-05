# -*- encoding: utf-8 -*-
#
# Copyright © 2012 New Dream Network, LLC (DreamHost)
#
# Author: Doug Hellmann <doug.hellmann@dreamhost.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
"""Base classes for storage engines
"""

from eventlet.db_pool import ConnectionPool
from eventlet.support.psycopg2_patcher import make_psycopg_green
make_psycopg_green()

import psycopg2
import json
import datetime
import six

from psycopg2.extras import NamedTupleCursor
from psycopg2.extras import Json

import ceilometer
from ceilometer.openstack.common.gettextutils import _
from ceilometer.openstack.common import jsonutils
from ceilometer.openstack.common import log
from ceilometer import storage
from ceilometer.storage import base
from ceilometer.storage import models as api_models
from ceilometer import utils
from oslo.utils import timeutils
LOG = log.getLogger(__name__)


AVAILABLE_CAPABILITIES = {
    'meters': {'query': {'simple': True,
                         'metadata': True}},
    'resources': {'query': {'simple': True,
                            'metadata': True}},
    'samples': {'pagination': True,
                'groupby': True,
                'query': {'simple': True,
                          'metadata': True,
                          'complex': True}},
    'statistics': {'groupby': True,
                   'query': {'simple': True,
                             'metadata': True},
                   'aggregation': {'standard': True,
                                   'selectable': {
                                       'max': True,
                                       'min': True,
                                       'sum': True,
                                       'avg': True,
                                       'count': True,
                                       'stddev': True,
                                       'cardinality': True}}
                   },
    'events': {'query': {'simple': True}},
}

AVAILABLE_STORAGE_CAPABILITIES = {
    'storage': {'production_ready': True},
}


class PoolConnection(object):

    """Wraps connection pool to ease of use with transactions"""

    def __init__(self, pool, readonly=False):
        self._pool = pool
        self._readonly = readonly

    def __enter__(self):
        self._conn = self._pool.get()
        self._curr = self._conn.cursor(
            cursor_factory=NamedTupleCursor
        )
        if self._readonly:
            self._conn.autocommit = True
        return self._curr

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._curr.close()
        if self._readonly:
            self._conn.autocommit = False
        elif exc_type is None:
            self._conn.commit()
        else:
            self._conn.rollback()

        self._pool.put(self._conn)


def make_list(resp):
    result = []
    for r in resp:
        result.append(r[0])
    return result


def make_metaquery(metastr, value):
    elements = metastr.split('.')[1:]
    if not elements:
        return None
    jsq = dict()
    jsq_last = jsq
    for e in elements[:-1]:
        jsq_last[e] = dict()
        jsq_last = jsq_last[e]
    jsq_last[elements[-1]] = value
    return jsq


def apply_metaquery_filter(metaquery):
    meta_filter = dict()
    for key, value in six.iteritems(metaquery):
        meta_filter.update(make_metaquery(key, value))
    return ('metadata @> %s', Json(meta_filter))


def make_sql_query_from_filter(query, sample_filter,
                               limit=None, require_meter=True):
    sql_where_body = ''
    subq_and = ' AND {}'
    values = []
    if sample_filter.meter:
        sql_where_body += subq_and.format('meters.name = %s')
        values.append(sample_filter.meter)
    elif require_meter:
        raise RuntimeError('Missing required meter specifier')
    if sample_filter.source:
        sql_where_body += subq_and.format('sources.name = %s')
        values.append(sample_filter.source)
    if sample_filter.start:
        ts_start = sample_filter.start
        if sample_filter.start_timestamp_op == 'gt':
            sql_where_body += subq_and.format('samples.timestamp > %s')
        else:
            sql_where_body += subq_and.format('samples.timestamp >= %s')
        values.append(ts_start)
    if sample_filter.end:
        ts_end = sample_filter.end
        if sample_filter.end_timestamp_op == 'le':
            sql_where_body += subq_and.format('samples.timestamp <= %s')
        else:
            sql_where_body += subq_and.format('samples.timestamp < %s')
        values.append(ts_end)
    if sample_filter.user:
        sql_where_body += subq_and.format('users.uuid = %s')
        values.append(sample_filter.user)
    if sample_filter.project:
        sql_where_body += subq_and.format('projects.uuid = %s')
        values.append(sample_filter.project)
    if sample_filter.resource:
        sql_where_body += subq_and.format('resources.resource_id = %s')
        values.append(sample_filter.resource)
    if sample_filter.message_id:
        sql_where_body += subq_and.format('samples.message_id = %s')
        values.append(sample_filter.message_id)
    if sample_filter.metaquery:
        q, v = apply_metaquery_filter(sample_filter.metaquery)
        sql_where_body += subq_and.format(q)
        values.append(v)
    if limit:
        sql_limit_body = " LIMIT %s"
        values.append(limit)
    sql_where_body = sql_where_body.replace(' AND', ' WHERE', 1)
    query = query + sql_where_body + sql_limit_body
    return query, values
complex_operators = ['and', 'or', 'not']


def _handle_complex_op(complex_op, nodes, values):
    items = []
    for node in nodes:
        node_str = _transform_filter(node, values)
        items.append(node_str)
    if complex_op is 'or':
        return '(' + ' {} '.format(complex_op).join(items) + ')'
    if complex_op is 'and':
        return ' {} '.format(complex_op).join(items)


def _handle_simple_op(simple_op, nodes, values):
    if nodes.keys()[0].startswith('resource_metadata'):
        q, v = apply_metaquery_filter(nodes)
        values.append(v)
        return q
    values.append(nodes.values()[0])
    return "%s %s %%s" % (nodes.keys()[0], simple_op)


def _transform_filter(tree, values):
    operator = tree.keys()[0]
    nodes = tree.values()[0]
    if operator in complex_operators:
        return _handle_complex_op(operator, nodes, values)
    else:
        return _handle_simple_op(operator, nodes, values)


def transform_filter(tree):
    values = []
    res = _transform_filter(tree, values)
    return ' WHERE ' + res, values


def transform_orderby(orderby):
    return ' ORDER BY ' + ', '.join(['%s %s' % (x.keys()[0], x.values()[0])
                                     for x in orderby])
STANDARD_AGGREGATES = dict(
    avg='avg(samples.volume)',
    sum='sum(samples.volume)',
    min='min(samples.volume)',
    max='max(samples.volume)',
    count='count(samples.volume)'
)
ID_UUID_NAME_CONFORMITY = {
    'source_id': 'sources.name',
    'project_id': 'projects.uuid',
    'user_id': 'users.uuid',
    'resource_id': 'resources.resource_id'
}


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
    return api_models.Statistics(**stats_args)


class Connection(base.Connection):

    """PostgreSQL connections."""
    CAPABILITIES = utils.update_nested(base.Connection.CAPABILITIES,
                                       AVAILABLE_CAPABILITIES)
    STORAGE_CAPABILITIES = utils.update_nested(
        base.Connection.STORAGE_CAPABILITIES,
        AVAILABLE_STORAGE_CAPABILITIES,
    )

    def __init__(self, conf):
        """Constructor."""
        self.conn_pool = self._get_connection_pool(None)

    @staticmethod
    def _get_connection_pool(db_conf):
        """Returns connection pool to the database"""
        return ConnectionPool(psycopg2,
                              min_size=0,
                              max_size=4,
                              max_idle=10,
                              connect_timeout=5,
                              host='controller1',
                              port=5432,
                              user='ceilometer',
                              password='ceilometer',
                              database='ceilometer')

    def upgrade(self):
        """Migrate the database to `version` or the most recent version."""
        with PoolConnection(self.conn_pool) as db:
            db.execute("""
                      
                      """)

    def record_metering_data(self, data):
        """Write the data to the backend storage system.

        :param data: a dictionary such as returned by
                    ceilometer.meter.meter_message_from_counter

        All timestamps must be naive utc datetime object.
        """
        dthandler = lambda obj: obj.isoformat() if isinstance(
            obj, datetime.datetime) else None
        if not data['user_id']:
            data['user_id'] = '2c9ffd7a-ee60-406c-bd3a-85ef8911a33a'
        d = json.dumps(data, ensure_ascii=False, default=dthandler)
        LOG.debug(_("---------"))
        LOG.debug(_(d))
        with PoolConnection(self.conn_pool) as db:
            db.execute('SELECT \"write_sample\"(%s);', (d,))

    def clear_expired_metering_data(self, ttl):
        """Clear expired data from the backend storage system according to the
        time-to-live.

        :param ttl: Number of seconds to keep records for.

        """
        date = datetime.datetime.now() - datetime.timedelta(seconds=ttl)
        query = "DELETE FROM samples WHERE samples.timestamp < %s;"
        with PoolConnection(self.conn_pool) as db:
            db.execute(query, [date])

    def get_users(self, source=None):
        """Return an iterable of user id strings.

       :param source: Optional source filter.
       """
        with PoolConnection(self.conn_pool) as cur:
            if source:
                cur.execute("SELECT uuid FROM users WHERE source_id=%s;",
                            (source,))
            else:
                cur.execute("SELECT uuid FROM users;")
            resp = make_list(cur.fetchall())
        return resp

    def get_projects(self, source=None):
        """Return an iterable of project id strings.

        :param source: Optional source filter.
        """
        with PoolConnection(self.conn_pool) as cur:
            if source:
                cur.execute("SELECT uuid FROM projects WHERE source_id=%s;",
                            (source,))
            else:
                cur.execute("SELECT uuid FROM projects;")
            resp = make_list(cur.fetchall())
        return resp

    def get_resources(self, user=None, project=None, source=None,
                      start_timestamp=None, start_timestamp_op=None,
                      end_timestamp=None, end_timestamp_op=None,
                      metaquery={}, resource=None, pagination=None):
        """Return an iterable of models.Resource instances containing
       resource information.

       :param user: Optional ID for user that owns the resource.
       :param project: Optional ID for project that owns the resource.
       :param source: Optional source filter.
       :param start_timestamp: Optional modified timestamp start range.
       :param start_timestamp_op: Optional timestamp start range operation.
       :param end_timestamp: Optional modified timestamp end range.
       :param end_timestamp_op: Optional timestamp end range operation.
       :param metaquery: Optional dict with metadata to match on.
       :param resource: Optional resource filter.
       :param pagination: Optional pagination query.
       """
        if pagination:
            raise ceilometer.NotImplementedError('Pagination not implemented')

        s_filter = storage.SampleFilter(user=user,
                                        project=project,
                                        source=source,
                                        start=start_timestamp,
                                        start_timestamp_op=start_timestamp_op,
                                        end=end_timestamp,
                                        end_timestamp_op=end_timestamp_op,
                                        metaquery=metaquery,
                                        resource=resource)

        resource_id = None
        user_id = None
        project_id = None
        source_id = None

        subq_values = []
        samples_subq = ("SELECT resource_id, source_id, user_id, project_id,"
                        " max(timestamp) as max_ts, min(timestamp) as min_ts,"
                        " metadata"
                        " FROM samples")

        if s_filter.resource:
            resource_id_q = ("SELECT id FROM resources"
                             " WHERE resource_id = %s")
            with PoolConnection(self.conn_pool) as cur:
                cur.execute(resource_id_q, (s_filter.resource,))
                resource_resp = cur.fetchone()
            if resource_resp:
                resource_id = resource_resp[0]
                samples_subq += " AND resource_id = %s"
                subq_values.append(resource_id)
            else:
                LOG.debug(
                    _("Resource from sample filter does not exist in DB"))
                return tuple()

        if s_filter.user:
            user_id_q = ("SELECT id FROM users"
                         " WHERE uuid = %s")
            with PoolConnection(self.conn_pool) as cur:
                cur.execute(user_id_q, (s_filter.user,))
                user_resp = cur.fetchone()
            if user_resp:
                user_id = user_resp[0]
                samples_subq += " AND user_id = %s"
                subq_values.append(user_id)
            else:
                LOG.debug(_("User from sample filter does not exist in DB"))
                return tuple()

        if s_filter.project:
            project_id_q = ("SELECT id FROM projects"
                            " WHERE uuid = %s")
            with PoolConnection(self.conn_pool) as cur:
                cur.execute(project_id_q, (s_filter.project,))
                project_resp = cur.fetchone()
            if project_resp:
                project_id = project_resp[0]
                samples_subq += " AND project_id = %s"
                subq_values.append(project_id)
            else:
                LOG.debug(_("Project from sample filter does not exist in DB"))
                return tuple()

        if s_filter.source:
            source_name_q = ("SELECT id FROM sources"
                             " WHERE name = %s")
            with PoolConnection(self.conn_pool) as cur:
                cur.execute(source_name_q, (s_filter.source,))
                source_resp = cur.fetchone()
            if source_resp:
                source_id = source_resp[0]
                samples_subq += " AND source_id = %s"
                subq_values.append(source_id)
            else:
                LOG.debug(_("Source from sample filter does not exist in DB"))
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

        samples_subq += (" GROUP BY resource_id, source_id,"
                         " user_id, project_id, metadata")

        samples_subq = samples_subq.replace(" AND", " WHERE", 1)

        query = ("SELECT resources.resource_id as id, sources.name as source_name,"
                 " users.uuid as user_id, projects.uuid as project_id,"
                 " max_ts, min_ts, metadata"
                 " FROM ({}) as samples"
                 " JOIN resources ON samples.resource_id = resources.id"
                 " JOIN users ON samples.user_id = users.id"
                 " JOIN projects ON samples.project_id = projects.id"
                 " JOIN sources ON samples.source_id = sources.id")
        query = query.format(samples_subq)

        with PoolConnection(self.conn_pool) as cur:
            cur.execute(query, subq_values)
            resp = cur.fetchall()

        return (api_models.Resource(
            resource_id=res[0],
            user_id=res[2],
            project_id=res[3],
            first_sample_timestamp=res[4],
            last_sample_timestamp=res[5],
            source=res[1],
            metadata=res[6]) for res in resp)

    def get_meters(self, user=None, project=None, resource=None, source=None,
                   metaquery={}, pagination=None):
        """Return an iterable of model.Meter instances containing meter
         information.

         :param user: Optional ID for user that owns the resource.
         :param project: Optional ID for project that owns the resource.
         :param resource: Optional resource filter.
         :param source: Optional source filter.
         :param metaquery: Optional dict with metadata to match on.
         :param pagination: Optional pagination query.
         """
        s_filter = storage.SampleFilter(user=user,
                                        project=project,
                                        source=source,
                                        metaquery=metaquery,
                                        resource=resource)
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
        query, values = make_sql_query_from_filter(query, s_filter,
                                                   require_meter=False)
        if resource:
            values = [resource] + values
        query = query.format(subq)
        query += " ORDER BY meter_id;"
        with PoolConnection(self.conn_pool) as cur:
            cur.execute(query, values)
            res = cur.fetchall()
        for row in res:
            yield api_models.Meter(
                name=row.name,
                type=row.type,
                unit=row.unit,
                resource_id=row.resource_id,
                project_id=row.project_id,
                source=row.source_id,
                user_id=row.user_id)

    def get_samples(self, sample_filter, limit=None):
        """Return an iterable of model.Sample instances.

        :param sample_filter: Filter.
        :param limit: Maximum number of results to return.
        """
        query = ("SELECT sources.name as source_id,"
                 " meters.name as counter_name,"
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
        with PoolConnection(self.conn_pool) as cur:
            cur.execute(query, values)
            resp = cur.fetchall()
        for s in resp:
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

    def get_meter_statistics(self, sample_filter, period=None, groupby=None,
                             aggregate=None):
        """Return an iterable of model.Statistics instances.

       The filter must have a meter value set.
       """
        if groupby:
            for group in groupby:
                if group not in ['user_id', 'project_id', 'resource_id']:
                    raise ceilometer.NotImplementedError('Unable to group by '
                                                         'these fields')
        if not period:
            q, v = _make_stats_query(sample_filter, groupby, aggregate)
            with PoolConnection(self.conn_pool) as db:
                db.execute(q, v)
                result = db.fetchall()
            if result:
                for res in result:
                    yield _stats_result_to_model(res, 0,
                                                 res.tsmin, res.tsmax,
                                                 groupby,
                                                 aggregate)
            return

        if not sample_filter.start or not sample_filter.end:
            q, v = _make_stats_query(sample_filter, None, aggregate)
            with PoolConnection(self.conn_pool) as db:
                db.execute(q, v)
                res = db.fetchone()
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
        for period_start, period_end in base.iter_period(
                sample_filter.start or res.tsmin,
                sample_filter.end or res.tsmax,
                period):
            values_to_add = []
            values_to_delete = []

            if query.find(" samples.timestamp >=") == -1:
                seq = [query[:query.index('GROUP BY') - 1],
                       query[query.index('GROUP BY'):]]
                query = ' AND samples.timestamp >= %s '.join(seq)
            if query.find(" samples.timestamp <") == -1:
                seq = [query[:query.index('GROUP BY') - 1],
                       query[query.index('GROUP BY'):]]
                query = ' AND samples.timestamp < %s '.join(seq)

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

            with PoolConnection(self.conn_pool) as db:
                db.execute(query, values + values_to_add)
                results = db.fetchall()
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

    @staticmethod
    def get_alarms(name=None, user=None,
                   project=None, enabled=None, alarm_id=None, pagination=None):
        """Yields a lists of alarms that match filters."""
        raise NotImplementedError('Alarms not implemented')

    @staticmethod
    def create_alarm(alarm):
        """Create an alarm. Returns the alarm as created.

       :param alarm: The alarm to create.
       """
        raise NotImplementedError('Alarms not implemented')

    @staticmethod
    def update_alarm(alarm):
        """Update alarm."""
        raise NotImplementedError('Alarms not implemented')

    @staticmethod
    def delete_alarm(alarm_id):
        """Delete an alarm."""
        raise NotImplementedError('Alarms not implemented')

    @staticmethod
    def get_alarm_changes(alarm_id, on_behalf_of,
                          user=None, project=None, type=None,
                          start_timestamp=None, start_timestamp_op=None,
                          end_timestamp=None, end_timestamp_op=None):
        """Yields list of AlarmChanges describing alarm history

       Changes are always sorted in reverse order of occurrence, given
       the importance of currency.

       Segregation for non-administrative users is done on the basis
       of the on_behalf_of parameter. This allows such users to have
       visibility on both the changes initiated by themselves directly
       (generally creation, rule changes, or deletion) and also on those
       changes initiated on their behalf by the alarming service (state
       transitions after alarm thresholds are crossed).

       :param alarm_id: ID of alarm to return changes for
       :param on_behalf_of: ID of tenant to scope changes query (None for
                            administrative user, indicating all projects)
       :param user: Optional ID of user to return changes for
       :param project: Optional ID of project to return changes for
       :project type: Optional change type
       :param start_timestamp: Optional modified timestamp start range
       :param start_timestamp_op: Optional timestamp start range operation
       :param end_timestamp: Optional modified timestamp end range
       :param end_timestamp_op: Optional timestamp end range operation
       """
        raise NotImplementedError('Alarm history not implemented')

    @staticmethod
    def record_alarm_change(alarm_change):
        """Record alarm change event."""
        raise NotImplementedError('Alarm history not implemented')

    @staticmethod
    def clear():
        """Clear database."""

    @staticmethod
    def record_events(events):
        """Write the events to the backend storage system.

       :param events: a list of model.Event objects.
       """
        raise NotImplementedError('Events not implemented.')

    @staticmethod
    def get_events(event_filter):
        """Return an iterable of model.Event objects.
       """
        raise NotImplementedError('Events not implemented.')

    @staticmethod
    def get_event_types():
        """Return all event types as an iterable of strings.
       """
        raise NotImplementedError('Events not implemented.')

    @staticmethod
    def get_trait_types(event_type):
        """Return a dictionary containing the name and data type of
       the trait type. Only trait types for the provided event_type are
       returned.

       :param event_type: the type of the Event
       """
        raise NotImplementedError('Events not implemented.')

    @staticmethod
    def get_traits(event_type, trait_type=None):
        """Return all trait instances associated with an event_type. If
       trait_type is specified, only return instances of that trait type.

       :param event_type: the type of the Event to filter by
       :param trait_type: the name of the Trait to filter by
       """

        raise NotImplementedError('Events not implemented.')

    @staticmethod
    def query_samples(filter_expr=None, orderby=None, limit=None):
        """Return an iterable of model.Sample objects.

       :param filter_expr: Filter expression for query.
       :param orderby: List of field name and direction pairs for order by.
       :param limit: Maximum number of results to return.
       """

        raise NotImplementedError('Complex query for samples '
                                  'is not implemented.')

    @staticmethod
    def query_alarms(filter_expr=None, orderby=None, limit=None):
        """Return an iterable of model.Alarm objects.

       :param filter_expr: Filter expression for query.
       :param orderby: List of field name and direction pairs for order by.
       :param limit: Maximum number of results to return.
       """

        raise NotImplementedError('Complex query for alarms '
                                  'is not implemented.')

    @staticmethod
    def query_alarm_history(filter_expr=None, orderby=None, limit=None):
        """Return an iterable of model.AlarmChange objects.

       :param filter_expr: Filter expression for query.
       :param orderby: List of field name and direction pairs for order by.
       :param limit: Maximum number of results to return.
       """

        raise NotImplementedError('Complex query for alarms '
                                  'history is not implemented.')

    @classmethod
    def get_capabilities(cls):
        """Return an dictionary with the capabilities of each driver."""
        return cls.CAPABILITIES

    @classmethod
    def get_storage_capabilities(cls):
        """Return a dictionary representing the performance capabilities.

        This is needed to evaluate the performance of each driver.
        """
        return cls.STORAGE_CAPABILITIES
