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

from ceilometer.openstack.common.gettextutils import _
from ceilometer.openstack.common import jsonutils
from ceilometer.openstack.common import log
from ceilometer import storage
from ceilometer.storage import base
from ceilometer.storage import models as api_models
from ceilometer import utils

LOG = log.getLogger(__name__)


AVAILABLE_CAPABILITIES = {
    'meters': {'pagination': True,
               'query': {'simple': True,
                         'metadata': True,
                         'complex': True}},
    'resources': {'pagination': True,
                  'query': {'simple': True,
                            'metadata': True,
                            'complex': True}},
    'samples': {'pagination': True,
                'groupby': True,
                'query': {'simple': True,
                          'metadata': True,
                          'complex': True}},
    'statistics': {'pagination': False,
                   'groupby': False,
                   'query': {'simple': False,
                             'metadata': False,
                             'complex': False},
                   'aggregation': {'standard': False,
                                   'selectable': {
                                       'max': False,
                                       'min': False,
                                       'sum': False,
                                       'avg': False,
                                       'count': False,
                                       'stddev': False,
                                       'cardinality': False}}
                   },
    'alarms': {'query': {'simple': False,
                         'complex': False},
               'history': {'query': {'simple': False,
                                     'complex': False}}},
    'events': {'query': {'simple': False}},
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
        query += " LIMIT %s"
        values.append(limit)
    sql_where_body = sql_where_body.replace(' AND', ' WHERE', 1)
    query += sql_where_body
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
                      drop function if exists insert_ignore(int);
                      create function insert_ignore(int)
                      returns void
                      as $$
                      begin
                      perform 1 from tbl where a = $1;
                      if not found then
                          begin
                              insert into tbl (a, b) values ($1, 'abc');
                          exception when unique_violation then
                          end;
                      end if;
                      end
                      $$ language plpgsql;)
                      """)

    def record_metering_data(self, data):
        """Write the data to the backend storage system.

        :param data: a dictionary such as returned by
                    ceilometer.meter.meter_message_from_counter

        All timestamps must be naive utc datetime object.
        """
        d = json.dumps(data)
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
        print date
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

    @staticmethod
    def get_resources(user=None, project=None, source=None,
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
        raise NotImplementedError('Resources not implemented')

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
                 " JOIN users ON samples.user_id = users.id"
                 " JOIN sources ON samples.source_id = sources.id"
                 " JOIN projects ON samples.project_id = projects.id")
        query, values = make_sql_query_from_filter(query, s_filter,
                                                   require_meter=False)
        if resource:
            values = [resource] + values
        query = query.format(subq)
        query += " ORDER BY meter_id;"
        print query
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
                resource_metadata=s.resource_metadata,
                message_id=s.message_id,
                message_signature=s.message_signature,
            )

    @staticmethod
    def get_meter_statistics(sample_filter, period=None, groupby=None,
                             aggregate=None):
        """Return an iterable of model.Statistics instances.

       The filter must have a meter value set.
       """
        raise NotImplementedError('Statistics not implemented')

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
