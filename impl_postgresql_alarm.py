#
# Author: John Tran <jhtran@att.com>
#         Julien Danjou <julien@danjou.info>
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

"""SQLAlchemy storage backend."""

from __future__ import absolute_import

from eventlet.db_pool import ConnectionPool
from eventlet.support.psycopg2_patcher import make_psycopg_green
make_psycopg_green()

import psycopg2
import re
from psycopg2.extras import NamedTupleCursor, DictCursor
from psycopg2.extras import Json

from oslo.config import cfg

import ceilometer
from ceilometer.alarm.storage import base
from ceilometer.alarm.storage import models as alarm_api_models
from ceilometer.openstack.common import log
from ceilometer import utils

LOG = log.getLogger(__name__)

AVAILABLE_CAPABILITIES = {
    'alarms': {'query': {'simple': False,
                         'complex': False},
               'history': {'query': {'simple': False,
                                     'complex': False}}},
}


AVAILABLE_STORAGE_CAPABILITIES = {
    'storage': {'production_ready': True},
}


class PoolConnection(object):

    """Wraps connection pool to ease of use with transactions"""

    def __init__(self, pool, readonly=False, cursor_factory=None):
        self._pool = pool
        self._readonly = readonly
        self._cursor_factory = (
            NamedTupleCursor if not cursor_factory else cursor_factory)

    def __enter__(self):
        self._conn = self._pool.get()
        self._curr = self._conn.cursor(
            cursor_factory=self._cursor_factory
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
    sql_limit_body = ''
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
    if complex_op == 'or':
        return '(' + ' {} '.format(complex_op).join(items) + ')'
    if complex_op == 'and':
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


def get_connection_opts(conn):
    template = re.compile('[^:/@]+')
    result = template.findall(conn)
    try:
        return {'db_engine': result[0], 'user': result[1],
                'password': result[2], 'host': result[3],
                'port': int(result[4]), 'database': result[5]}
    except ValueError:
        return {'db_engine': result[0], 'user': result[1],
                'port': 5432, 'password': result[2],
                'host': result[3], 'database': result[4]}
    except IndexError:
        return None


class Connection(base.Connection):

    """Put the data into a SQLAlchemy database.

    Tables::

        - meter
          - meter definition
          - { id: meter def id
              name: meter name
              type: meter type
              unit: meter unit
              }
        - sample
          - the raw incoming data
          - { id: sample id
              meter_id: meter id            (->meter.id)
              user_id: user uuid
              project_id: project uuid
              resource_id: resource uuid
              source_id: source id
              resource_metadata: metadata dictionaries
              volume: sample volume
              timestamp: datetime
              message_signature: message signature
              message_id: message uuid
              }
    """
    CAPABILITIES = utils.update_nested(base.Connection.CAPABILITIES,
                                       AVAILABLE_CAPABILITIES)
    STORAGE_CAPABILITIES = utils.update_nested(
        base.Connection.STORAGE_CAPABILITIES,
        AVAILABLE_STORAGE_CAPABILITIES,
    )

    def __init__(self, url):
        self.conn_pool = self._get_connection_pool()

    @staticmethod
    def _get_connection_pool():
        """Returns connection pool to the database"""
        connection_dict = get_connection_opts(cfg.CONF.database.connection)
        if connection_dict:
            return ConnectionPool(psycopg2,
                                  min_size=cfg.CONF.database.min_pool_size,
                                  max_size=cfg.CONF.database.max_pool_size,
                                  max_idle=cfg.CONF.database.idle_timeout,
                                  connect_timeout=cfg.CONF.database.pool_timeout,
                                  host=connection_dict['host'],
                                  port=connection_dict['port'],
                                  user=connection_dict['user'],
                                  password=connection_dict['password'],
                                  database=connection_dict['database'])
        else:
            raise Exception('Wrong connection string is set')

    def upgrade(self):
        raise ceilometer.NotImplementedError('Upgrade not implemented')

    def clear(self):
        raise ceilometer.NotImplementedError('Clear not implemented')

    @staticmethod
    def _row_to_alarm_model(row):
        return alarm_api_models.Alarm(alarm_id=row['alarm_id'],
                                      enabled=row['enabled'],
                                      type=row['type'],
                                      name=row['name'],
                                      description=row['description'],
                                      timestamp=row['timestamp'],
                                      user_id=row['user_id'],
                                      project_id=row['project_id'],
                                      state=row['state'],
                                      state_timestamp=row['state_timestamp'],
                                      ok_actions=row['ok_actions'],
                                      alarm_actions=row['alarm_actions'],
                                      insufficient_data_actions=(
                                          row['insufficient_data_actions']),
                                      rule=row['rule'],
                                      time_constraints=row['time_constraints'],
                                      repeat_actions=row['repeat_actions'])

    def get_alarms(self, name=None, user=None, state=None, meter=None,
                   project=None, enabled=None, alarm_id=None, pagination=None):
        sql_query = ('SELECT alarm.alarm_id, alarm.enabled, alarm.type,'
                     ' alarm.name, alarm.description, alarm.timestamp,'
                     ' users.uuid as user_id, projects.uuid as project_id,'
                     ' alarm.state, alarm.state_timestamp, alarm.ok_actions,'
                     ' alarm.alarm_actions, alarm.insufficient_data_actions,'
                     ' alarm.rule, alarm.time_constraints,'
                     ' alarm.repeat_actions FROM alarm'
                     ' LEFT JOIN users ON alarm.user_id = users.id'
                     ' LEFT JOIN projects ON alarm.project_id = projects.id')
        values = []
        if name:
            sql_query += ' AND name = %s'
            values.append(name)
        if enabled:
            sql_query += ' AND enabled = %s'
            values.append(enabled)
        if user:
            sql_query += ' AND users.uuid = %s'
            values.append(user)
        if project:
            sql_query += ' AND projects.uuid = %s'
            values.append(project)
        if alarm_id:
            sql_query += ' AND alarm_id = %s'
            values.append(alarm_id)
        if state:
            sql_query += ' AND state = %s'
            values.append(state)
        sql_query = sql_query.replace(' AND', ' WHERE', 1)
        with PoolConnection(self.conn_pool, cursor_factory=DictCursor) as db:
            db.execute(sql_query, values)
            res = db.fetchall()
        return (self._row_to_alarm_model(x) for x in res)

    def delete_alarm(self, alarm_id):
        with PoolConnection(self.conn_pool) as db:
            db.execute('DELETE FROM alarm WHERE alarm_id = %s', [alarm_id])

    def create_alarm(self, alarm):
        data = alarm.as_dict()
        user_id = None
        project_id = None
        if data['user_id']:
            user_id_query = 'SELECT id from users WHERE uuid = %s'
            with PoolConnection(self.conn_pool) as db:
                db.execute(user_id_query, [data['user_id']])
                response = db.fetchone()
                if response:
                    user_id = response[0]
                else:
                    raise Exception('Could not find required user_id.')

        if data['project_id']:
            project_id_query = 'SELECT id from projects WHERE uuid = %s'
            with PoolConnection(self.conn_pool) as db:
                db.execute(project_id_query, [data['project_id']])
                response = db.fetchone()
                if response:
                    project_id = response[0]
                else:
                    raise Exception('Could not find required project_id.')

        values = [data['enabled'], data['name'], data['type'],
                  data['description'], data['timestamp'], user_id, project_id,
                  data['state'], Json(data['ok_actions']),
                  Json(data['alarm_actions']),
                  Json(data['insufficient_data_actions']),
                  data['repeat_actions'], Json(data['rule']),
                  Json(data['time_constraints']), data['state_timestamp'],
                  data['alarm_id']]
        query = ('INSERT INTO alarm (enabled, name, type, description,'
                 ' timestamp, user_id, project_id, state, ok_actions,'
                 ' alarm_actions, insufficient_data_actions,'
                 ' repeat_actions, rule,'
                 ' time_constraints, state_timestamp, alarm_id) VALUES (%s,'
                 ' %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                 ' RETURNING enabled, name, type, description,'
                 ' timestamp, user_id, project_id, state, ok_actions,'
                 ' alarm_actions, insufficient_data_actions,'
                 ' repeat_actions, rule,'
                 ' time_constraints, state_timestamp, alarm_id;')
        with PoolConnection(self.conn_pool, cursor_factory=DictCursor) as db:
            db.execute(query, values)
            res = db.fetchone()
        res['user_id'] = data['user_id']
        res['project_id'] = data['project_id']
        return self._row_to_alarm_model(res)

    @staticmethod
    def _row_to_alarm_change_model(row):
        return alarm_api_models.AlarmChange(event_id=row.event_id,
                                            alarm_id=row.alarm_id,
                                            type=row.type,
                                            detail=row.detail,
                                            user_id=row.user_id,
                                            project_id=row.project_id,
                                            on_behalf_of=row.on_behalf_of,
                                            timestamp=row.timestamp)

    def get_alarm_changes(self, alarm_id, on_behalf_of,
                          user=None, project=None, type=None,
                          start_timestamp=None, start_timestamp_op=None,
                          end_timestamp=None, end_timestamp_op=None):
        values = []
        sql_query = ('SELECT alarm_change.event_id,'
                     ' alarm.alarm_id, alarm_change.type,'
                     ' alarm_change.detail, users.uuid as user_id,'
                     ' p1.uuid as project_id, p2.uuid as on_behalf_of,'
                     ' alarm_change.timestamp FROM alarm_change'
                     ' JOIN alarm ON alarm_change.alarm_id = alarm.id'
                     ' JOIN users ON alarm_change.user_id = users.id'
                     ' JOIN projects p1 ON alarm_change.project_id = p1.id'
                     ' JOIN projects p2 ON alarm_change.on_behalf_of = p2.id')
        sql_query += ' WHERE alarm.alarm_id = %s'
        values.append(alarm_id)
        if on_behalf_of is not None:
            sql_query += ' AND p2.uuid = %s'
            values.append(on_behalf_of)
        if project is not None:
            sql_query += ' AND p1.uuid = %s'
            values.append(project)
        if type is not None:
            sql_query += ' AND alarm_change.type = %s'
            values.append(type)
        if start_timestamp:
            if start_timestamp_op == 'gt':
                sql_query += ' AND alarm_change.timestamp > %s'
            else:
                sql_query += ' AND alarm_change.timestamp >= %s'
            values.append(start_timestamp)
        if end_timestamp:
            if end_timestamp_op == 'lt':
                sql_query += ' AND alarm_change.timestamp < %s'
            else:
                sql_query += ' AND alarm_change.timestamp <= %s'
        sql_query += ' ORDER BY timestamp DESC;'
        with PoolConnection(self.conn_pool) as db:
            db.execute(sql_query, values)
            res = db.fetchall()
        return (self._row_to_alarm_change_model(x) for x in res)

    def record_alarm_change(self, alarm_change):
        values = [alarm_change['event_id'], alarm_change['type'],
                  Json(alarm_change['detail']), alarm_change['timestamp'],
                  alarm_change['alarm_id'], alarm_change['project_id'],
                  alarm_change['user_id']]
        with PoolConnection(self.conn_pool) as db:
            db.execute('SELECT id FROM projects WHERE uuid = %s',
                       [alarm_change['on_behalf_of']])
            on_behalf_of_query = db.fetchone().id
        values.insert(1, on_behalf_of_query)
        sql_query = ('INSERT INTO alarm_change (event_id, alarm_id,'
                     ' on_behalf_of,'
                     ' project_id, user_id, type, detail, timestamp)'
                     ' SELECT %s, alarm.id, %s, projects.id,'
                     ' users.id, %s, %s, %s FROM alarm, projects, users'
                     ' WHERE alarm.alarm_id = %s AND projects.uuid = %s AND'
                     ' users.uuid = %s')
        with PoolConnection(self.conn_pool) as db:
            db.execute(sql_query, values)

    def query_alarms(self, filter_expr=None, orderby=None, limit=None):
        sql_query = ('SELECT * FROM ('
                     'SELECT alarm.alarm_id, alarm.enabled, alarm.type,'
                     ' alarm.name, alarm.description, alarm.timestamp,'
                     ' users.uuid as user_id, projects.uuid as project_id,'
                     ' alarm.state, alarm.state_timestamp, alarm.ok_actions,'
                     ' alarm.alarm_actions, alarm.insufficient_data_actions,'
                     ' alarm.rule, alarm.time_constraints,'
                     ' alarm.repeat_actions FROM alarm'
                     ' LEFT JOIN users ON alarm.user_id = users.id'
                     ' LEFT JOIN projects ON alarm.project_id = projects.id'
                     ') as c')
        values = []
        if filter_expr:
            sql_where_body, values = transform_filter(filter_expr)
            sql_query += sql_where_body
        if orderby:
            sql_query += transform_orderby(orderby)
        if limit:
            sql_query += ' LIMIT %s'
            values.append(limit)
        with PoolConnection(self.conn_pool, cursor_factory=DictCursor) as db:
            db.execute(sql_query, values)
            res = db.fetchall()
        return (self._row_to_alarm_model(x) for x in res)

    def query_alarm_history(self, filter_expr=None, orderby=None, limit=None):
        sql_query = ('SELECT * FROM ('
                     'SELECT alarm_change.event_id,'
                     ' alarm.alarm_id, alarm_change.type,'
                     ' alarm_change.detail, users.uuid as user_id,'
                     ' p1.uuid as project_id, p2.uuid as on_behalf_of,'
                     ' alarm_change.timestamp FROM alarm_change'
                     ' JOIN alarm ON alarm_change.alarm_id = alarm.id'
                     ' JOIN users ON alarm_change.user_id = users.id'
                     ' JOIN projects p1 ON alarm_change.project_id = p1.id'
                     ' JOIN projects p2 ON alarm_change.on_behalf_of = p2.id'
                     ') as c')
        values = []
        if filter_expr:
            sql_where_body, values = transform_filter(filter_expr)
            sql_query += sql_where_body
        if orderby:
            sql_query += transform_orderby(orderby)
        if limit:
            sql_query += ' LIMIT %s'
            values.append(limit)
        with PoolConnection(self.conn_pool) as db:
            db.execute(sql_query, values)
            res = db.fetchall()
        return (self._row_to_alarm_change_model(x) for x in res)

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
