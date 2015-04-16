# -*- encoding: utf-8 -*-
# Copyright © 2015 Servionica, LLC (I-Teco)
#
# Authors: Dmirty Kubatkin <kubatkin@servionica.ru>
#          Alexander Chadin <joker946@gmail.com>
#          Alexander Stavitsky <alexandr.stavitsky@gmail.com>
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

import datetime

import psycopg2
from psycopg2.extras import DictCursor
from psycopg2.extras import Json

from oslo.config import cfg
from urlparse import urlparse
import ceilometer
from ceilometer.alarm.storage import base
from ceilometer.alarm.storage import models as alarm_api_models
from ceilometer.openstack.common import log
from ceilometer import utils

import ceilometer.storage.postgresql.utils as psql_utils
from ceilometer.storage.postgresql.utils import PoolConnection
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
        connection = urlparse(cfg.CONF.database.connection)
        if connection:
            return ConnectionPool(psycopg2,
                                  min_size=cfg.CONF.database.min_pool_size,
                                  max_size=cfg.CONF.database.max_pool_size,
                                  max_idle=cfg.CONF.database.idle_timeout,
                                  connect_timeout=cfg.CONF.database.pool_timeout,
                                  host=connection.hostname,
                                  port=connection.port or 5432,
                                  user=connection.username,
                                  password=connection.password,
                                  database=connection.path[1:])
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
        return (self._row_to_alarm_model(alarm) for alarm in res)

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
            sql_where_body, values = psql_utils.transform_filter(filter_expr)
            sql_query += sql_where_body
        if orderby:
            sql_query += psql_utils.transform_orderby(orderby)
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
            sql_where_body, values = psql_utils.transform_filter(filter_expr)
            sql_query += sql_where_body
        if orderby:
            sql_query += psql_utils.transform_orderby(orderby)
        if limit:
            sql_query += ' LIMIT %s'
            values.append(limit)
        with PoolConnection(self.conn_pool) as db:
            db.execute(sql_query, values)
            res = db.fetchall()
        return (self._row_to_alarm_change_model(x) for x in res)

    def update_alarm(self, alarm):
        alarm = alarm.as_dict()
        values = []
        sql_query = (' UPDATE alarm'
                     ' SET')

        if alarm['user_id']:
            user_id_q = ("SELECT id FROM users"
                         " WHERE uuid = %s")
            with PoolConnection(self.conn_pool) as db:
                db.execute(user_id_q, (alarm['user_id'],))
                user_resp = db.fetchone()
            if user_resp:
                user_id = user_resp.id
                sql_query += " user_id = %s,"
                values.append(user_id)
            else:
                LOG.debug(_("User does not exist in DB"))
                return

        if alarm['project_id']:
            project_id_q = ('SELECT id from projects'
                            ' WHERE uuid = %s')
            with PoolConnection(self.conn_pool) as db:
                db.execute(project_id_q, (alarm['project_id'],))
                project_resp = db.fetchone()
            if project_resp:
                project_id = project_resp.id
                sql_query += " project_id = %s,"
                values.append(alarm['project_id'])
            else:
                LOG.debug(_("Project does not exist in DB"))
                return
        # Note (alexstav): "'key' in dict" form required
        # for values with bool or empty array types
        if 'enabled' in alarm:
            sql_query += ' enabled = %s,'
            values.append(alarm['enabled'])
        if 'name' in alarm:
            sql_query += ' name = %s,'
            values.append(alarm['name'])
        if 'description' in alarm:
            sql_query += ' description = %s,'
            values.append(alarm['description'])
        if 'state' in alarm:
            sql_query += ' state = %s,'
            values.append(alarm['state'])
        if 'alarm_actions' in alarm:
            sql_query += ' alarm_actions = %s,'
            values.append(Json(alarm['alarm_actions']))
        if 'ok_actions' in alarm:
            sql_query += ' ok_actions = %s,'
            values.append(Json(alarm['ok_actions']))
        if 'insufficient_data_actions' in alarm:
            sql_query += ' insufficient_data_actions = %s,'
            values.append(Json(alarm['insufficient_data_actions']))
        if 'repeat_actions' in alarm:
            sql_query += ' repeat_actions = %s,'
            values.append(alarm['repeat_actions'])
        if 'time_constraints' in alarm:
            sql_query += ' time_constraints = %s,'
            values.append(Json(alarm['time_constraints']))
        if 'rule' in alarm:
            sql_query += ' rule = %s'
            values.append(Json(alarm['rule']))

        sql_query = sql_query.rstrip(',')
        sql_query += ' WHERE alarm_id = %s'
        values.append(alarm['alarm_id'])

    with PoolConnection(self.conn_pool) as db:
        db.execute(sql_query, values)
    # returns first Alarm object from generator
    stored_alarm = self.get_alarms(alarm_id=alarm['alarm_id']).next()
    LOG.debug(
        _("/nStored alarm camed from get_alarms():\n{}\n".format(str(stored_alarm.as_dict()))))
    return stored_alarm


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
