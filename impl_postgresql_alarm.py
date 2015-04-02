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
import os

from eventlet.db_pool import ConnectionPool
from eventlet.support.psycopg2_patcher import make_psycopg_green
make_psycopg_green()

import psycopg2
import json

from psycopg2.extras import NamedTupleCursor
from psycopg2.extras import Json

from oslo.config import cfg
from oslo.db.sqlalchemy import session as db_session
from sqlalchemy import desc

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
        raise ceilometer.NotImplementedError('Upgrade not implemented')

    def create_alarm(self, alarm):
        data = alarm.as_dict()
        user_id = None
        project_id = None
        if data['user_id']:
            user_id_query = 'SELECT id from users WHERE uuid = %s'
            with PoolConnection() as db:
                db.execute(user_id_query, [data['user_id']])
                response = db.fetchone()
                if response:
                    user_id = response[0]
                else:
                    raise Exception('Could not find required user_id.')

        if data['project_id']:
            project_id_query = 'SELECT id from projects WHERE uuid = %s'
            with PoolConnection() as db:
                db.execute(project_id_query, [data['project_id']])
                response = db.fetchone()
                if response:
                    project_id = response[0]
                else:
                    raise Exception('Could not find required project_id.')

        values = [data['enabled'], data['name'], data['type'],
                  data['description'],
                  data['timestamp'], user_id, project_id, data['state'],
                  data['ok_actions'], Json(data['alarm_actions']),
                  data['insufficient_data_actions'], data['repeat_actions'],
                  data['rule'], data['time_constraints']]
        print values
        query = ('INSERT INTO alarm (enabled, name, type, description,'
                 ' timestamp, user_id, project_id, state, ok_actions,'
                 ' alarm_actions, insufficient_data_actions,'
                 ' repeat_actions, rule,'
                 ' time_constraints) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,'
                 ' %s, %s, %s, %s, %s, %s);')
        with PoolConnection() as db:
            db.execute(query, values)
    def clear(self):
        raise ceilometer.NotImplementedError('Clear not implemented')

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
