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


from ceilometer.openstack.common.gettextutils import _  # noqa
from ceilometer.storage import base
from ceilometer.storage import models
from ceilometer import utils

LOG = log.getLogger(__name__)


AVAILABLE_CAPABILITIES = {
    'meters': {'pagination': False,
               'query': {'simple': False,
                         'metadata': False,
                         'complex': False}},
    'resources': {'pagination': False,
                  'query': {'simple': False,
                            'metadata': False,
                            'complex': False}},
    'samples': {'pagination': False,
                'groupby': False,
                'query': {'simple': False,
                          'metadata': False,
                          'complex': False}},
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


class PoolConnection(object):

    """Wraps connection pool to ease of use with transactions"""

    def __init__(self, pool, readonly=False):
        self._pool = pool
        self._readonly = readonly

    def __enter__(self):
        self._conn = self.pool.get()
        self._curr = self._conn.cursor()
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

        self.pool.put(self._conn)


class PostgreSQLStorage(base.StorageEngine):

    """Put the data into a PostgreSQL database."""

    @staticmethod
    def get_connection(self, conf):
        """Return a Connection instance based on the configuration settings."""
        return Connection(conf)


class Connection(base.Connection):

    """PostgreSQL connections."""

    def __init__(self, conf):
        """Constructor."""
        self.conn_pool = self._get_connection_pool(conf.postgres)
        self._CAPABILITIES = utils.update_nested(self.DEFAULT_CAPABILITIES,
                                                 AVAILABLE_CAPABILITIES)

    @staticmethod
    def _get_connection_pool(db_conf):
        """Returns connection pool to the database"""
        return ConnectionPool(psycopg2,
                              min_size=db_conf.min_size,
                              max_size=db_conf.max_size,
                              max_idle=db_conf.max_idle,
                              connect_timeout=db_conf.timeout,
                              host=db_conf.host,
                              port=db_conf.port,
                              user=db_conf.user,
                              password=db_conf.password,
                              database=db_conf.database)

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
        with PoolConnection(self.conn_pool) as db:
            db.execute('INSERT INTO samples ')

    @staticmethod
    def clear_expired_metering_data(ttl):
        """Clear expired data from the backend storage system according to the
       time-to-live.

       :param ttl: Number of seconds to keep records for.

       """
        raise NotImplementedError('Clearing samples not implemented')

    @staticmethod
    def get_users(source=None):
        """Return an iterable of user id strings.

       :param source: Optional source filter.
       """
        raise NotImplementedError('Users not implemented')

    @staticmethod
    def get_projects(source=None):
        """Return an iterable of project id strings.

       :param source: Optional source filter.
       """
        raise NotImplementedError('Projects not implemented')

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

    @staticmethod
    def get_meters(user=None, project=None, resource=None, source=None,
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
        raise NotImplementedError('Meters not implemented')

    @staticmethod
    def get_samples(sample_filter, limit=None):
        """Return an iterable of model.Sample instances.

       :param sample_filter: Filter.
       :param limit: Maximum number of results to return.
       """
        raise NotImplementedError('Samples not implemented')

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

    @staticmethod
    def get_capabilities():
        """Return an dictionary representing the capabilities of this driver.
       """
        return self._CAPABILITIES
