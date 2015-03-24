import psycopg2
from psycopg2.extras import NamedTupleCursor
from psycopg2.extras import Json
import six


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
        sql_where_body += subq_and.format('user_id = %s')
        values.append(sample_filter.user)
    if sample_filter.project:
        sql_where_body += subq_and.format('projects_id = %s')
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


class Object(object):
    pass


class PoolConnection(object):

    def __init__(self, readonly=False):
        self._conn = psycopg2.connect("dbname=ceilometer user=alexchadin")
        self._readonly = readonly

    def __enter__(self):
        self._cur = self._conn.cursor(
            cursor_factory=NamedTupleCursor
        )
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
