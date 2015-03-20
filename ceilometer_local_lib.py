import psycopg2
from psycopg2.extras import NamedTupleCursor
import six


def make_complex_json_query(l, n, v):
    """Recursive function that makes complex query with 2 or more nesting.

    :param l: list of splitted substring.
    :param n: initial index (default=0).
    :param v: value that should be returned at the end of recursion.
    """
    if n == len(l):
        if isinstance(v, (int, long, bool)):
            return str(v)
        else:
            return '\"{}\"'.format(str(v))

    while n < len(l):
        n += 1
        return ('{\"' + l[n - 1] + '\": ' +
                str(make_complex_json_query(l, n, v)) + '}')


def apply_metaquery_filter(metaquery):
    """Apply provided metaquery filter to existing query.

    :param session: session used for original query
    :param query: Query instance
    :param metaquery: dict with metadata to match on.
    """
    sql_metaquery = ''
    subq_and = ' AND metadata @> \'{}\''
    for k, value in six.iteritems(metaquery):
        # TODO (alexchadin) is bool req?
        key = k[9:]  # strip out 'metadata.' prefix
        l = key.split('.')
        res = make_complex_json_query(l, 0, value)
        sql_metaquery += subq_and.format(res)
    return sql_metaquery


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
        sql_where_body += apply_metaquery_filter(sample_filter.metaquery)
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
