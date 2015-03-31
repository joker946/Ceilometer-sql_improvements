from ceilometer_local_lib import make_sql_query_from_filter
from ceilometer_local_lib import PoolConnection
from ceilometer_local_lib import Object
import six

# MOCK OBJECT
metaquery = {'metadata.status': u'active',
             'metadata.memory_mb': 512,
             'metadata.image.name': 'cirros-0.3.3-x86_64'}


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
    for k, value in six.iteritems(metaquery):
        # TODO (alexchadin) is bool req?
        key = k[9:]  # strip out 'metadata.' prefix
        l = key.split('.')
        if len(l) == 1:
            if isinstance(value, (int, long, bool)):
                sql_metaquery += '\"{}\": {}, '.format(key, value)
            else:
                sql_metaquery += '\"{}\": \"{}\", '.format(key, value)
        else:
            sql_metaquery += '\"{}\": {}, '.format(
                l[0], make_complex_json_query(l, 1, value))
    # ':-2' strip out is needed to delete ', '
    return ' AND metadata @> \'{{{}}}\''.format(sql_metaquery[:-2])

print apply_metaquery_filter(metaquery)
