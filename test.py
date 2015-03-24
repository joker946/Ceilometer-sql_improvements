import six
from psycopg2.extras import Json


metaquery = {'metadata.status': 'active',
             'metadata.memory_mb': 512,
             'metadata.image.name': 'cirros-0.3.3-x86_64'}


def make_metaquery(metastr, value):
    elemets = metastr.split('.')[1:]
    if not elemets:
        return None
    jsq = dict()
    jsq_last = jsq
    for e in elemets[:-1]:
        jsq_last[e] = dict()
        jsq_last = jsq_last[e]
    jsq_last[elemets[-1]] = value
    return jsq


def apply_metaquery_filter(metaquery):
    meta_filter = dict()
    for key, value in six.iteritems(metaquery):
        meta_filter.update(make_metaquery(key, value))
    return ' AND metadata @> %s' % Json(meta_filter).getquoted()
print apply_metaquery_filter(metaquery)
