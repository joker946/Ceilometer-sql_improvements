from ceilometer_local_lib import PoolConnection


def make_list(resp):
    result = []
    for r in resp:
        result.append(r[0])
    return result


def get_users(source=None):
    with PoolConnection() as cur:
        if source:
            cur.execute("SELECT uuid FROM users WHERE source_id=%s;",
                        (source,))
        else:
            cur.execute("SELECT uuid FROM users;")
        resp = make_list(cur.fetchall())
    return resp

print get_users()
