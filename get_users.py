import psycopg2


class PoolConnection(object):

    def __init__(self, readonly=False):
        self._conn = psycopg2.connect("dbname=ceilometer user=alexchadin")
        self._readonly = readonly

    def __enter__(self):
        self._cur = self._conn.cursor()
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
    print resp

get_users()
