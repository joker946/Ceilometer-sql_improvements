import psycopg2
import datetime


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


def clear_expired_metering_data(ttl):
    """Clear expired data from the backend storage system.

    Clearing occurs according to the time-to-live.
    :param ttl: Number of seconds to keep records for.
    """
    date = datetime.datetime.now() - datetime.timedelta(seconds=ttl)
    print date
    query = "DELETE FROM samples WHERE samples.timestamp < %s;"
    with PoolConnection() as cur:
        cur.execute(query, [date])
    # TODO Remove Meter definitions with no matching samples

clear_expired_metering_data(360)
