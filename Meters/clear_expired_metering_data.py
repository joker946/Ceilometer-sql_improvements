from ceilometer_local_lib import PoolConnection
import datetime


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
