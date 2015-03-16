import psycopg2
import datetime

def clear_expired_metering_data(ttl):
        """Clear expired data from the backend storage system.

        Clearing occurs according to the time-to-live.
        :param ttl: Number of seconds to keep records for.
        """
        date = datetime.datetime.now() - datetime.timedelta(seconds=ttl)
        query = "DELETE FROM samples WHERE samples.timestamp < %s;"
        conn = psycopg2.connect("dbname=ceilometer user=alexchadin")
        cur = conn.cursor()
        cur.execute(query, [date])
        cur.close()
        conn.close()

clear_expired_metering_data(36000)