from ceilometer_local_lib import PoolConnection

def record_alarm_change(self, alarm_change):
    sql_query = 'INSERT INTO alarm_change ()'