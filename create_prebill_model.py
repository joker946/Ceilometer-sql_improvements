#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2

con = None

try:
    con = psycopg2.connect("dbname=dbname user=user")
    cur = con.cursor()

    cur.execute('CREATE TABLE IF NOT EXISTS prebill ('
                ' seq bigserial,'
                ' class text,'
                ' obj_key text,'
                ' event_type event_type,'  # should be event_type?
                ' stamp_start timestamp,'
                ' stamp_end timestamp,'
                ' volume float,'
                ' owner_key text,'
                ' tenant_key text,'
                ' counter_type counter_type,'  # should be counter_type?
                ' last_value float,'
                ' last_timestamp timestamp,'
                ' CONSTRAINT prebill_constraint UNIQUE (class, obj_key,'
                ' event_type, owner_key, tenant_key, counter_type, updating);'
                )
    con.commit()

finally:
    if con:
        con.close()