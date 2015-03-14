#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2
import sys

con = None

try:
    con = psycopg2.connect("dbname=ceilometer user=alexchadin")
    cur = con.cursor()
    # sources
    cur.execute('CREATE SEQUENCE sources_id_seq;')
    cur.execute('CREATE TABLE IF NOT EXISTS sources ('
                ' id bigserial PRIMARY KEY,'
                ' name text'
                ');')
    cur.execute('ALTER TABLE sources ALTER COLUMN id SET DEFAULT'
                ' NEXTVAL(\'sources_id_seq\')')
    # users
    cur.execute("CREATE SEQUENCE users_id_seq;")
    cur.execute('CREATE TABLE IF NOT EXISTS users ('
                ' id bigserial PRIMARY KEY,'
                ' uuid UUID,'
                ' source_id bigint references sources(id)'
                ');')
    cur.execute('ALTER TABLE users ALTER COLUMN id SET DEFAULT'
                ' NEXTVAL(\'users_id_seq\')')
    # projects
    cur.execute('CREATE SEQUENCE projects_id_seq;')
    cur.execute('CREATE TABLE IF NOT EXISTS projects ('
                ' id bigserial PRIMARY KEY,'
                ' uuid UUID,'
                ' source_id bigint references sources(id)'
                ');')
    cur.execute('ALTER TABLE projects ALTER COLUMN id SET DEFAULT'
                ' NEXTVAL(\'projects_id_seq\')')
    # resources
    cur.execute('CREATE SEQUENCE resources_id_seq;')
    cur.execute('CREATE TABLE IF NOT EXISTS resources ('
                ' id bigserial PRIMARY KEY,'
                ' resource_id text,'  # for what?
                ' user_id bigint references users(id),'
                ' project_id bigint references projects(id),'
                ' source_id bigint references sources(id)'
                ');')
    cur.execute('ALTER TABLE resources ALTER COLUMN id SET DEFAULT'
                ' NEXTVAL(\'resources_id_seq\')')
    # meters
    cur.execute('CREATE SEQUENCE meters_id_seq;')
    cur.execute('CREATE TABLE IF NOT EXISTS meters ('
                ' id bigserial PRIMARY KEY,'
                ' name text,'
                ' type text,'
                ' unit text'
                ');')
    cur.execute('ALTER TABLE meters ALTER COLUMN id SET DEFAULT'
                ' NEXTVAL(\'meters_id_seq\')')
    # samples
    cur.execute('CREATE SEQUENCE samples_id_seq;')
    cur.execute('CREATE TABLE IF NOT EXISTS samples ('
                ' id bigserial PRIMARY KEY,'
                ' user_id bigint references users(id),'
                ' project_id bigint references projects(id),'
                ' resource_id bigint references resources(id),'
                ' meter_id bigint references meters(id),'
                ' source_id bigint references sources(id),'
                ' timestamp timestamp,'
                ' message_id text,'
                ' message_signature text,'
                ' recorded_at timestamp,'
                ' volume double precision,'
                ' metadata jsonb'
                ');')
    cur.execute('ALTER TABLE samples ALTER COLUMN id SET DEFAULT'
                ' NEXTVAL(\'samples_id_seq\')')
    con.commit()



finally:
    if con:
        con.close()
