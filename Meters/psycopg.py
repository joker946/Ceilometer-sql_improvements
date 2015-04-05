import datetime
t = {
    'counter_name': 'image_test',
    'user_id': None,
    'message_signature': 'cb823c822932b57441e1c92c993a810cba62a0a3177b28ee4018813943992714',
    'timestamp': datetime.datetime(2015, 4, 5, 12, 33, 21),
    'resource_id': '9c37c6d4-fda3-4b0e-a114-363f35934ee8',
    'message_id': 'f2126a3a-db8f-11e4-b020-00505696f68d',
    'source': 'openstack',
    'counter_unit': 'image',
    'counter_volume': 1,
    'project_id': 'f2cc0bbd0b724e41b0b70059ea2b9f91',
    'resource_metadata': {
        'status': 'active',
        'name': 'cirros-0.3.3-x86_64',
        'deleted': False,
        'container_format': 'bare',
        'created_at': '2014-12-18T13: 54: 04.760635',
        'disk_format': 'qcow2',
        'updated_at': '2014-12-18T13: 54: 05.140236',
        'properties': {

        },
        'protected': False,
        'checksum': '133eae9fb1c98f45894a4e60d8736619',
        'min_disk': 0,
        'is_public': True,
        'deleted_at': None,
        'min_ram': 0,
        'size': 13200896
    },
    'counter_type': u'gauge'
}
import psycopg2
import json
conn = psycopg2.connect("dbname=ceilometer user=alexchadin")
cur = conn.cursor()
"""
json_data = open('samples.json')
data = json.load(json_data)
for i in data:
    d = json.dumps(i)
    cur.execute("SELECT \"write_sample\"(%s);", (d,))
    conn.commit()
"""
dthandler = lambda obj: obj.isoformat() if isinstance(
    obj, datetime.datetime) else None
d = json.dumps(t, ensure_ascii=False, default=dthandler)
cur.execute("SELECT \"write_sample\"(%s);", (d,))
conn.commit()
cur.close()
conn.close()
