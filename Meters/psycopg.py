import datetime
from ceilometer_local_lib import PoolConnection
t = {
    "counter_name": "instance:m1.tiny",
    "user_id": "3d622ea5a70a42d3aae549ddfc1ef355",
    "message_signature": "f9d8bc4ffe438735a9faed75d7854e2873f7314f78afa4993abac989541b8da2",
    "timestamp": "2015-04-28T17:35:46",
    "resource_id": "c73270fc-f314-4aeb-8ff7-d94aef3e37d3",
    "message_id": "dc1fff94-ea71-11e4-97f9-005056968dc7",
    "source": "openstack",
    "counter_unit": "instance",
    "counter_volume": 1,
    "project_id": "f2cc0bbd0b724e41b0b70059ea2b9f91",
    "resource_metadata": {
        "status": "active",
        "ramdisk_id": None,
        "display_name": "alexmustwin",
        "name": "instance-00000006",
        "disk_gb": 1,
        "kernel_id": None,
        "image": {
            "id": "9c37c6d4-fda3-4b0e-a114-363f35934ee8",
            "links": [
                {
                    "href": "http://controller1:8774/3de35c7ecdca4974b5af5adaca49ca9b/images/9c37c6d4-fda3-4b0e-a114-363f35934ee8",
                    "rel": "bookmark"
                }
            ],
            "name": "cirros-0.3.3-x86_64"
        },
        "ephemeral_gb": 0,
        "host": "718734f24c0996b10bd544553ad09029d9b95180cc1ceff2ecc657c6",
        "memory_mb": 512,
        "instance_type": "1",
        "vcpus": 1,
        "root_gb": 1,
        "image_ref": "9c37c6d4-fda3-4b0e-a114-363f35934ee8",
        "flavor": {
            "name": "m1.tiny",
            "links": [
                {
                    "href": "http://controller1:8774/3de35c7ecdca4974b5af5adaca49ca9b/flavors/1",
                    "rel": "bookmark"
                }
            ],
            "ram": 512,
            "ephemeral": 0,
            "vcpus": 1,
            "disk": 1,
            "id": "1"
        },
        "OS-EXT-AZ:availability_zone": "nova",
        "image_ref_url": "http://controller1:8774/3de35c7ecdca4974b5af5adaca49ca9b/images/9c37c6d4-fda3-4b0e-a114-363f35934ee8"
    },
    "counter_type": "gauge"
}

sample = {
    "counter_name": "cpu",
    "user_id": "3d622ea5a70a42d3aae549ddfc1ef355",
    "message_signature": "a8742176d9b8009ca43c40cac241f4c8d2c3b5203cccb832ab0ee49c125eaf3b",
    "timestamp": "2015-04-23T18:05:44",
    "resource_id": "963412f3-326b-48bb-8df7-14619dd0deee",
    "message_id": "37db6808-e9ca-11e4-97f9-005056968dc7",
    "source": "openstack",
    "counter_unit": "ns",
    "counter_volume": 7051350000000,
    "project_id": "f2cc0bbd0b724e41b0b70059ea2b9f91",
    "resource_metadata": {
        "status": "active",
        "cpu_number": 1,
        "ramdisk_id": None,
        "display_name": "alexmustwin3",
        "name": "instance-00000008",
        "disk_gb": 1,
        "kernel_id": None,
        "image": {
            "id": "9c37c6d4-fda3-4b0e-a114-363f35934ee8",
            "links": [
                {
                    "href": "http://controller1:8774/3de35c7ecdca4974b5af5adaca49ca9b/images/9c37c6d4-fda3-4b0e-a114-363f35934ee8",
                    "rel": "bookmark"
                }
            ],
            "name": "cirros-0.3.3-x86_64"
        },
        "ephemeral_gb": 0,
        "host": "718734f24c0996b10bd544553ad09029d9b95180cc1ceff2ecc657c6",
        "memory_mb": 512,
        "instance_type": "1",
        "vcpus": 1,
        "root_gb": 1,
        "image_ref": "9c37c6d4-fda3-4b0e-a114-363f35934ee8",
        "flavor": {
            "name": "m1.tiny",
            "links": [
                {
                    "href": "http://controller1:8774/3de35c7ecdca4974b5af5adaca49ca9b/flavors/1",
                    "rel": "bookmark"
                }
            ],
            "ram": 512,
            "ephemeral": 0,
            "vcpus": 1,
            "disk": 1,
            "id": "1"
        },
        "OS-EXT-AZ:availability_zone": "nova",
        "image_ref_url": "http://controller1:8774/3de35c7ecdca4974b5af5adaca49ca9b/images/9c37c6d4-fda3-4b0e-a114-363f35934ee8"
    },
    "counter_type": "cumulative"
}
import psycopg2
import json
data = json.dumps(t)
with PoolConnection() as db:
    db.execute("SELECT \"write_sample\"(%s);", (data,))
"""
json_data = open('samples.json')
data = json.load(json_data)
for i in data:
    d = json.dumps(i)
    cur.execute("SELECT \"write_sample\"(%s);", (d,))
    conn.commit()
"""
"""dthandler = lambda obj: obj.isoformat() if isinstance(
    obj, datetime.datetime) else None
d = json.dumps(t, ensure_ascii=False, default=dthandler)
cur.execute("SELECT \"write_sample\"(%s);", (d,))
conn.commit()"""
