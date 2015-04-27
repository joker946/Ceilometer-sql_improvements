import plpy
jdata = ''
SD = {}


create or replace function write_sample(jdata text) returns void as $$

try:
    import simplejson as json
except ImportError:
    import json

from plpy import spiexceptions
from plpy import prepare as prep
from dateutil import parser
data = json.loads(jdata)

plpy.info('start')


def upsert(upd, ins, field, data):
    """ Upsert capabale function. Can do select-insert or update-insert,
   depends on upd parameter"""
    result = plpy.execute(upd, data)
    if not result:
        with plpy.subtransaction():
            try:
                result = plpy.execute(ins, data)
            except spiexceptions.UniqueViolation:
                result = plpy.execute(upd, data)

    return result[0][field]


def fillBaseRefs():
    brefs_sel = SD.setdefault('brefs_sel',
                              prep("SELECT i.id as id, ip.value as base_id"
                                   " FROM glance_images as i"
                                   " LEFT OUTER JOIN ("
                                   "  SELECT image_id, value"
                                   "  FROM glance_image_properties"
                                   "  WHERE name = 'base_image_ref') as ip"
                                   " ON i.id = ip.image_id"))

    result = plpy.execute(brefs_sel)
    brefs = {}
    for record in result:
        brefs[record['id']] = record['base_id']

    SD['brefs'] = brefs


def checkBaseRef(rid):
    bref_sel = SD.setdefault('bref_sel',
                             prep("SELECT i.id as id, ip.value as base_id"
                                  " FROM glance_images as i"
                                  " LEFT OUTER JOIN ("
                                  "  SELECT image_id, value"
                                  "  FROM glance_image_properties"
                                  "  WHERE image_id = $1"
                                  "  AND name = 'base_image_ref') as ip"
                                  " ON i.id = ip.image_id"
                                  " WHERE i.id = $1", ['text']))
    result = plpy.execute(bref_sel, [rid])
    if result:
        SD['brefs'][rid] = result[0]['base_id']
        if result[0]['base_id']:
            return result[0]['base_id']

    return rid


def imageSuffix(rid):
    def getBRef(ref):
        if ref in SD['brefs']:
            bref = SD['brefs'][ref]
            return bref if bref else ref
        else:
            return checkBaseRef(ref)

    result = rid
    if "brefs" not in SD:
        fillBaseRefs()
    tmp = getBRef(result)
    while tmp != result:
        result = tmp
        tmp = getBRef(result)
    return "+%s" % result


def fillVolumeTypes():
    volts_sel = SD.setdefault('volts_sel',
                              prep("SELECT id, name FROM volume_types"))
    result = plpy.execute(volts_sel)
    voltsuf = {}
    for record in result:
        voltsuf[record['id']] = '-sata' if 'sata' in record['name'] else ''

    SD['voltsuf'] = voltsuf


def checkVolumeType(volt):
    volt_sel = SD.setdefault('volt_sel',
                             prep("SELECT name FROM volume_types"
                                  " WHERE id = $1", ['text']))
    result = plpy.execute(volt_sel, [volt])
    if result:
        SD['voltsuf'][volt] = '-sata' if 'sata' in result[0]['name'] else ''
        return SD['voltsuf'][volt]
    else:
        return ''


def volumeSuffix(volt):
    if volt in SD['voltsuf']:
        return SD['voltsuf'][volt]
    else:
        return checkVolumeType(volt)

plpy.info('before_sel_write')
source_sel = SD.setdefault('source_sel',
                           prep("SELECT id FROM sources WHERE name = $1",
                                ['text']))
source_ins = SD.setdefault('source_ins',
                           prep("INSERT INTO sources (name) VALUES ($1)"
                                " RETURNING id", ['text']))
source_id = upsert(source_sel, source_ins, 'id', [data['source']])
plpy.info('after_sel_write')
if data['user_id']:
    user_sel = SD.setdefault('user_sel',
                             prep("SELECT id FROM users WHERE"
                                  " uuid = $1 AND source_id = $2",
                                  ['uuid', 'bigint']))
    user_ins = SD.setdefault('user_ins',
                             prep("INSERT INTO users (uuid, source_id)"
                                  " VALUES ($1, $2) RETURNING id",
                                  ['uuid', 'bigint']))
    user_id = upsert(user_sel, user_ins, 'id', [data['user_id'], source_id])
else:
    user_id = None

project_sel = SD.setdefault('project_sel',
                            prep("SELECT id FROM projects WHERE "
                                 "uuid = $1 AND source_id = $2",
                                 ['uuid', 'bigint']))
project_ins = SD.setdefault('project_ins',
                            prep("INSERT INTO projects (uuid, source_id)"
                                 " VALUES ($1, $2) RETURNING id",
                                 ['uuid', 'bigint']))
project_id = upsert(project_sel, project_ins, 'id',
                    [data['project_id'], source_id])

resource_sel = SD.setdefault('resource_sel',
                             prep("SELECT id FROM resources WHERE "
                                  "resource_id = $1 AND user_id = $2 AND"
                                  " project_id = $3 AND source_id = $4",
                                  ['text', 'bigint', 'bigint', 'bigint']))
resource_ins = SD.setdefault('resource_ins',
                             prep("INSERT INTO resources"
                                  " (resource_id, user_id, project_id,"
                                  " source_id)"
                                  " VALUES ($1, $2, $3, $4) RETURNING id",
                                  ['text', 'bigint', 'bigint', 'bigint']))
resource_id = upsert(resource_sel, resource_ins, 'id',
                     [data['resource_id'], user_id, project_id, source_id])

meter_sel = SD.setdefault('meter_sel',
                          prep("SELECT id FROM meters WHERE "
                               "name = $1 AND type = $2 AND unit = $3",
                               ['text', 'text', 'text']))
meter_ins = SD.setdefault('meter_ins',
                          prep("INSERT INTO meters (name, type, unit)"
                               " VALUES ($1, $2, $3) RETURNING id",
                               ['text', 'text', 'text']))
meter_id = upsert(meter_sel, meter_ins, 'id', [data['counter_name'],
                                               data['counter_type'],
                                               data['counter_unit']])

sample_ins = SD.setdefault('sample_ins',
                           prep("INSERT INTO samples (user_id, project_id,"
                                " resource_id, meter_id, source_id, timestamp,"
                                " message_id, message_signature, recorded_at,"
                                " volume, metadata) VALUES ($1, $2, $3, $4,"
                                " $5, $6, $7, $8, now(), $9, $10)",
                                ['bigint', 'bigint', 'bigint', 'bigint',
                                 'bigint', 'timestamp', 'text', 'text',
                                 'double precision', 'jsonb']))
plpy.execute(sample_ins,
             [user_id, project_id, resource_id, meter_id, source_id,
              data['timestamp'], data['message_id'], data['message_signature'],
              data['counter_volume'], json.dumps(data['resource_metadata'])])

# aggregation \/ \/ \/ \/ \/
counter_type = data['counter_type']
if counter_type not in ['gauge', 'cumulative']:
    return

if data['counter_name'] == 'memory' and \
   'state' in data['resource_metadata'] and \
   data['resource_metadata']['state'] not in ['active', 'resized', 'paused']:
    return

if data['project_id'] is None or data['project_id'] == 'None':
    return

obj_id = data['resource_id']
owner = ''
volume = data['counter_volume']
meter_name = data['counter_name'].replace('.', '-')
# ASK: image_ref isn't always in sample metadata
if meter_name.startswith('instance') and 'image_ref' in data['resource_metadata']:
    meter_name += imageSuffix(data['resource_metadata']['image_ref'])
elif meter_name.startswith('volume'):
    meter_name += volumeSuffix(data['resource_metadata']['volume_type'])
elif meter_name in ['cpu', 'memory']:  # subcounters
    owner = obj_id
    obj_id = obj_id + ':' + meter_name
    if meter_name == 'cpu':
        volume /= 1000000000.0  # nanoseconds
elif meter_name == 'image-size' and \
        'properties' in data['resource_metadata'] and \
        'instance_uuid' in data['resource_metadata']['properties']:
    owner = data['resource_metadata']['properties']['instance_uuid']
    if '_backup_' in data['resource_metadata']['properties']['name']:
        meter_name = 'backup-size'

if 'event_type' in data['resource_metadata'] and \
        'create' in data['resource_metadata']['event_type']:
    event_type = 'start'
elif 'event_type' in data['resource_metadata'] and \
        'delete' in data['resource_metadata']['event_type']:
    event_type = 'stop'
else:
    event_type = 'exists'

timestamp = parser.parse(data['timestamp']).replace(microsecond=0)
tenant_key = data['project_id']

prebill_selfu = SD.setdefault('prebill_selfu',
                              prep("SELECT seq, volume, last_value,"
                                   " last_timestamp FROM prebill"
                                   " WHERE updating = 0 AND"
                                   " event_type = 'exists' AND obj_key = $1"
                                   " AND class = $2 AND tenant_key = $3"
                                   " AND counter_type = $4 FOR UPDATE",
                                   ['text', 'text', 'text', 'text']))

prebill_upd = SD.setdefault('prebill_upd',
                            prep("UPDATE prebill SET stamp_end = $1,"
                                 " volume = $2, last_value = $3,"
                                 " last_timestamp = $4 WHERE seq = $5",
                                 ['timestamp', 'float', 'float', 'timestamp',
                                  'bigint']))

prebill_ins = SD.setdefault('prebill_ins',
                            prep("INSERT INTO prebill (class, obj_key,"
                                 " event_type, stamp_start, stamp_end, volume,"
                                 " owner_key, tenant_key, counter_type,"
                                 " last_value, last_timestamp, updating) VALUES"
                                 " ($1, $2, $3, $4, $4, $5, $6, $7, $8, $9,"
                                 " $10, 0)",
                                 ['text', 'text', 'text', 'timestamp', 'float',
                                  'text', 'text', 'text', 'float',
                                  'timestamp']))
prebill_get = SD.setdefault('prebill_get',
                            prep("SELECT last_value, last_timestamp"
                                 " FROM prebill"
                                 " WHERE class = $1 AND"
                                 " obj_key = $2 AND tenant_key = $3 AND"
                                 " counter_type = $4 AND event_type = $5 AND"
                                 " owner_key = $6 AND updating != 0"
                                 " ORDER BY seq DESC LIMIT 1",
                                 ['text', 'text', 'text', 'text', 'text',
                                  'text']))


def get_prev_val():
    # Note (alexchadin): Get previous value and last timestamp
    # to calculate new value after previous row is collected (updating != 0).
    result = plpy.execute(prebill_get, [meter_name, obj_id, tenant_key,
                                        counter_type, event_type, owner])
    if result:
        last_value = result[0]['last_value']
        last_timestamp = result[0]['last_timestamp']
        seconds = (
            timestamp - parser.parse(last_timestamp)).total_seconds()
        return ((volume + last_value) / 2) * seconds
    return 0


def calcValue(data):
    last_value = data[0]['last_value']
    cur_volume = data[0]['volume']
    if counter_type == 'cumulative':
        value = cur_volume + volume
        if last_value <= volume:
            value -= last_value
    else:
        period = timestamp - parser.parse(data[0]['last_timestamp'])
        #seconds = period.seconds + period.days * 24 * 3600
        seconds = period.total_seconds()
        value = cur_volume + ((volume + last_value) / 2) * seconds
    return value


with plpy.subtransaction():
    result = plpy.execute(prebill_selfu,
                          [obj_id, meter_name, tenant_key, counter_type])
    if result:
        value = calcValue(result)
        plpy.execute(prebill_upd,
                     [timestamp, value, volume, timestamp, result[0]['seq']])
    else:
        try:
            value = volume if counter_type == 'cumulative' else get_prev_val()
            plpy.execute(prebill_ins,
                         [meter_name, obj_id, event_type, timestamp,
                          value, owner, tenant_key, counter_type, volume,
                          timestamp])
        except spiexceptions.UniqueViolation:
            result = plpy.execute(prebill_selfu,
                                  [obj_id, meter_name, tenant_key,
                                   counter_type])
            value = calcValue(result)
            plpy.execute(prebill_upd,
                         [timestamp, value, volume, timestamp, result[0]['seq']])

$$ language plpythonu;
