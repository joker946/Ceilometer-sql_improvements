import plpy
jdata = ''
SD = {}
#CREATE OR REPLACE FUNCTION write_sample (jdata text) RETURNS void AS $$
    import json
    from plpy import spiexceptions
    from plpy import prepare as prep

    data = json.loads(jdata)


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

    source_sel = SD.setdefault('source_sel',
                               prep("SELECT id FROM sources WHERE name = $1",
                                    ['text']))
    source_ins = SD.setdefault('source_ins',
                               prep("INSERT INTO sources (name) VALUES ($1)"
                                    " RETURNING id", ['text']))
    source_id = upsert(source_sel, source_ins, 'id', [data['source']])

    user_sel = SD.setdefault('user_sel',
                             prep("SELECT id FROM users WHERE"
                                  " uuid = $1 AND source_id = $2",
                                  ['uuid', 'bigint']))
    user_ins = SD.setdefault('user_ins',
                             prep("INSERT INTO users (uuid, source_id)"
                                  " VALUES ($1, $2) RETURNING id",
                                  ['uuid', 'bigint']))
    user_id = upsert(user_sel, user_ins, 'id', [data['user_id'], source_id])

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
#$$ language plpythonu;