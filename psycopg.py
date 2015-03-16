import psycopg2
import json
conn = psycopg2.connect("dbname=ceilometer user=alexchadin")
cur = conn.cursor()
json_data = open('samples.json')
data = json.load(json_data)
for i in data:
    d = json.dumps(i)
    cur.execute("SELECT \"write_sample\"(%s);", (d,))
    conn.commit()

cur.close()
conn.close()
