import psycopg2
import json

json_data = open('input.json')
data = json.load(json_data)
data = json.dumps(data)

conn = psycopg2.connect("dbname=ceilometer user=alexchadin")
cur = conn.cursor()
cur.execute("SELECT \"write_sample\"(%s);", (data,))
conn.commit()
cur.close()
conn.close()