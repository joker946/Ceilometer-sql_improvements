import psycopg2


def make_list(resp):
    result = []
    for r in resp:
        result.append(r[0])
    return result
def get_users(source=None):
    conn = psycopg2.connect("dbname=ceilometer user=alexchadin")
    cur = conn.cursor()
    if source:
        cur.execute("SELECT uuid FROM users WHERE source_id=%s;", (source,))
    else:
        cur.execute("SELECT uuid FROM users;")
    resp = make_list(cur.fetchall())
    print resp
    cur.close()
    conn.close()

get_users()