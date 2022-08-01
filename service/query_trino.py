from pyhive import trino

conn = trino.Connection(host="localhost", port='8098', catalog='hive', schema='default', protocol='http')
cur = conn.cursor()

cur.execute('SELECT _id FROM coauthor LIMIT 10')
rows = cur.fetchall()
