from pyhive import presto

conn = presto.Connection(host="localhost", port='8080', username='user', catalog='hive', schema='default', protocol='http')
cur = conn.cursor()

cur.execute('SELECT `_id` FROM coauthor LIMIT 10')
rows = cur.fetchall()
