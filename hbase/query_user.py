import happybase

connection = happybase.Connection('localhost', port=9090)
table = connection.table('user_sessions')

print("Scanning for user_000042...")
for key, data in table.scan(row_prefix=b'user_000042'):
    print(key.decode('utf-8'), data)

connection.close()