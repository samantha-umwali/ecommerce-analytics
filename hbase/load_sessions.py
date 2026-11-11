import happybase
import json
import time

connection = happybase.Connection('localhost', port=9090)
table = connection.table('user_sessions')

print("Loading sessions into HBase...")
with open('data/sessions_0.json', 'r') as f:
    sessions = json.load(f)

with table.batch(batch_size=1000) as b:
    for s in sessions:
        # Create a reverse timestamp for sorting (newest first)
        rev_ts = str(2**63 - 1 - int(time.time())) 
        row_key = f"{s['user_id']}_{rev_ts}".encode('utf-8')
        
        b.put(row_key, {
            b'meta:session_id': str(s['session_id']).encode('utf-8'),
            b'meta:start_time': str(s['start_time']).encode('utf-8'),
            b'activity:page_views': str(s.get('page_views', '')).encode('utf-8')
        })

print("Successfully loaded sessions into HBase.")
connection.close()