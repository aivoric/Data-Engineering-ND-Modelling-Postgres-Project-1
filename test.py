"""
Tests to check that the tables have been created successfully
and that records have been inserted into them.
"""

import psycopg2
from sql_queries import select_test_records, select_not_null_artist_and_song_ids

conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
cur = conn.cursor() 

cur.execute(select_test_records.format("songs", 5));
print("\n#################################### \nSONGS TABLE:\n####################################\n")
print('\n'.join(map(str, cur.fetchall())))

cur.execute(select_test_records.format("artists", 5));
print("\n#################################### \nARTISTS TABLE:\n####################################\n")
print('\n'.join(map(str, cur.fetchall())))

cur.execute(select_test_records.format("users", 5));
print("\n#################################### \nUSERS TABLE:\n####################################\n")
print('\n'.join(map(str, cur.fetchall())))

cur.execute(select_test_records.format("time", 5));
print("\n#################################### \nTIME TABLE:\n####################################\n")
print('\n'.join(map(str, cur.fetchall())))

cur.execute(select_test_records.format("songplays", 5));
print("\n#################################### \nSONGPLAYS TABLE:\n####################################\n")
print('\n'.join(map(str, cur.fetchall())))

cur.execute(select_not_null_artist_and_song_ids);
print("\n#################################### \nNOT NULL ARTIST & SONG IDS:\n####################################\n")
print('\n'.join(map(str, cur.fetchall())))

conn.close()