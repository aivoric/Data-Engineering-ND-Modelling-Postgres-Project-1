import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Reads a song file which needs to be processed into a pandas dataframe.
    - Extracts the necessary song and artist data from the dataframe.
    - Prepares the data for the database and runs an insert.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].to_numpy().flatten().tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].to_numpy().flatten().tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Reads a log file which needs to be processed into a pandas dataframe.
    - Extracts the necessary time, user, and songplay data from the dataframe.
    - Queries song and artist data for additional songplay data.
    - Prepares the data for the database and runs an insert.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # save the timestamp series and add new column containing datetime
    timestamp = df['ts']
    df['ts_datetime']= pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    hour = df['ts_datetime'].dt.hour
    day = df['ts_datetime'].dt.day
    weekofyear = df['ts_datetime'].dt.isocalendar().week
    month = df['ts_datetime'].dt.month
    year = df['ts_datetime'].dt.year
    weekday = df['ts_datetime'].dt.dayofweek

    column_labels = ["timestamp", "hour", "day", "weekofyear", "month", "year", "weekday"]

    time_df = pd.DataFrame(list(zip(timestamp, hour, day, weekofyear, month, year, weekday)), columns = column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName","lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - Creates a list of files which need to be processed.
    - Iterates over each file and calls a different function to process the file.
    - Commits the processed data to the database.
    - Prints progress.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    - Established connection to database.
    - Calls the necessation functions to process song and log data.
    - Closes connection.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()