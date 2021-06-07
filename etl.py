import os
import pathlib
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_bulk_data(table_name, csv_path, cur):
    """
    - Create a temporary table
    - Upload CSV data to it
    - Insert data into table_name from the temporary table while ensuring primary key limitations
    - Drop the temporary table
    """
    cur.execute(create_temp_table.format(table_name))
    cur.execute(copy_to_temp_table.format(csv_path))
    cur.execute(insert_into_temp_table.format(table_name))
    cur.execute(drop_temp_table)
    
def process_bulk_songplay_data(csv_path, cur):
    """
    - Create a temporary table
    - Uploading CSV data to it but without song_id and artist_id
    - Insert data into the songplay table while also fetching artist_id and song_id directly inside SQL
    - Drop the temporary table
    """
    cur.execute(create_temp_songplay_table)
    cur.execute(copy_to_temp_songplay_table.format(csv_path))
    cur.execute(insert_into_temp_songplay_data)
    cur.execute(drop_temp_songplay_table)


def process_song_file(cur, filepath, conn):
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

def process_log_file(cur, filepath, conn):
    """
    - Reads a log file which needs to be processed into a pandas dataframe.
    - Extracts the necessary time, user, and songplay data from the dataframe.
    - Queries song and artist data for additional songplay data.
    - Prepares the data for the database and runs an insert.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)
    
    # create path for a temporary CSV which is used for bulk data uploading
    csv_path = os.path.join(pathlib.Path(__file__).parent.absolute(), "temp_csv.csv")

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # save the timestamp series and add new column containing datetime
    timestamp = df['ts']
    df['ts_datetime']= pd.to_datetime(df['ts'], unit='ms')
    
    # extract each time unit from datetime object
    hour = df['ts_datetime'].dt.hour
    day = df['ts_datetime'].dt.day
    weekofyear = df['ts_datetime'].dt.isocalendar().week
    month = df['ts_datetime'].dt.month
    year = df['ts_datetime'].dt.year
    weekday = df['ts_datetime'].dt.dayofweek

    # save time data into csv and then run process_bulk_data to upload it to the database
    column_labels = ["timestamp", "hour", "day", "weekofyear", "month", "year", "weekday"]
    time_df = pd.DataFrame(
        list(zip(timestamp, hour, day, weekofyear, month, year, weekday)), 
        columns = column_labels
        )
    time_df.to_csv(csv_path, index=False, header=False)
    process_bulk_data("time", csv_path, cur)
    os.remove(csv_path)

    # extract user data and then run process_bulk_data to upload it to the database
    user_df = df[["userId", "firstName","lastName", "gender", "level"]]    
    user_df.to_csv(csv_path, index=False, header=False)
    process_bulk_data("users", csv_path, cur)
    os.remove(csv_path)
    
    # extract songplay data and then run process_bulk_songplay_data
    # note: songplay data has a special function because it requires custom SQL
    # in order to merge additional data into it (artist_id and song_id)
    songplay_df = df[["ts", "userId","level", "sessionId", "location", "userAgent", "song", "artist", "length"]]
    songplay_df.to_csv(csv_path, index=False, header=False)
    process_bulk_songplay_data(csv_path, cur)
    os.remove(csv_path)

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
        func(cur, datafile, conn)
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
