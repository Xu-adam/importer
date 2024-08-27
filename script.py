import os
import pandas as pd
import pyodbc
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from queue import Queue
import gc
import psutil
import math

### function definitions ###

MEMORY_THRESHOLD = 5 * 1024 * 1024 * 1024 # 5GB
def check_memory():
    available_memory = psutil.virtual_memory().available
    print(f"Current memory available: {available_memory / (1024 ** 3):.2f} GB")
    if available_memory < MEMORY_THRESHOLD:
        print("Warning: low memory detected. Running garbage collection...")
        clean()

def clean():
    gc.collect()

# insert data to database
def insert_csv_to_db(chunk, connection_queue, table_name):
    try:
        conn = connection_queue.get()
        cursor = conn.cursor()
        cursor.fast_executemany = True
        cols = ','.join(chunk.columns)
        placeholders = ','.join(['?' for _ in chunk.columns])
        sql_insert = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
        cursor.executemany(sql_insert, chunk.values.tolist())
        conn.commit()
        connection_queue.put(conn)
    except Exception as e:
        print(f"Error inserting data into {table_name}: {e}")
        if 'conn' in locals():
            connection_queue.put(conn)

# submit the next percentage of the chunks to the thread pool
def submit_next_percentage(executor, chunk_iter, submit_size, connection_queue, table):
    futures = {}
    try:
        for _ in range(submit_size):
            chunk = next(chunk_iter)
            futures[executor.submit(insert_csv_to_db, chunk, connection_queue, table)] = chunk
    except StopIteration:
        pass
    return futures

###########################################################

def main():
    # ask for the database configuration
    server = input("Please enter the server name: ")
    database = input("Please enter the database name: ")
    table = input("please enter the table name: ")
    username = input("Please enter the username(input sa): ")
    password = input("Please enter the password for sa: ")
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};MARS_Connection=Yes;MaxPoolSize=10'

    # get the csv files to import in the current directory
    script_directory = os.getcwd()
    files = [os.path.join(script_directory, f) for f in os.listdir(script_directory) if f.endswith('.csv') and os.path.isfile(os.path.join(script_directory, f))]
    print(f"{len(files)} target csv files found in the current directory.")

    # multi-threading setup and database connection
    max_workers = max_connections = 5
    split_size = 50000
    connection_queue = Queue(maxsize=max_connections)

    for _ in range(max_connections):
        conn = pyodbc.connect(connection_string)
        connection_queue.put(conn)
    print('Database connection established.')

    # iterate over all the csv files to import
    for input_file in files:
        # read and split the csv file into chunks
        start_time_for_split = time.time()
        print(f"Reading and Splitting {input_file} into chunks...")
        chunk_iter = pd.read_csv(input_file, chunksize=split_size, encoding='utf-8', low_memory=False, keep_default_na=False)
        rest_chunk_num = chunk_num = sum(1 for _ in chunk_iter)
        chunk_iter = pd.read_csv(input_file, chunksize=split_size, encoding='utf-8', low_memory=False, keep_default_na=False)
        print(f"Read and Split {input_file} into {chunk_num} chunks in {(time.time() - start_time_for_split) / 60:.2f} minutes.")
        del start_time_for_split
        check_memory()

        # start timer
        total_start_time = time.time()
        print(f"Start importing {input_file} to the database ...")
        # submit 20% at a time to save memory
        submit_size = math.ceil(chunk_num / 5)
        percent_checkpoint = math.ceil(chunk_num / 10)

        # begin multi-threading
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # use a dictionary to store the future objects
            futures = submit_next_percentage(executor, chunk_iter, submit_size, connection_queue, table)
            rest_chunk_num -= submit_size
            check_memory()
            completed_files = 0
            completed_percentage = 0

            # make sure that all the chunks are processed
            while completed_files < chunk_num:
                for future in as_completed(futures):
                    del futures[future]
                    completed_files += 1

                    if completed_files % percent_checkpoint == 0:
                        completed_percentage += 10
                        print(f"{completed_percentage}% completed in {(time.time() - total_start_time) / 60:.2f} minutes.")

                    # check every 20% of the chunks
                    if completed_files % submit_size  == 0:
                        clean() # free memory of the previous 20%
                        submit_size = min(submit_size, rest_chunk_num)
                        if submit_size > 0:
                            # submit the next 20% (or the rest)
                            new_futures = submit_next_percentage(executor, chunk_iter, submit_size, connection_queue, table)
                            rest_chunk_num -= submit_size
                            futures.update(new_futures)
                            check_memory()
                        else:
                            # no more chunks to submit
                            break

        print(f"Data import completed in {(time.time() - total_start_time) / 60:.2f} minutes.")
        print(f"{input_file} has been imported to the database.")
        print("Cleaning up and moving on to the next file...")
        clean()

    # all files have been processed, close the database connection
    while not connection_queue.empty():
        conn = connection_queue.get()
        conn.close()
    clean()
    print('All files imported successfully. Database connection closed.')

if __name__ == "__main__":
    main()