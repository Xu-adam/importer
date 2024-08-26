import os
import pandas as pd
import pyodbc
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from queue import Queue
import gc
import psutil

### function definition

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
    # ask for the configuration
    server = input("Please enter the server name: ")
    database = input("Please enter the database name: ")
    table = input("please enter the table name: ")
    username = input("Please enter the username(input sa): ")
    password = input("Please enter the password for sa: ")
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};MARS_Connection=Yes;MaxPoolSize=10'

    # get all the csv files in the current directory
    script_directory = os.getcwd()
    files = [os.path.join(script_directory, f) for f in os.listdir(script_directory) if f.endswith('.csv') and os.path.isfile(os.path.join(script_directory, f))]
    print(f"{len(files)} target csv files found in the current directory.")

    # multi-threading setup and database connection
    max_workers = max_connections = batch_size = 5
    split_size = 50000
    connection_queue = Queue(maxsize=max_connections)

    for _ in range(max_connections):
        conn = pyodbc.connect(connection_string)
        connection_queue.put(conn)
    print('Database connection established.')

    # iterate over all the csv files to import
    for input_file in files:
        # split the csv file into chunks
        start_time_for_split = time.time()
        print(f"Reading and Splitting {input_file} into chunks...")
        chunk_iter = pd.read_csv(input_file, chunksize=split_size, encoding='utf-8', low_memory=False, keep_default_na=False)
        rest_chunk_num = chunk_num = sum(1 for _ in chunk_iter)
        chunk_iter = pd.read_csv(input_file, chunksize=split_size, encoding='utf-8', low_memory=False, keep_default_na=False)
        print(f"Read and Split {input_file} into {chunk_num} chunks in {(time.time() - start_time_for_split) / 60:.2f} minutes.")
        del start_time_for_split
        total_batch_num = chunk_num / batch_size
        check_memory()

        # start timer
        print(f"Start importing {input_file} to the database ...")
        total_start_time = time.time()
        # begin multi-threading
        submit_size = chunk_num // 5
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # use a dictionary to store the future objects
            futures = submit_next_percentage(executor, chunk_iter, submit_size, connection_queue, table)
            rest_chunk_num -= submit_size
            check_memory()
            completed_files = 0
            batch_number = 1
            start_time_for_batch = time.time()

            while completed_files < chunk_num:
                for future in as_completed(futures):
                    del futures[future]
                    completed_files += 1
                    if completed_files % batch_size == 0:
                        print(f"Batch {batch_number}/{total_batch_num}: {(time.time() - start_time_for_batch) / 60:.2f} minutes.")
                        start_time_for_batch = time.time()
                        batch_number += 1
                    # check every 20% of the chunks
                    if completed_files % submit_size  == 0:
                        clean()
                        submit_size = min(submit_size, rest_chunk_num)
                        if submit_size > 0:
                            new_futures = submit_next_percentage(executor, chunk_iter, submit_size, connection_queue, table)
                            rest_chunk_num -= submit_size
                            futures.update(new_futures)
                            check_memory()
                        else:
                            break


        print(f"Data import completed in {(time.time() - total_start_time) / 60:.2f} minutes.")
        print(f"{input_file} has been imported to the database.")
        print("Cleaning up and moving on to the next file...")
        clean()

    while not connection_queue.empty():
        conn = connection_queue.get()
        conn.close()
    clean()
    print('All files imported successfully. Database connection closed.')

if __name__ == "__main__":
    main()