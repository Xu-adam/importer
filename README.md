# High-Performance CSV-SQL Importer
## This script can provide an average import speed of `50000 rows/s`, with peaks of over `100000 rows/s`.
#### Utilizes `pandas` + `pyodbc` + `multi-threading` + `memory management`. The larger the data file to import, the better the script works compared with traditional import methods.
With a more powerful CPU/SSD, increasing the number of threads can further enhance the import speed.
## Features:
#### 1. `Optimized Multi-threading:` Provides a speed comparable to, and sometimes faster than built-in database methods through efficient multi-threading.
#### 2. `Memory Efficiency:` Consumes minimal memory due to optimization and management techniques, allowing smooth operation on 32(even 16)GB RAM systems when importing multiple 10 GB files consecutively, with a peak usage of less than 5 GB.
#### 3. `User-Friendly:` Easy to use—simply enter the database configurations, and the script will begin importing all the files in the same directory consecutively. It can also be easily automated for hands-free operation.
#### 4. `Improved Data Type Conversions:` Outperforms in handling implicit data type conversions, ensuring smoother imports.
#### 5. `Better Error Handling:` The import process is monitored; if an error occurs, it is easily traceable while the rest of the data continues to import successfully.

