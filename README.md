# High-Performance CSV-SQL Importer
## This script provides a `baseline` import speed of approximately `35,000 rows per second`. 
#### Utilizes `pandas` + `pyodbc` + `multi-threading` + `memory management`.
With a more powerful CPU, increasing the number of threads can further enhance the import speed.
## Features:
#### 1. `Optimized Multi-threading:` Provides a speed comparable to built-in database methods through efficient multi-threading.
#### 2. `Memory Efficiency:` Consumes minimal memory due to optimization and management techniques, allowing smooth operation on 32(even 16)GB RAM systems when importing multiple 10 GB files consecutively with a peak usage of less than 6 GB.
#### 3. `User-Friendly:` Easy to use—simply enter the database configurations, and the script will begin importing all the files in the same directory consecutively. It can also be easily automated for hands-free operation.
#### 4. `Improved Data Type Conversions:` Outperforms in handling implicit data type conversions, ensuring smoother imports.

