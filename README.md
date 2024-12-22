# pyspark_multiple_db_query

The goal of this project is to provide a simple python script capable of run a query in multiple databases with the same structure and, with the result, run another query on top of that.

The main usage is to be able to calculate events distributed on multiple databases with differente data, but same structute, as a cluster.

This example works on postgres, but you can easily change it altering the driver provided with this distribution. It will be also necessary to change the RDMS and driver on the "main.py" script.

## Usage

You will have to alter three main files on this project in order to make it work for you:

### config.json

This file contains the database host, port, username and password for each one.

The basic structure is an object that can be replicated as many times as you need.

In the example provided with this project, we have three databases configured.

There is virtually no limit of databases number where you can run the query.
The file provided have the structure below:
```
    {
      "url": "jdbc:postgresql://ip_or_host_name_for_database_1:port/database",
      "user": "user",
      "password": "pass"
    }
```
You must keep only the amount of entries you are going to use.

_________________________

### first_query.sql

This file contains the query that will be executed on the databases defined in config.json file.
You will need to edit it respecting the RDMS sintax.
___________________
### second_query.sql

This file constains the query that will be executed on top of the results provided from the execution of the first query (**first_query.sql**) on all dabatases listed on **config.json**.
For default, the name of the **combined dataframe** resulted from the execution of the **first query** on all databases is named **combined_table** (you can change that on the main.py file).

Because of that, the "from" clause on the second_query file if set to **combined_table**.
You don't need to change that but keep in mind you cannot invoke a column that is not created on the execution of the first query.
The name of all columns will be preserved in most of the RDMS (some drivers can overlay this and rename them to "column1", "column2", etc).

It is important to note the results of the first query execution will be loaded on memory in order to the **second query** to be executed.

Thus, you machine will need to have enough memory (RAM) to load the results of the first query from all databases.

Because of that, keep in mind that is important to work with optimized execution on databases. On a very simple example, if you need to count the rows from all databases, it's better to run a count() on first query and then a sum() on second query than to load all rows on first query and then a count() on second query.

## Results

The results on the second query run will the on the file "results.csv" on the root directory of the project.
If there is already a file with this name, it will be overwritten. Be careful to move your resultd to another file or directory after each run.
If there is no file with this name, it will be created.
