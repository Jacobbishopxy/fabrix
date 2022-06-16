# Examples

## file_process_service

check [main.rs](./file_process_service/src/main.rs)

## dispatch_service

check [main.rs](./dispatch_service/src/main.rs)

- [**POST**] `http://localhost:8080/api/csv/to_json`: upload a csv file (by form-data), and return in json format

- [**POST**] `http://localhost:8080/api/csv/to_db`: upload a csv file, and save the data into a database table

- [**POST**] `http://localhost:8080/api/xl/to_json`: upload a xl file, and return in json format

- [**POST**] `http://localhost:8080/api/xl/to_db`: upload a xl file, and save the data into a database table

- [**GET**] `http://localhost:8080/api/db/show_tables`: show all the tables' name

- [**GET**] `http://localhost:8080/api/db/show_table_schema?table_name=${name}`: show table's schema

- [**POST**] `http://localhost:8080/api/db/to_csv`: download a csv file by selecting data from a database table. use `fabrix::sql_adt::Select` as json body, see below:

  ```json
  {
   "table": "test",
   "columns": [
    {
     "name": "id"
    },
    {
     "name": "first_name"
    },
    {
     "name": "last_name"
    },
    {
     "name": "ip_address"
    },
    {
     "name": "issued_date"
    },
    {
     "name": "issued_times"
    }
   ],
   "filter": [
    "not",
    {
     "column": "id",
     "<": "10"
    },
    "and",
    [
     {
      "column": "issued_times",
      "between": ["2", "6"]
     },
     "or",
     {
      "column": "last_name",
      "%": "Mayzes%"
     }
    ]
   ],
   "limit": 10
  }
  ```

- [**POST**] `http://localhost:8080/api/db/to_parquet`: download a parquet file by selecting data from a table. json body see above.

## dync_service

check [main.rs](./dync_service/src/main.rs)
