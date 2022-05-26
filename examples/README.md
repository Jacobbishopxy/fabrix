# Examples

## xl_process_service

check [main.rs](./xl_process_service/src/main.rs)

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
  		["id", "index_id"],
  		"first_name",
  		"last_name",
  		"ip_address",
  		"issued_date",
  		"issued_times"
  	],
  	"limit": 10
  }
  ```

- [**POST**] `http://localhost:8080/api/db/to_parquet`: download a parquet file by selecting data from a table. json body see above.
