# Fabrix

Fabrix is a lib crate, who uses [Polars](https://github.com/pola-rs/polars) Series and DataFrame as fundamental data structures, and is capable to communicate among different data sources, such as Database (MySql/Postgres/Sqlite), File, JSON, MongoDB and etc. Furthermore, ETL process among different sources are provided as well, and additionally, manipulation or operation on data itself is enhanced.

There are three main parts in this crate:

- core: defines the fundamental data structures and provides the basic functions to manipulate them. `Value`, `Series`, `DataFrame` and `Row` represent unit data, 1D, 2D and cross-sectional data respectively.
- sources: defines the data sources, such as Sql, CSV, Excel, Parquet, JSON, MongoDB etc.
- dispatcher: a compositional data source dispatcher, which is capable to dispatch data from one source to another. Additionally, it can process data as a streaming pipeline.

## Features

- `default`: [`csv`, `parquet`, `json`]
- `sql`: Mysql, Postgres, Sqlite sources
- `xl`: Excel source
- `csv`: Csv source
- `parquet`: Parquet source
- `json`: Json source
- `mongo`: MongoDB source
- `dync`: dynamic connections management

## Structure

```txt
├── fabrix-core                        // Core package
│   │
│   ├── value.rs                       // the smallest data unit
│   ├── series.rs                      // series of value
│   ├── fabrix.rs                      // dataframe holder
│   ├── row.rs                         // row-wise data structure
│   ├── namedrow.rs                    // row-wise with name
│   ├── schema.rs                      // dataframe schema
│   ├── util.rs                        // utility functions
│   ├── error.rs                       // error handling for core
│   └── macros.rs                      // macros for core
│
├── fabrix-sql                         // SQL package
│   │
│   ├── builder                        // SQL builder
│   │   ├── sql_adt.rs                 // algebraic data type
│   │   ├── bd.rs                      // builder functions
│   │   ├── interface.rs               // SQL builder & ddl/dml logic interface
│   │   ├── query_ddl.rs               // ddl query: check table or schema
│   │   ├── query_dml.rs               // dml query: select and etc
│   │   ├── mutation_ddl.rs            // ddl mutation: create/alter/drop table
│   │   ├── mutation_dml.rs            // dml mutation: insert/update/delete data
│   │   └── macros.rs                  // Macros for SQL builder
│   │
│   ├── executor                       // SQL executor
│   │   ├── types.rs                   // Conversion between Sql data type and Fabrix `Value`
│   │   ├── processor.rs               // Sql row process, turn raw sql row into `Vec<Value>` or `Row`
│   │   ├── loader.rs                  // Database loader, CRUD logic implementation
│   │   ├── ec.rs                      // Sql executor, business logic implementation
│   │   └── macros.rs                  // Macros for sql executor
│   │
│   └── error.rs                       // Sql error
│
├── fabrix-json                        // Json package
│
├── fabrix-csv                         // Csv package
│
├── fabrix-parquet                     // Parquet package
│
├── fabrix-xl                          // Excel package
│   │
│   ├── util.rs                        // Excel utility
│   ├── worksheet.rs                   // Excel worksheet
│   ├── workbook.rs                    // Excel workbook
│   ├── executor.rs                    // Excel executor, business logic implementation
│   └── error.rs                       // Xl error
│
├── fabrix-mg-derive                   // MongoDB procedural macro
│   │
│   ├── dr.rs                          // derive macro
│   └── indexes.rs                     // mongo indexes methods
│
├── fabrix-mg                          // MongoDB package
│   │
│   ├── bd.rs                          // BSON conversion and util
│   ├── dr.rs                          // MongoDB traits for derived macro
│   ├── indexes.rs                     // MongoDB index
│   ├── ec.rs                          // MongoDB executor
│   └── error.rs                       // MongoError
│
├── fabrix-dyn-conn                    // Dynamic connection package for Database & MongoDB
│   │
│   ├── sql.rs                         // DynConn for Sql
│   └── error.rs                       // DynConn error
│
├── fabrix                             // Fabrix
│   │
│   ├── sources                        // Fabrix source
│   │   │
│   │   ├── sql                        // Sql data source
│   │   │   ├── reader.rs              // Sql reader
│   │   │   └── writer.rs              // Sql writer
│   │   │
│   │   ├── xl                         // Excel data source
│   │   │   └── reader.rs              // Excel reader
│   │   │
│   │   ├── csv                        // CSV data source
│   │   │   ├── reader.rs              // CSV reader
│   │   │   └── writer.rs              // CSV writer
│   │   │
│   │   ├── parquet                    // Parquet data source
│   │   │   ├── reader.rs              // Parquet reader
│   │   │   └── writer.rs              // Parquet writer
│   │   │
│   │   ├── json                       // JSON data source
│   │   │   ├── reader.rs              // JSON reader
│   │   │   └── writer.rs              // JSON writer
│   │   │
│   │   └── mongo                      // MongoDB data source
│   │       ├── reader.rs              // MongoDB reader
│   │       └── writer.rs              // MongoDB writer
│   │
│   ├── dispatcher                     // dispatcher for different data source
│   │   │
│   │   ├── ds.rs                      // dispatcher and source traits
│   │   ├── xl_db.rs                   // Excel -> Database
│   │   └── xl_json.rs                 // Excel -> JSON
│   │
│   ├── errors.rs                      // error handling
│   ├── prelude.rs                     // prelude of this crate
│   └── lib.rs
│
└── LICENSE
```

## Test Cases

1. [Declare DataFrame](./fabrix-core/tests/declare_df_test.rs)
1. [Sql executor CRUD](./fabrix-sql/tests/sql_executor_test.rs)
1. [Read Excel file and write to database](./fabrix/tests/read_xl_to_db_test.rs)
1. [Dispatcher test - from one source to another](./fabrix/tests/dispatcher_tests.rs)

## Examples

1. [File process service](./examples/file_process_service/src/main.rs):

   - a simple web server who accepts multiple types of file and turn their data into json format.

1. [Dispatcher service](./examples/dispatcher_service/src/main.rs):

   - upload csv file, read data and save into database. [code](./examples/dispatcher_service/src/csv2db.rs)

   - upload csv file, read data and response by json format. [code](./examples/dispatcher_service/src/csv2json.rs)

   - upload excel file, read data and save into database. [code](./examples/dispatcher_service/src/xl2db.rs)

   - upload excel file, read data and response by json format. [code](./examples/dispatcher_service/src/xl2json.rs)

   - select data from database and download a csv file. [code](./examples/dispatcher_service/src/db2csv.rs)

   - select data from database and download a parquet file. [code](./examples/dispatcher_service/src/db2parquet.rs)

   - upload parquet file, read data and save into MongoDB. [code](./examples/dispatcher_service/src/parquet2mongo.rs)

   - select data from MongoDB and download a csv file. [code](./examples/dispatcher_service/src/mongo2csv.rs)

   - select data from MongoDB and download a parquet file. [code](./examples/dispatcher_service/src/mongo2parquet.rs)

1. [Dynamic connection service](./examples/dync_service/src/main.rs):

   - a dynamic connection pool service for database.

## Todo

- `fabrix` sources: csv/json needs new de/se impl for their read/write
- `fabrix-csv`: new package
- `fabrix-parquet`: new package (parquet2)
- `fabrix-dyn-conn`: add initialization methods (serde_yaml)
- core: series/df `apply` method
- sources: from remote (use `reqwest` crate)
