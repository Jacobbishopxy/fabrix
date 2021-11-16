# Fabrix

Fabrix is a lib crate, who uses [Polars](https://github.com/pola-rs/polars) Series and DataFrame as fundamental data structures, and is capable to communicate among different data sources, such as Database (MySql/Postgres/Sqlite), File, BSON/JSON and etc. Furthermore, ETL process among different sources are provided as well, and additionally, manipulation or operation on data itself is enhanced.

## Structure

```txt
├── core
│   ├── value.rs                        // the smallest data unit
│   ├── series.rs                       // series of value
│   ├── dataframe.rs                    // collection of series, with index series
│   ├── row.rs                          // row-wise data structure
│   ├── util.rs                         // utility functions
│   └── macros.rs
│
├── sources
│   ├── db
│   │   ├── sql_builder                 // SQL builder
│   │   │   ├── adt.rs                  // algebraic data type
│   │   │   ├── query_ddl.rs            // ddl query: check table or schema
│   │   │   ├── query_dml.rs            // dml query: select and etc
│   │   │   ├── mutation_ddl.rs         // ddl mutation: create/alter/drop table
│   │   │   ├── mutation_dml.rs         // dml mutation: insert/update/delete data
│   │   │   ├── interface.rs            // SQL builder & ddl/dml logic interface
│   │   │   ├── builder.rs              // SQL builder & ddl/dml logic implement
│   │   │   └── macros.rs
│   │   │
│   │   └── sql_executor
│   │       ├── types.rs                // Conversion between Sql data type and Fabrix `Value`
│   │       ├── processor.rs            // Sql row process, turn raw sql row into `Vec<Value>` or `Row`
│   │       ├── loader.rs               // Database loader, CRUD logic implementation
│   │       ├── executor.rs             // Sql executor, business logic implementation
│   │       └── macros.rs
│   │
│   ├── file
│   │   ├── xl                          // Excel
│   │   │   ├── util.rs
│   │   │   ├── worksheet.rs
│   │   │   ├── workbook.rs
│   │   │   └── executor.rs
│   │   │
│   │   ├── csv                         // CSV
│   │   │
│   │   └── parquet                     // Parquet
│   │
│   └── bson
│
├── errors.rs                           // error handling
│
├── macros.rs                           // helpful macros
│
├── prelude.rs                          // prelude of this crate
│
└── lib.rs
```

## Examples

under construction...

## Note

- Progression of `dataframe/core`: `value` -> `series` -> `dataframe` -> `row`
- Progression of `dataframe/sources`: `db` -> `file` -> `json`
  - `db`: `sql_builder` -> `sql_executor`
  - `file`: `xl` -> `csv` -> `parquet`
