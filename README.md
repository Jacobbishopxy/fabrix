# Fabrix

Fabrix is a lib crate, who uses [Polars](https://github.com/pola-rs/polars) Series and DataFrame as fundamental data structures, and is capable to communicate among different data sources, such as Database (MySql/Postgres/Sqlite), File, BSON/JSON and etc. Furthermore, ETL process among different sources are provided as well, and additionally, manipulation or operation on data itself is enhanced.

There are three main parts in this crate:

- core: defines the fundamental data structures and provides the basic functions to manipulate them. `Value`, `Series`, `DataFrame` and `row` represent unit data, 1D, 2D and cross-sectional data respectively.
- sources: defines the data sources, such as Sql, File, BSON/JSON, etc.
- dispatcher: a compositional data source dispatcher, which is capable to dispatch data from one source to another. Additionally, it can process data as a streaming pipeline.

## Structure

```txt
├── core
│   ├── value.rs                        // the smallest data unit
│   ├── series.rs                       // series of value
│   ├── dataframe.rs                    // collection of series, with index series
│   ├── row.rs                          // row-wise data structure
│   ├── util.rs                         // utility functions
│   ├── error.rs
│   └── macros.rs
│
├── sources
│   │
│   ├── sql
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
│   ├── xl                              // Excel
│   │   ├── util.rs                     // Excel utility
│   │   ├── worksheet.rs                // Excel worksheet
│   │   ├── workbook.rs                 // Excel workbook
│   │   └── executor.rs                 // Excel executor, business logic implementation
│   │
│   ├── csv                             // CSV
│   │
│   ├── parquet                         // Parquet
│   │
│   ├── bson                            // BSON & JSON
│   │
│   └── mgo                             // MongoDB
│
├── dispatcher                          // dispatcher for different data source
│   ├── xl_source.rs                    // Excel as stream source
│   └── db_source.rs                    // DB as stream source
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
