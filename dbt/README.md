# dbt connecting to DuckDB

## Setup profiles.yml
```yml
duckdbdemo:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: '../demo.duckdb'
      extensions:
        - httpfs
        - parquet
```

## Run
```bash
dbt run
```