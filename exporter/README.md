# Export from DWH to Duck DB

## Snowflake
- Configure Snowflake source in dbt profiles.yml
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
  exporter:
    snowflake:
      type: snowflake
      account: xxx
      user: DQ_TOOLS_USER
      password: xxx
      role: ROLE_TRANSFORM_DQ_TOOLS
      database: DQ_TOOLS
      warehouse: BI_CT_1_WH
      schema: dq_tools_integration_tests_dat
      threads: 10
```
- Run exporter
```bash
python ./exporter/export_snowflake.py BI_DQ_METRICS
```
- Check db file created named `streamlit/duckdbdemo.duckdb`

# View data in Duck DB
Download [Tad](https://www.tadviewer.com/) and open `.duckdb` file to view data