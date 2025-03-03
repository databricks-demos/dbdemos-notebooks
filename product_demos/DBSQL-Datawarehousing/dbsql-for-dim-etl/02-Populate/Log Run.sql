-- Databricks notebook source
declare or replace variable op_sql string;

-- COMMAND ----------

-- to obtain the results of the previous DML
set variable op_sql = "
  create or replace temporary view metrics_tv
  as
  select coalesce(operationMetrics.numTargetRowsInserted, operationMetrics.numOutputRows) as num_inserted, operationMetrics.numTargetRowsUpdated as num_updated from (describe history " || session.load_table || ") where operation in ('MERGE', 'WRITE', 'COPY INTO') limit 1
";

-- COMMAND ----------

-- create metrics view
execute immediate op_sql;

-- COMMAND ----------

merge into identifier(session.run_log_table) as t
using (select * from values (session.data_source, session.load_table, session.load_start_time, session.load_end_time, session.process_id) as
  source(data_source, table_name, load_start_time, load_end_time, process_id))
on (t.data_source = source.data_source and t.table_name = source.table_name and t.load_start_time = source.load_start_time)
when matched then update set t.load_end_time = source.load_end_time, t.num_inserts = (select num_inserted from metrics_tv), t.num_updates = (select num_updated from metrics_tv)
when not matched then insert (data_source, table_name, load_start_time, process_id) values (source.data_source, source.table_name, source.load_start_time, source.process_id)
;