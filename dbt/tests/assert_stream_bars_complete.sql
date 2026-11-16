-- Closed minutes that silver has but the stream never emitted. Causes seen so
-- far: trades older than the watermark when they arrived, and Iceberg skipping
-- epochs after a checkpoint's offsets were rolled back (the sink treats epoch
-- ids <= its last commit as already written). Warn: repair from batch.
{{ config(severity='warn') }}
select * from {{ ref('rpt_bar_reconciliation') }}
where status = 'missing_in_stream'
