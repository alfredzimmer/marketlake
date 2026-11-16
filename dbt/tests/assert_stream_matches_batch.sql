-- Value mismatches mean the stream and batch disagree on the same trades: a
-- bug. Count mismatches (late trades past the watermark) only warn.
{{ config(severity='error', error_if='>0', warn_if='>0') }}
select * from {{ ref('rpt_bar_reconciliation') }}
where status in ('value_mismatch', 'missing_in_batch')
