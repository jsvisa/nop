CHECK_TRACE_READY_TEMPLATE = r"""
SELECT
    distinct blknum AS blknum
FROM
    {chain}.traces
WHERE
    _st_day >= '{st_day}' AND _st_day <= '{et_day}'
    AND blknum >= {st_blknum} AND blknum <= {et_blknum}
    AND trace_type <> 'reward'
"""
