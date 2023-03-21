# flake8: noqa
READ_TRACE_TEMPLATE = r"""
WITH abi AS (
    SELECT DISTINCT
        address,
        abi,
        byte_sign,
        split_part(text_sign, '(', 1) AS method
    FROM
        "ethereum".func_signatures
    WHERE
        address = '0x2b2e8cda09bba9660dca5cb6233787738ad68329' -- sudoswap Pair Router
        AND text_sign IN (
            'swapETHForSpecificNFTs((address,uint256[])[],address,address,uint256)', -- 0x11132000
            'swapNFTsForToken((address,uint256[])[],uint256,address,uint256)', -- 0xdabf67d7
            'robustSwapETHForSpecificNFTs(((address,uint256[]),uint256)[],address,address,uint256)',  -- 0x3efd9e71
            'robustSwapNFTsForToken(((address,uint256[]),uint256)[],address,uint256)', -- 0x2b997f8e
            'robustSwapETHForSpecificNFTsAndNFTsToToken((((address,uint256[]),uint256)[],((address,uint256[]),uint256)[],uint256,address,address))' -- 0xab5c0da2
        )
),
traces AS (
    SELECT
        *,
        substring(input from 1 for 10) AS func_sign
    FROM
        "ethereum".traces
    WHERE
        _st_day >= '{st_day}' AND _st_day <= '{et_day}'
        AND blknum >= {st_blknum} AND blknum <= {et_blknum}
        AND to_address = '{sudoswap_contract_address}' -- sudoswap Pair Router
        AND length(input) > 10
        AND status = 1
)

SELECT
    a._st,
    a._st_day::text,
    a.blknum,
    a.txhash,
    a.txpos,
    a.from_address AS taker,
    a.to_address AS maker,
    a.value,
    a.output,
    a.trace_address,
    b.method AS pattern,
    eth_decode_input2(b.abi::json, input)::json->'parameter' AS _out
FROM
    traces a
INNER JOIN abi b ON
    a.func_sign = b.byte_sign
"""
