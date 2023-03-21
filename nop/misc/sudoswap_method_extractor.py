import pandas as pd
from eth_abi.abi import decode_single

PACK_GROUP_KEY = ["txhash", "trace_address"]
SUDOSWAP_COLUMNS = [
    "_st",
    "_st_day",
    "blknum",
    "txhash",
    "txpos",
    "maker",
    "taker",
    "from_address",
    "to_address",
    "price",
    "trace_address",
    "pattern",
    "pair",
    "token_id",
]


def _safe_explode(df: pd.DataFrame, column: str) -> pd.DataFrame:
    df = df.explode(column=column, ignore_index=True)
    df = df[pd.notna(df[column])]
    return df


def _extract_orderbook_swapETHForSpecificNFTs(xf: pd.DataFrame) -> pd.DataFrame:
    # {
    #   "inputs": [
    #     {
    #       "components": [
    #         {"name": "pair", "type": "address"},
    #         {"name": "nftIds", "type": "uint256[]"},
    #       ],
    #       "name": "swapList",
    #       "type": "tuple[]",
    #     },
    #     {"name": "ethRecipient", "type": "address"},
    #     {"name": "nftRecipient", "type": "address"},
    #     {"name": "deadline", "type": "uint256"},
    #   ],
    #   "name": "swapETHForSpecificNFTs",
    #   "outputs": [{"name": "remainingValue", "type": "uint256"}],
    #   "stateMutability": "payable",
    #   "type": "function",
    # }
    xf["price"] = xf.apply(lambda row: row["value"] - int(row["output"], 16), axis=1)
    xf["to_address"] = xf["_out"].apply(lambda x: x["nftRecipient"])
    xf["swapList"] = xf["_out"].apply(lambda x: x["swapList"])
    xf = _safe_explode(xf, "swapList")
    xf["pair"] = xf["swapList"].apply(lambda x: x["pair"])
    xf["from_address"] = xf["pair"]
    xf["token_id"] = xf["swapList"].apply(lambda x: x["nftIds"])
    xf = _safe_explode(xf, "token_id")

    return xf


def _extract_orderbook_swapNFTsForToken(xf: pd.DataFrame) -> pd.DataFrame:
    # {
    #   "inputs": [
    #     {
    #       "components": [
    #         {"name": "pair", "type": "address"},
    #         {"name": "nftIds", "type": "uint256[]"},
    #       ],
    #       "name": "swapList",
    #       "type": "tuple[]",
    #     },
    #     {"name": "minOutput", "type": "uint256"},
    #     {"name": "tokenRecipient", "type": "address"},
    #     {"name": "deadline", "type": "uint256"},
    #   ],
    #   "name": "swapNFTsForToken",
    #   "outputs": [{"name": "outputAmount", "type": "uint256"}],
    #   "stateMutability": "nonpayable",
    #   "type": "function",
    # }
    xf["price"] = xf.apply(lambda row: int(row["output"], 16), axis=1)
    xf["from_address"] = xf["_out"].apply(lambda x: x["tokenRecipient"])
    xf["swapList"] = xf["_out"].apply(lambda x: x["swapList"])
    xf = _safe_explode(xf, "swapList")
    xf["pair"] = xf["swapList"].apply(lambda x: x["pair"])
    xf["to_address"] = xf["pair"]
    xf["token_id"] = xf["swapList"].apply(lambda x: x["nftIds"])
    xf = _safe_explode(xf, "token_id")

    return xf


def _extract_orderbook_robustSwapETHForSpecificNFTs(xf: pd.DataFrame) -> pd.DataFrame:
    # {
    #   "inputs": [
    #     {
    #       "components": [
    #         {
    #           "components": [
    #             {"name": "pair", "type": "address"},
    #             {"name": "nftIds", "type": "uint256[]"},
    #           ],
    #           "name": "swapInfo",
    #           "type": "tuple",
    #         },
    #         {"name": "maxCost", "type": "uint256"},
    #       ],
    #       "name": "swapList",
    #       "type": "tuple[]",
    #     },
    #     {"name": "ethRecipient", "type": "address"},
    #     {"name": "nftRecipient", "type": "address"},
    #     {"name": "deadline", "type": "uint256"},
    #   ],
    #   "name": "robustSwapETHForSpecificNFTs",
    #   "outputs": [{"name": "remainingValue", "type": "uint256"}],
    #   "stateMutability": "payable",
    #   "type": "function",
    # }
    xf["price"] = xf.apply(lambda row: row["value"] - int(row["output"], 16), axis=1)
    xf["to_address"] = xf["_out"].apply(lambda x: x["nftRecipient"])
    xf["swapList"] = xf["_out"].apply(lambda x: x["swapList"])
    xf = _safe_explode(xf, "swapList")
    xf["pair"] = xf["swapList"].apply(lambda x: x["swapInfo"]["pair"])
    xf["from_address"] = xf["pair"]
    xf["token_id"] = xf["swapList"].apply(lambda x: x["swapInfo"]["nftIds"])
    xf = _safe_explode(xf, "token_id")

    return xf


def _extract_orderbook_robustSwapNFTsForToken(xf: pd.DataFrame) -> pd.DataFrame:
    # {
    #   "inputs": [
    #     {
    #       "components": [
    #         {
    #           "components": [
    #             {"name": "pair", "type": "address"},
    #             {"name": "nftIds", "type": "uint256[]"},
    #           ],
    #           "name": "swapInfo",
    #           "type": "tuple",
    #         },
    #         {"name": "minOutput", "type": "uint256"},
    #       ],
    #       "name": "swapList",
    #       "type": "tuple[]",
    #     },
    #     {"name": "tokenRecipient", "type": "address"},
    #     {"name": "deadline", "type": "uint256"},
    #   ],
    #   "name": "robustSwapNFTsForToken",
    #   "outputs": [{"name": "outputAmount", "type": "uint256"}],
    #   "stateMutability": "nonpayable",
    #   "type": "function",
    # }
    xf["price"] = xf.apply(lambda row: int(row["output"], 16), axis=1)
    xf["from_address"] = xf["_out"].apply(lambda x: x["tokenRecipient"])
    xf["swapList"] = xf["_out"].apply(lambda x: x["swapList"])
    xf = _safe_explode(xf, "swapList")
    xf["pair"] = xf["swapList"].apply(lambda x: x["swapInfo"]["pair"])
    xf["to_address"] = xf["pair"]
    xf["token_id"] = xf["swapList"].apply(lambda x: x["swapInfo"]["nftIds"])
    xf = _safe_explode(xf, "token_id")

    return xf


def _extract_orderbook_robustSwapETHForSpecificNFTsAndNFTsToToken(
    xf: pd.DataFrame,
) -> pd.DataFrame:
    # {
    #   "inputs": [
    #     {
    #       "components": [
    #         {
    #           "components": [
    #             {
    #               "components": [
    #                 {"name": "pair", "type": "address"},
    #                 {"name": "nftIds", "type": "uint256[]"},
    #               ],
    #               "name": "swapInfo",
    #               "type": "tuple",
    #             },
    #             {"name": "maxCost", "type": "uint256"},
    #           ],
    #           "name": "tokenToNFTTrades",
    #           "type": "tuple[]",
    #         },
    #         {
    #           "components": [
    #             {
    #               "components": [
    #                 {"name": "pair", "type": "address"},
    #                 {"name": "nftIds", "type": "uint256[]"},
    #               ],
    #               "name": "swapInfo",
    #               "type": "tuple",
    #             },
    #             {"name": "minOutput", "type": "uint256"},
    #           ],
    #           "name": "nftToTokenTrades",
    #           "type": "tuple[]",
    #         },
    #         {"name": "inputAmount", "type": "uint256"},
    #         {"name": "tokenRecipient", "type": "address"},
    #         {"name": "nftRecipient", "type": "address"},
    #       ],
    #       "name": "params",
    #       "type": "tuple",
    #     }
    #   ],
    #   "name": "robustSwapETHForSpecificNFTsAndNFTsToToken",
    #   "outputs": [
    #     {"name": "remainingValue", "type": "uint256"},
    #     {"name": "outputAmount", "type": "uint256"},
    #   ],
    #   "stateMutability": "payable",
    #   "type": "function",
    # }

    # Tips:
    # user maybe provider partial arguments, in this case eg:
    #   https://cn.etherscan.com/tx/0xb7fe3c4b0dab6965747addec53913c737831e83667f54515dc8ee4c8ea1cca78
    # this tx has BuyOrder, but the SellOrder were missing
    xf["output"] = xf["output"].apply(
        lambda x: decode_single("(uint256,uint256)", bytes(bytearray.fromhex(x[2:])))
    )
    xf["_out"] = xf["_out"].apply(lambda x: x["params"])
    # Buys NFTs with ETH
    bf = xf.copy()
    bf["price"] = bf.apply(lambda row: row["value"] - row["output"][0], axis=1)
    bf["to_address"] = bf["_out"].apply(lambda x: x["nftRecipient"])
    bf["swapList"] = bf["_out"].apply(lambda x: x["tokenToNFTTrades"])
    bf = _safe_explode(bf, "swapList")
    bf["pair"] = bf["swapList"].apply(lambda x: x["swapInfo"]["pair"])
    bf["from_address"] = bf["pair"]

    # sells NFTS for tokens
    sf = xf.copy()
    sf["price"] = sf.apply(lambda row: row["output"][1], axis=1)
    sf["from_address"] = sf["_out"].apply(lambda x: x["tokenRecipient"])
    sf["swapList"] = sf["_out"].apply(lambda x: x["nftToTokenTrades"])
    sf = _safe_explode(sf, "swapList")
    sf["pair"] = sf["swapList"].apply(lambda x: x["swapInfo"]["pair"])
    sf["to_address"] = sf["pair"]

    # merge buy and sell orders
    xf = pd.concat([bf, sf])
    xf["token_id"] = xf["swapList"].apply(lambda x: x["swapInfo"]["nftIds"])
    xf = _safe_explode(xf, "token_id")

    return xf


PATTERN_EXTRACTORS = {
    "swapETHForSpecificNFTs": _extract_orderbook_swapETHForSpecificNFTs,
    "robustSwapETHForSpecificNFTs": _extract_orderbook_robustSwapETHForSpecificNFTs,
    "swapNFTsForToken": _extract_orderbook_swapNFTsForToken,
    "robustSwapETHForSpecificNFTsAndNFTsToToken": _extract_orderbook_robustSwapETHForSpecificNFTsAndNFTsToToken,
    "robustSwapNFTsForToken": _extract_orderbook_robustSwapNFTsForToken,
}
