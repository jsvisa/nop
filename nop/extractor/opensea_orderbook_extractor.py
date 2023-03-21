import logging
from typing import Dict, List, Set
import pandas as pd

from nop.extractor.extractor import NopExtractor
from nop.utils import hex_to_dec, as_st_day, word_to_address
from nop.constant import ZERO_ADDR, ZERO_HASH
from nop.columns import ORDERBOOK_COLUMNS

logger = logging.getLogger(__name__)


ORDERS_MATCHED_TOPIC = (
    "0xc4109843e0b7d514e4c093114b863f8e7d8d9a458c372cd51bfe526b588006c9"
)

OpenSea_Apps = {
    "0x7be8076f4ea4a4ad08075c2508e481d6c946d12b": "OpenSea_V1",
    "0x7f268357a8c2552623316e2562d90e642bb538e5": "OpenSea_V2",
}


class OpenseaOrderbookExtractor(NopExtractor):
    def _extract_orderbook(self, topics_with_data: List[str], **kwargs) -> Dict:
        kwargs = kwargs
        # {
        #     "inputs": [
        #       { "indexed": false, "name": "buyHash", "type": "bytes32" },
        #       { "indexed": false, "name": "sellHash", "type": "bytes32" },
        #       { "indexed": true, "name": "maker", "type": "address" },
        #       { "indexed": true, "name": "taker", "type": "address" },
        #       { "indexed": false, "name": "price", "type": "uint256" },
        #       { "indexed": true, "name": "metadata", "type": "bytes32" }
        #     ],
        #     "name": "OrdersMatched",
        #     "type": "event"
        # }

        return dict(
            maker=word_to_address(topics_with_data[1]),
            taker=word_to_address(topics_with_data[2]),
            metadata=topics_with_data[3],
            price=hex_to_dec(topics_with_data[6]),
            action="Bid Win" if topics_with_data[4] != ZERO_HASH else "Bought",
        )

    def _allowed_orderbook_topics(self) -> Set[str]:
        return set([ORDERS_MATCHED_TOPIC])

    def _allowed_topic_data_length(self) -> int:
        return 7

    def _known_platform_apps(self) -> Dict[str, str]:
        return OpenSea_Apps

    def _calculate(
        self,
        tx_df: pd.DataFrame,  # transaction
        ob_df: pd.DataFrame,  # orderbook
        tf_df: pd.DataFrame,  # token transfer
        ef_df: pd.DataFrame,  # erc1155 transfer
    ):
        return calculate_opensea_orderbooks(tx_df, ob_df, tf_df, ef_df)

    @staticmethod
    def platform():
        return "opensea"


def calculate_opensea_orderbooks(
    tx_df: pd.DataFrame,
    ob_df: pd.DataFrame,
    tf_df: pd.DataFrame,
    ef_df: pd.DataFrame,
) -> pd.DataFrame:
    merge_key = ["blknum", "txpos", "txhash", "_st"]

    od_df = (
        ob_df.merge(tf_df, how="left", on=merge_key)
        .merge(ef_df, how="left", on=merge_key)
        .merge(tx_df, how="left", on=merge_key)
    )

    od_df = od_df[
        (
            (od_df["order_logpos"] > od_df["xfer_logpos"])
            & (od_df["prev_order_logpos"] < od_df["xfer_logpos"])
        )
        | (
            (od_df["order_logpos"] > od_df["sfer_logpos"])
            & (od_df["prev_order_logpos"] < od_df["sfer_logpos"])
        )
    ]
    st_df = (
        od_df.groupby(merge_key)
        .agg(
            {
                "x_token_address": "nunique",
                "s_token_address": "nunique",
                "order_logpos": "nunique",
                "xfer_logpos": "nunique",
                "sfer_logpos": "nunique",
            }
        )
        .rename(
            columns={
                "x_token_address": "xfer_tokens",
                "s_token_address": "sfer_tokens",
                "order_logpos": "order_count",
                "xfer_logpos": "xfer_count",
                "sfer_logpos": "sfer_count",
            }
        )
        .reset_index()
    )

    full_df = od_df.merge(st_df, how="left", on=merge_key)

    # 87%
    e11nn_df = (
        full_df.query("(order_count == xfer_count + sfer_count)")
        .query("(xfer_count == 0 | sfer_count == 0)")
        .copy()
    )
    e11nn_df = extract_e11nn_df(e11nn_df)

    # 5.5%
    e1n1n_df = (
        full_df.query("(order_count == 1)")
        .query(
            "  ( (xfer_count > 1) & (xfer_tokens == 1) & (sfer_count == 0) )"
            + " | "
            + "( (sfer_count > 1) & (sfer_tokens == 1) & (xfer_count == 0) )"
        )
        .copy()
    )
    e1n1n_df = extract_e1n1n_df(e1n1n_df)

    # all: 7.5%
    # currency and fee in ERC20, NFT in ERC721
    t1n20_df = (
        full_df.query("(order_count == 1) & (ether == 0) & (tx_to == platform)")
        .query("(xfer_count > 2) & (xfer_tokens == 2) & (sfer_count == 0)")
        .copy()
    )
    t1n20_df = extract_t1n20_df(t1n20_df)

    # currency and fee in ERC20, NFT in ERC1155
    t121n_df = (
        full_df.query("(order_count == 1) & (ether == 0) & (tx_to == platform)")
        .query("(xfer_count == 2) & (xfer_tokens == 1) & (sfer_count > 0)")
        .copy()
    )
    t121n_df = extract_t121n_df(t121n_df)
    dfs = []

    if e11nn_df is not None:
        dfs.append(e11nn_df)
    if e1n1n_df is not None:
        dfs.append(e1n1n_df)
    if t1n20_df is not None:
        dfs.append(t1n20_df)
    if t121n_df is not None:
        dfs.append(t121n_df)

    if len(dfs) == 0:
        return pd.DataFrame(columns=ORDERBOOK_COLUMNS)

    df = pd.concat(dfs, ignore_index=True)
    return df


def extract_e11nn_df(e11nn_df: pd.DataFrame):
    if len(e11nn_df) == 0:
        return None

    apply_xfer_sfer_attributes(e11nn_df)
    apply_ether_attributes(e11nn_df)
    e11nn_df["pack_index"] = 0
    e11nn_df["pack_count"] = 1
    e11nn_df["value"] = e11nn_df.apply(
        lambda row: 0 if row["to_address"] == ZERO_ADDR else row["price"],
        axis=1,
    )

    def pattern_it(row):
        if row["xfer_count"] == 1:
            return "e110"
        elif row["sfer_count"] == 1:
            return "e101"
        elif row["xfer_count"] > 1:
            return "enn0"
        elif row["sfer_count"] > 1:
            return "en0n"
        else:
            return None

    e11nn_df["pattern"] = e11nn_df.apply(pattern_it, axis=1)
    e11nn_df["_st_day"] = e11nn_df["_st"].apply(as_st_day)
    e11nn_df["trace_address"] = None
    return e11nn_df[ORDERBOOK_COLUMNS]


def extract_e1n1n_df(e1n1n_df: pd.DataFrame):
    if len(e1n1n_df) == 0:
        return None

    merge_key = ["txhash", "order_logpos"]
    e1n1n_df["pack_index"] = (
        e1n1n_df.sort_values(by=["xfer_logpos"], ascending=True)
        .groupby(merge_key)  # type: ignore
        .cumcount()
    )
    _df: pd.DataFrame = (
        e1n1n_df.groupby(merge_key)["blknum"]
        .count()
        .reset_index()
        .rename(columns={"blknum": "pack_count"})  # type: ignore
    )
    e1n1n_df = e1n1n_df.merge(_df, how="left", on=merge_key)

    apply_xfer_sfer_attributes(e1n1n_df)
    apply_ether_attributes(e1n1n_df)

    _vf = (
        e1n1n_df[e1n1n_df["to_address"] != ZERO_ADDR]
        .groupby(merge_key)["blknum"]
        .count()
        .reset_index()
        .rename(columns={"blknum": "split_count"})  # type: ignore
    )
    e1n1n_df = e1n1n_df.merge(_vf, how="left", on=merge_key)
    e1n1n_df["value"] = e1n1n_df.apply(
        lambda row: row["price"] / row["split_count"]
        if row["to_address"] != ZERO_ADDR
        else 0,
        axis=1,
    )

    def pattern_it(row):
        if row["xfer_count"] > 1:
            return "e1n0"
        elif row["sfer_count"] > 1:
            return "e10n"
        else:
            return None

    e1n1n_df["pattern"] = e1n1n_df.apply(pattern_it, axis=1)
    e1n1n_df["_st_day"] = e1n1n_df["_st"].apply(as_st_day)
    e1n1n_df["trace_address"] = None
    return e1n1n_df[ORDERBOOK_COLUMNS]


def extract_t1n20_df(t1n20_df: pd.DataFrame):
    if len(t1n20_df) == 0:
        return None

    merge_key = ["txhash", "order_logpos"]
    t1n20_df["_row"] = (
        t1n20_df.sort_values(by=["xfer_logpos"], ascending=True)
        .groupby(merge_key)  # type: ignore
        .cumcount()
        + 1  # row number starts with 1
    )

    # the first one is the currency row
    t1n20_cur_df = t1n20_df.query("_row == 1").rename(  # type: ignore
        columns={
            "x_token_address": "currency",
            "x_from_address": "value_from",
            "x_to_address": "value_to",
            "x_token_id": "value",
        }
    )[merge_key + ["currency", "value_from", "value_to", "value"]]

    # the second one is the fee(paid in currency)
    t1n20_fee_df = t1n20_df.query("_row == 2").rename(  # type: ignore
        columns={
            "x_token_address": "fee_currency",
            "x_from_address": "fee_from",
            "x_to_address": "fee_to",
            "x_token_id": "fee_value",
        }
    )[merge_key + ["fee_currency", "fee_from", "fee_to", "fee_value"]]

    # the last are NFT Transfers
    t1n20_nft_df = t1n20_df.query("_row >= 3").drop(columns=["_row"])  # type: ignore

    t1n20_nft_df["pack_index"] = (
        t1n20_nft_df.sort_values(by=["xfer_logpos"], ascending=True)
        .groupby(merge_key)
        .cumcount()
    )
    _df = (
        t1n20_nft_df.groupby(merge_key)
        .agg({"blknum": "count", "x_token_address": "nunique"})
        .rename(columns={"blknum": "pack_count", "x_token_address": "nft_tokens"})
        .reset_index()
    )

    t1n20_nft_df = (
        t1n20_nft_df.rename(
            columns={
                "x_token_address": "token_address",
                "x_from_address": "from_address",
                "x_to_address": "to_address",
                "x_token_id": "token_id",
                "x_token_value": "token_value",
            }
        )
        .merge(_df, how="left", on=merge_key)
        .merge(t1n20_cur_df, how="left", on=merge_key)
        .merge(t1n20_fee_df, how="left", on=merge_key)
        .query("(price == value) & (currency == fee_currency) & (nft_tokens == 1)")
    )

    if t1n20_nft_df.empty:
        return None

    _vf = (
        t1n20_nft_df[t1n20_nft_df["to_address"] != ZERO_ADDR]
        .groupby(merge_key)["blknum"]
        .count()
        .reset_index()
        .rename(columns={"blknum": "split_count"})  # type: ignore
    )

    t1n20_nft_df = t1n20_nft_df.merge(_vf, how="left", on=merge_key)

    t1n20_nft_df["token_type"] = "erc721"
    t1n20_nft_df["value"] = t1n20_nft_df.apply(
        lambda row: row["price"] / row["split_count"]
        if row["to_address"] != ZERO_ADDR
        else 0,
        axis=1,
    )
    t1n20_nft_df["fee_value"] = t1n20_nft_df.apply(
        lambda row: row["fee_value"] / row["split_count"]
        if row["to_address"] != ZERO_ADDR
        else 0,
        axis=1,
    )

    def pattern_it(row):
        if row["xfer_count"] > 3:
            return "t1n0"
        elif row["xfer_count"] == 3:
            return "t130"
        elif row["xfer_count"] == 2:
            return "t12n"
        else:
            return None

    t1n20_nft_df["pattern"] = t1n20_nft_df.apply(pattern_it, axis=1)
    t1n20_nft_df["_st_day"] = t1n20_nft_df["_st"].apply(as_st_day)
    t1n20_nft_df["trace_address"] = None
    return t1n20_nft_df[ORDERBOOK_COLUMNS]


def extract_t121n_df(t121n_df: pd.DataFrame):
    if len(t121n_df) == 0:
        return None

    merge_key = ["txhash", "order_logpos"]
    t121n_df["_row"] = (
        t121n_df.sort_values(by=["xfer_logpos"], ascending=True)
        .groupby(merge_key)  # type: ignore
        .cumcount()
        + 1  # row number starts with 1
    )

    # the first one is the currency row
    t121n_cur_df = t121n_df.query("_row == 1").rename(  # type: ignore
        columns={
            "x_token_address": "currency",
            "x_from_address": "value_from",
            "x_to_address": "value_to",
            "x_token_id": "value",
        }
    )[merge_key + ["currency", "value_from", "value_to", "value"]]

    # the second one is the fee(paid in currency)
    t121n_fee_df = t121n_df.query("_row == 2").rename(  # type: ignore
        columns={
            "x_token_address": "fee_currency",
            "x_from_address": "fee_from",
            "x_to_address": "fee_to",
            "x_token_id": "fee_value",
        }
    )[merge_key + ["fee_currency", "fee_from", "fee_to", "fee_value"]]

    # ERC1155 Transfers
    t121n_df.drop_duplicates(subset=merge_key + ["sfer_logpos"], inplace=True)

    t121n_df["pack_index"] = (
        t121n_df.sort_values(by=["sfer_logpos"], ascending=True)
        .groupby(merge_key)  # type: ignore
        .cumcount()
    )
    _df = (
        t121n_df.groupby(merge_key)
        .agg({"blknum": "count", "s_token_address": "nunique"})
        .rename(columns={"blknum": "pack_count", "s_token_address": "nft_tokens"})
        .reset_index()
    )

    t121n_df = (
        t121n_df.rename(
            columns={
                "s_token_address": "token_address",
                "s_from_address": "from_address",
                "s_to_address": "to_address",
                "s_token_id": "token_id",
                "s_token_value": "token_value",
            }
        )
        .merge(_df, how="left", on=merge_key)  # type: ignore
        .merge(t121n_cur_df, how="left", on=merge_key)
        .merge(t121n_fee_df, how="left", on=merge_key)
        .query("(price == value) & (currency == fee_currency) & (nft_tokens == 1)")
    )

    if t121n_df.empty:
        return None

    _vf = (
        t121n_df[t121n_df["to_address"] != ZERO_ADDR]
        .groupby(merge_key)["blknum"]
        .count()
        .reset_index()
        .rename(columns={"blknum": "split_count"})  # type: ignore
    )
    t121n_df = t121n_df.merge(_vf, how="left", on=merge_key)
    t121n_df["xfer_logpos"] = t121n_df["sfer_logpos"]
    t121n_df["token_type"] = "erc1155"
    t121n_df["value"] = t121n_df.apply(
        lambda row: row["price"] / row["split_count"]
        if row["to_address"] != ZERO_ADDR
        else 0,
        axis=1,
    )
    t121n_df["fee_value"] = t121n_df.apply(
        lambda row: row["fee_value"] / row["split_count"]
        if row["to_address"] != ZERO_ADDR
        else 0,
        axis=1,
    )
    t121n_df["pattern"] = "t12n"
    t121n_df["_st_day"] = t121n_df["_st"].apply(as_st_day)
    t121n_df["trace_address"] = None
    return t121n_df[ORDERBOOK_COLUMNS]


def apply_ether_attributes(df):
    df["currency"] = "0x0000000000000000000000000000000000000000"
    df["value_from"] = None
    df["value_to"] = None
    df["fee_from"] = None
    df["fee_to"] = None
    df["fee_currency"] = None
    df["fee_value"] = None


def apply_xfer_sfer_attributes(df):
    df["xfer_logpos"] = df.apply(
        lambda row: row["xfer_logpos"] if row["xfer_count"] > 0 else row["sfer_logpos"],
        axis=1,
    )
    df["token_type"] = df.apply(
        lambda row: "erc721" if row["xfer_count"] > 0 else "erc1155",
        axis=1,
    )
    df["token_address"] = df.apply(
        lambda row: row["x_token_address"]
        if row["xfer_count"] > 0
        else row["s_token_address"],
        axis=1,
    )
    df["token_id"] = df.apply(
        lambda row: row["x_token_id"] if row["xfer_count"] > 0 else row["s_token_id"],
        axis=1,
    )
    df["token_value"] = df.apply(
        lambda row: row["x_token_value"]
        if row["xfer_count"] > 0
        else row["s_token_value"],
        axis=1,
    )
    df["from_address"] = df.apply(
        lambda row: row["x_from_address"]
        if row["xfer_count"] > 0
        else row["s_from_address"],
        axis=1,
    )
    df["to_address"] = df.apply(
        lambda row: row["x_to_address"]
        if row["xfer_count"] > 0
        else row["s_to_address"],
        axis=1,
    )
