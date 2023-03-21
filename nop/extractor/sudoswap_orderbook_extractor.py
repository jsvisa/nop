import logging
from typing import Dict
import pandas as pd
from sqlalchemy.engine import Engine

from nop.extractor.extractor import NopExtractor

from nop.misc.sudoswap_read_trace_template import READ_TRACE_TEMPLATE
from nop.misc.sudoswap_method_extractor import (
    PATTERN_EXTRACTORS,
    PACK_GROUP_KEY,
    SUDOSWAP_COLUMNS,
)

logger = logging.getLogger(__name__)

SUDOSWAP_CONTRACT = "0x2b2e8cda09bba9660dca5cb6233787738ad68329"
SUDOSWAP_APP = "Sudoswap"


class SudoswapOrderbookExtractor(NopExtractor):
    @staticmethod
    def platform():
        return "sudoswap"

    @staticmethod
    def extract_via_log() -> bool:
        return False

    __pools = dict()

    def get_pools(self, engine: Engine) -> Dict:
        if len(self.__pools) == 0:
            self.getset_pools(engine)

        return self.__pools

    def getset_pools(self, engine: Engine):
        df = pd.read_sql(
            f"SELECT DISTINCT pool_address, token_address, currency FROM {self.chain()}.sudoswap_pools",
            con=engine,
        )
        self.__pools = {
            r["pool_address"]: (r["token_address"], r["currency"])
            for _, r in df.iterrows()
        }

    def _extract_orderbook_from_traces(
        self,
        engine: Engine,
        start_blknum,
        end_blknum,
        start_day,
        end_day,
    ):
        sql = READ_TRACE_TEMPLATE.format(
            st_blknum=start_blknum,
            et_blknum=end_blknum,
            st_day=start_day,
            et_day=end_day,
            sudoswap_contract_address=SUDOSWAP_CONTRACT,
        )

        df = pd.read_sql(sql, con=engine)
        if len(df) == 0:
            return []

        of = pd.DataFrame(columns=SUDOSWAP_COLUMNS)
        for pattern, extractor in PATTERN_EXTRACTORS.items():
            mf = df[df.pattern == pattern].copy()
            mf_len = len(mf)
            if len(mf) > 0:
                mf = extractor(mf)
            logger.info(f"extract {pattern} with input: #{mf_len} output: #{len(mf)}")
            if len(mf) > 0:
                of = pd.concat([of, mf[SUDOSWAP_COLUMNS]])

        of = self.fill_pair_with_nft(of, engine)
        of.drop(columns=["pair"], inplace=True)

        return of.to_dict("records")

    def fill_pair_with_nft(self, df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
        pools = self.get_pools(engine)
        if not set(df["pair"]).issubset(set(pools.keys())):
            self.getset_pools(engine)
            pools = self.get_pools(engine)

        notfound = set(df["pair"]) - set(pools.keys())
        if len(notfound) > 0:
            raise ValueError(
                f"pair: {notfound} not found in {self.chain()}.sudoswap_pools"
            )
        df["token_address"] = df["pair"].apply(lambda x: pools[x][0])
        df["currency"] = df["pair"].apply(lambda x: pools[x][1])
        return df

    def _calculate(
        self,
        tx_df: pd.DataFrame,  # transaction
        ob_df: pd.DataFrame,  # orderbook
        tf_df: pd.DataFrame,  # token transfer
        ef_df: pd.DataFrame,  # erc1155 transfer
    ):
        return calculate_sudoswap_orderbooks(tx_df, ob_df, tf_df, ef_df)


MERGE_KEY = ["blknum", "txpos", "txhash", "_st"]
LEFT_ON = MERGE_KEY + ["token_address", "token_id", "from_address", "to_address"]
RIGHT_ON = MERGE_KEY + [
    "x_token_address",
    "x_token_id",
    "x_from_address",
    "x_to_address",
]


# 2022.08.10 Sudoswap ONLY supports ERC721
def calculate_sudoswap_orderbooks(
    tx_df: pd.DataFrame,
    ob_df: pd.DataFrame,
    tf_df: pd.DataFrame,
    ef_df: pd.DataFrame,
):
    tx_df = tx_df
    ef_df = ef_df

    # remove the entity, who's TokenXfer not found
    # see this tx for more informaction
    # the token-id 2261,2341,4803 not actually found in the swap event
    # {
    #   "txhash": "0xe116755c51688ccdc9f3990e3c4018247ec98e6c9b0eefaba98d24df95c0354b",
    #   "pattern": "robustSwapETHForSpecificNFTs",
    #   "swapList": [
    #     {
    #       "pool_address": "0x6Bf4e731941111833E64e9c9DDc29dA2aaA90252",
    #       "nft_collection": None,
    #       "value": "179370219201610803",
    #       "nft_ids": ["2261", "2341"],
    #     },
    #     {
    #       "pool_address": "0x0CB58B200dAf0FB6eFb8604fE90D097Bb2EB4d35",
    #       "nft_collection": None,
    #       "value": "89027601249565598",
    #       "nft_ids": ["4803"],
    #     },
    #     {
    #       "pool_address": "0x6748F6Ae90c619559348EA9980A311f18dFdeab7",
    #       "nft_collection": "0x6C5a06AE6b773457480c12F12C2fB22627507A3A",
    #       "value": "90157849599999925",
    #       "nft_ids": ["1786"],
    #     },
    #     {
    #       "pool_address": "0x2D7536109EFb51Bf770B2151101055226eB4B8Df",
    #       "nft_collection": "0x6C5a06AE6b773457480c12F12C2fB22627507A3A",
    #       "value": "90635357565550180",
    #       "nft_ids": ["2413"],
    #     },
    #   ]
    # }
    ob_df = ob_df.merge(tf_df, how="inner", left_on=LEFT_ON, right_on=RIGHT_ON)

    # TODO: drop the duplicate if the same token-id Transfered more than once?

    ob_df["pack_index"] = ob_df.groupby(PACK_GROUP_KEY).cumcount()
    _of = (
        ob_df.groupby(PACK_GROUP_KEY)["blknum"].count().reset_index(name="pack_count")  # type: ignore
    )
    ob_df = ob_df.merge(_of, how="left", on=PACK_GROUP_KEY)
    ob_df["value"] = ob_df["price"] / ob_df["pack_count"]
    ob_df["token_type"] = "erc721"
    ob_df["token_value"] = 1
    ob_df["action"] = ob_df["pattern"]
    ob_df["platform"] = SUDOSWAP_CONTRACT
    ob_df["app"] = SUDOSWAP_APP

    return ob_df
