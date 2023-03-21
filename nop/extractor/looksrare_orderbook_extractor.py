from typing import Dict, List, Set
import pandas as pd

from nop.extractor.extractor import NopExtractor
from nop.utils import hex_to_dec, as_st_day, word_to_address
from nop.constant import ZERO_ADDR


TAKER_BID_TOPIC = "0x95fb6205e23ff6bda16a2d1dba56b9ad7c783f67c96fa149785052f47696f2be"
TAKER_ASK_TOPIC = "0x68cd251d4d267c6e2034ff0088b990352b97b2002c0476587d0c4da889c11330"
LOOKSRARE_ORDERBOOK_TOPICS = set([TAKER_ASK_TOPIC, TAKER_BID_TOPIC])


LooksRare_Apps = {
    "0x59728544b08ab483533076417fbbb2fd0b17ce3a": "LooksRare",
}


class LooksrareOrderbookExtractor(NopExtractor):
    def _extract_orderbook(self, topics_with_data: List[str], **kwargs) -> Dict:
        kwargs = kwargs
        # TakerAsk and TakerBid are in the same topic/data ABI encoding sequence, use TakerAsk for example:
        #  {
        #    "inputs": [
        #     4. { "indexed": false, "internalType": "bytes32", "name": "orderHash", "type": "bytes32" },
        #     5. { "indexed": false, "internalType": "uint256", "name": "orderNonce", "type": "uint256" },
        #     1. { "indexed": true, "internalType": "address", "name": "taker", "type": "address" },
        #     2. { "indexed": true, "internalType": "address", "name": "maker", "type": "address" },
        #     3. { "indexed": true, "internalType": "address", "name": "strategy", "type": "address" },
        #     6. { "indexed": false, "internalType": "address", "name": "currency", "type": "address" },
        #     7. { "indexed": false, "internalType": "address", "name": "collection", "type": "address" },
        #     8. { "indexed": false, "internalType": "uint256", "name": "tokenId", "type": "uint256" },
        #     9. { "indexed": false, "internalType": "uint256", "name": "amount", "type": "uint256" },
        #    10. { "indexed": false, "internalType": "uint256", "name": "price", "type": "uint256" }
        #    ],
        #    "name": "TakerAsk",
        #    "type": "event"
        #  },
        return dict(
            taker=word_to_address(topics_with_data[1]),
            maker=word_to_address(topics_with_data[2]),
            currency=word_to_address(topics_with_data[6]),
            token_address=word_to_address(topics_with_data[7]),
            token_id=hex_to_dec(topics_with_data[8]),
            token_value=hex_to_dec(topics_with_data[9]),
            price=hex_to_dec(topics_with_data[10]),
            # action similar to etherscan's style
            action="Bought" if topics_with_data[0] == TAKER_ASK_TOPIC else "Bid Won",
        )

    def _allowed_orderbook_topics(self) -> Set[str]:
        return LOOKSRARE_ORDERBOOK_TOPICS

    def _allowed_topic_data_length(self) -> int:
        return 11

    def _known_platform_apps(self) -> Dict[str, str]:
        return LooksRare_Apps

    def _calculate(
        self,
        tx_df: pd.DataFrame,  # transaction
        ob_df: pd.DataFrame,  # orderbook
        tf_df: pd.DataFrame,  # token transfer
        ef_df: pd.DataFrame,  # erc1155 transfer
    ):
        return calculate_looksrare_orderbooks(tx_df, ob_df, tf_df, ef_df)

    @staticmethod
    def platform():
        return "looksrare"


def calculate_looksrare_orderbooks(
    tx_df: pd.DataFrame,
    ob_df: pd.DataFrame,
    tf_df: pd.DataFrame,
    ef_df: pd.DataFrame,
):
    tx_df = tx_df

    merge_key = ["blknum", "txpos", "txhash", "_st"]

    erc721_df = (
        ob_df.merge(tf_df, how="inner", on=merge_key)
        .query("token_address == x_token_address")
        .query("token_id == x_token_id")  # type: ignore
        .query("token_value == x_token_value")
        .query("order_logpos > xfer_logpos")
        .query("prev_order_logpos < xfer_logpos")
        .rename(
            columns={
                "x_from_address": "from_address",
                "x_to_address": "to_address",
            }
        )
    )
    erc721_df["token_type"] = "erc721"

    erc1155_df = (
        ob_df.merge(ef_df, how="inner", on=merge_key)
        .query("token_address == s_token_address")
        .query("token_id == s_token_id")  # type: ignore
        .query("token_value == s_token_value")
        .query("order_logpos > sfer_logpos")
        .query("prev_order_logpos < sfer_logpos")
        .rename(
            columns={
                "s_from_address": "from_address",
                "s_to_address": "to_address",
                "sfer_logpos": "xfer_logpos",
            }
        )
    )
    erc1155_df["token_type"] = "erc1155"

    df = pd.concat([erc721_df, erc1155_df], ignore_index=True)

    # README: kick out the BRUN events
    # if the Order has only one BURN event, then this order is not included
    df = df[df["to_address"] != ZERO_ADDR]
    merge_key = ["txhash", "order_logpos"]
    _df = (
        df.groupby(merge_key)["blknum"]
        .count()
        .reset_index()
        .rename(columns={"blknum": "pack_count"})  # type: ignore
    )
    df = df.merge(_df, how="left", on=merge_key)

    df["pack_index"] = (
        df.sort_values(by=["xfer_logpos"], ascending=True)
        .groupby(merge_key)  # type: ignore
        .cumcount()
    )

    # keep the last Transfer event
    df = df[(df["pack_index"] + 1 == df["pack_count"])]

    df["value"] = df["price"]
    df["pattern"] = df["action"].apply(str.lower)
    df["_st_day"] = df["_st"].apply(as_st_day)

    return df
