from typing import Dict, Set, List, Optional, Union
import logging
import json
import pandas as pd
from copy import copy
from eth_abi.abi import decode_single

from nop.extractor.extractor import NopExtractor
from nop.utils import as_st_day, to_normalized_address

from nop.constant import ZERO_ADDR
from nop.eth_decode import eth_decode_log

logger = logging.getLogger(__name__)


EV_INVENTORY_TOPIC = (
    "0x3cbb63f144840e5b1b0a38a7c19211d2e89de4d7c5faf8b2d3c1776c302d1d33"
)


X2Y2_Apps = {
    "0x74312363e45dcaba76c59ec49a7aa8a65a67eed3": "X2Y2",
}

# EvInventory(bytes32,address,address,uint256,uint256,uint256,uint256,uint256,address,bytes,tuple,tuple)

ABI = json.loads(
    r"""
{
  "anonymous": false,
  "inputs": [
    { "indexed": true, "internalType": "bytes32", "name": "itemHash", "type": "bytes32" },
    { "indexed": false, "internalType": "address", "name": "maker", "type": "address" },
    { "indexed": false, "internalType": "address", "name": "taker", "type": "address" },
    { "indexed": false, "internalType": "uint256", "name": "orderSalt", "type": "uint256" },
    { "indexed": false, "internalType": "uint256", "name": "settleSalt", "type": "uint256" },
    { "indexed": false, "internalType": "uint256", "name": "intent", "type": "uint256" },
    { "indexed": false, "internalType": "uint256", "name": "delegateType", "type": "uint256" },
    { "indexed": false, "internalType": "uint256", "name": "deadline", "type": "uint256" },
    { "indexed": false, "internalType": "contract IERC20Upgradeable", "name": "currency", "type": "address" },
    { "indexed": false, "internalType": "bytes", "name": "dataMask", "type": "bytes" },
    {
      "indexed": false,
      "internalType": "struct Market.OrderItem",
      "components": [
        { "internalType": "uint256", "name": "price", "type": "uint256" },
        { "internalType": "bytes", "name": "data", "type": "bytes" }
      ],
      "name": "item",
      "type": "tuple"
    },
    {
      "indexed": false,
      "internalType": "struct Market.SettleDetail",
      "components": [
        { "internalType": "enum Market.Op", "name": "op", "type": "uint8" },
        { "internalType": "uint256", "name": "orderIdx", "type": "uint256" },
        { "internalType": "uint256", "name": "itemIdx", "type": "uint256" },
        { "internalType": "uint256", "name": "price", "type": "uint256" },
        { "internalType": "bytes32", "name": "itemHash", "type": "bytes32" },
        { "internalType": "contract IDelegate", "name": "executionDelegate", "type": "address" },
        { "internalType": "bytes", "name": "dataReplacement", "type": "bytes" },
        { "internalType": "uint256", "name": "bidIncentivePct", "type": "uint256" },
        { "internalType": "uint256", "name": "aucMinIncrementPct", "type": "uint256" },
        { "internalType": "uint256", "name": "aucIncDurationSecs", "type": "uint256" },
        {
          "internalType": "struct Market.Fee[]",
          "name": "fees",
          "components": [
            { "internalType": "uint256", "name": "percentage", "type": "uint256" },
            { "internalType": "address", "name": "to", "type": "address" }
          ],
          "type": "tuple[]"
        }
      ],
      "name": "detail",
      "type": "tuple"
    }
  ],
  "name": "EvInventory",
  "type": "event"
}
"""
)

# enum Op {
#     INVALID,
#     // off-chain
#     COMPLETE_SELL_OFFER,
#     COMPLETE_BUY_OFFER,
#     CANCEL_OFFER,
#     // auction
#     BID,
#     COMPLETE_AUCTION,
#     REFUND_AUCTION,
#     REFUND_AUCTION_STUCK_ITEM
# }
#
# enum DelegationType {
#     INVALID,
#     ERC721,
#     ERC1155
# }
Op_enum = [
    "Invalid",
    "Sell",
    "Buy",
    "Cancel",
    "Bid",
    "Complete Auction",
    "Refund Aucton",
    "Refund Auction Stuck Item",
]

Delegation_type = ["Invalid", "erc721", "erc1155"]


class X2Y2OrderbookExtractor(NopExtractor):
    def _extract_orderbook(self, topics: List[str], data, **kwargs) -> List[Dict]:
        txhash = kwargs.get("txhash")
        logpos = kwargs.get("logpos")

        _, decoded_data_values = eth_decode_log(ABI, topics, data)
        if decoded_data_values is None:
            return [dict()]

        item = decoded_data_values[9]
        detail = decoded_data_values[10]

        item_price = item[0]  # Market.OrderItem
        op = detail[0]
        detail_price = detail[3]
        assert (
            detail_price >= item_price
        ), f"detail.price < item.price for tx: {txhash}, logpos: {logpos}"

        maker = to_normalized_address(decoded_data_values[0])
        taker = to_normalized_address(decoded_data_values[1])

        # # FIXME: Bid has different behaviors
        # if Op_enum[op] == "Buy":
        #     from_address, to_address = taker, maker
        # else:
        #     from_address, to_address = maker, taker

        base: Dict[str, Optional[Union[str, int]]] = dict(
            maker=maker,
            taker=taker,
            # from_address=from_address,
            # to_address=to_address,
            currency=to_normalized_address(decoded_data_values[7]),
            price=detail_price,
            executor=detail[5],
            token_type=Delegation_type[decoded_data_values[5]],
            action=Op_enum[op],
        )

        # bytes memory data = item.data;
        # {
        #     if (order.dataMask.length > 0 && detail.dataReplacement.length > 0) {
        #         _arrayReplace(data, detail.dataReplacement, order.dataMask);
        #     }
        # }
        # function _arrayReplace(
        #     bytes memory src,
        #     bytes memory replacement,
        #     bytes memory mask
        # ) internal view virtual {
        #     require(src.length == replacement.length);
        #     require(src.length == mask.length);
        #
        #     for (uint256 i = 0; i < src.length; i++) {
        #         if (mask[i] != 0) {
        #             src[i] = replacement[i];
        #         }
        #     }
        # }
        data = bytearray(item[1])
        data_mask = decoded_data_values[8]
        data_replacement = detail[6]
        if (
            len(data_mask) > 0
            and len(data_replacement) > 0
            and data != data_replacement
        ):
            assert (
                len(data_mask) == len(data_replacement) == len(data)
            ), f"data-mask <> data-replacement <> data tx: {txhash} logpos: {logpos}"
            for i, m in enumerate(data_mask):
                if m != 0:
                    data[i] = data_replacement[i]

            logger.info(
                f"replace data in tx: {txhash} logpos: {logpos} "
                f"before: {decode_single('((address,uint256)[])', item[1])[0]}"
                f"after: {decode_single('((address,uint256)[])', data)[0]}"
            )

        token_ids = decode_single("((address,uint256)[])", data)[0]
        orderbooks = []
        pack_index, pack_count = 0, len(token_ids)
        for token, token_id in token_ids:
            order = copy(base)
            order.update(
                dict(
                    pack_index=pack_index,
                    pack_count=pack_count,
                    # value=detail_price / pack_count,
                    token_address=token,
                    token_id=token_id,
                )
            )
            orderbooks.append(order)
            pack_index += 1

        return orderbooks

    def _allowed_orderbook_topics(self) -> Set[str]:
        return set([EV_INVENTORY_TOPIC])

    def _check_topic_data_length(self) -> bool:
        return False

    def _known_platform_apps(self) -> Dict[str, str]:
        return X2Y2_Apps

    def _calculate(
        self,
        tx_df: pd.DataFrame,  # transaction
        ob_df: pd.DataFrame,  # orderbook
        tf_df: pd.DataFrame,  # token transfer
        ef_df: pd.DataFrame,  # erc1155 transfer
    ):
        return calculate_x2y2_orderbooks(tx_df, ob_df, tf_df, ef_df)

    @staticmethod
    def platform():
        return "x2y2"


def calculate_x2y2_orderbooks(
    tx_df: pd.DataFrame,
    ob_df: pd.DataFrame,
    tf_df: pd.DataFrame,
    ef_df: pd.DataFrame,
):
    tx_df = tx_df
    merge_key = ["blknum", "txpos", "txhash", "_st"]

    erc721_df = (
        ob_df.query("token_type == 'erc721'")
        .merge(tf_df, how="inner", on=merge_key)  # type: ignore
        .query("token_address == x_token_address")
        .query("token_id == x_token_id")  # type: ignore
        .query("order_logpos > xfer_logpos")
        .query("prev_order_logpos < xfer_logpos")
        .rename(
            columns={
                "x_from_address": "from_address",
                "x_to_address": "to_address",
                "x_token_value": "token_value",
            }
        )
    )
    erc721_df["token_type"] = "erc721"

    erc1155_df = (
        ob_df.query("token_type == 'erc1155'")
        .merge(ef_df, how="inner", on=merge_key)  # type: ignore
        .query("token_address == s_token_address")
        .query("token_id == s_token_id")  # type: ignore
        .query("order_logpos > sfer_logpos")
        .query("prev_order_logpos < sfer_logpos")
        .rename(
            columns={
                "s_from_address": "from_address",
                "s_to_address": "to_address",
                "sfer_logpos": "xfer_logpos",
                "s_token_value": "token_value",
            }
        )
    )
    erc1155_df["token_type"] = "erc1155"

    df = pd.concat([erc721_df, erc1155_df], ignore_index=True)

    # README: kick out the BRUN events
    # if the Order has only one BURN event, then this order is not included
    df = df[df["to_address"] != ZERO_ADDR]
    # merge_key = ["txhash", "order_logpos"]
    # _df = (
    #     df.groupby(merge_key)["blknum"]
    #     .count()
    #     .reset_index()
    #     .rename(columns={"blknum": "pack_count"})  # type: ignore
    # )
    # df = df.merge(_df, how="left", on=merge_key)
    #
    # df["pack_index"] = (
    #     df.sort_values(by=["xfer_logpos"], ascending=True)
    #     .groupby(merge_key)  # type: ignore
    #     .cumcount()
    # )
    #
    # # keep the last Transfer event
    # df = df[(df["pack_index"] + 1 == df["pack_count"])]

    df["value"] = df["price"] / df["pack_count"]
    df["pattern"] = df["action"].apply(str.lower)
    df["_st_day"] = df["_st"].apply(as_st_day)

    return df
