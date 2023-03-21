import logging
import json
from typing import Dict, List, Set, NamedTuple, Optional, Union
import pandas as pd

from nop.extractor.extractor import NopExtractor
from nop.utils import as_st_day, to_normalized_address
from nop.eth_decode import eth_decode_log
from nop.constant import ZERO_ADDR
from nop.utils import partition_rank


logger = logging.getLogger(__name__)

Seaport_Apps = {
    "0x00000000006c3852cbef3e08e8df289169ede581": "Seaport_V1.1",
}


ORDER_FULFILLED_TOPIC = (
    "0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31"
)

# OrderFulfilled(bytes32,address,address,address,(uint8,address,uint256,uint256)[],(uint8,address,uint256,uint256,address)[])
ABI = json.loads(
    r"""
{
  "anonymous": false,
  "inputs": [
    { "indexed": false, "internalType": "bytes32", "name": "orderHash", "type": "bytes32" },
    { "indexed": true, "internalType": "address", "name": "offerer", "type": "address" },
    { "indexed": true, "internalType": "address", "name": "zone", "type": "address" },
    { "indexed": false, "internalType": "address", "name": "recipient", "type": "address" },
    {
      "components": [
        { "internalType": "enum ItemType", "name": "itemType", "type": "uint8" },
        { "internalType": "address", "name": "token", "type": "address" },
        { "internalType": "uint256", "name": "identifier", "type": "uint256" },
        { "internalType": "uint256", "name": "amount", "type": "uint256" }
      ],
      "indexed": false,
      "internalType": "struct SpentItem[]",
      "name": "offer",
      "type": "tuple[]"
    },
    {
      "components": [
        { "internalType": "enum ItemType", "name": "itemType", "type": "uint8" },
        { "internalType": "address", "name": "token", "type": "address" },
        { "internalType": "uint256", "name": "identifier", "type": "uint256" },
        { "internalType": "uint256", "name": "amount", "type": "uint256" },
        { "internalType": "address payable", "name": "recipient", "type": "address" }
      ],
      "indexed": false,
      "internalType": "struct ReceivedItem[]",
      "name": "consideration",
      "type": "tuple[]"
    }
  ],
  "name": "OrderFulfilled",
  "type": "event"
}
"""
)


ItemTypes = ["ether", "erc20", "erc721", "erc1155", "erc721-N", "erc1155-N"]


class SpentItem(NamedTuple):
    itemType: int
    token_address: str
    token_id: int
    amount: int

    def token_type(self) -> str:
        return ItemTypes[self.itemType]


class ReceivedItem(NamedTuple):
    itemType: int
    token_address: str
    token_id: int
    amount: int
    recipient: str


class OrderFulfilled(NamedTuple):
    orderHash: Optional[bytes]
    offerer: Optional[str]
    zone: Optional[str]
    recipient: Optional[str]
    spentItems: List[SpentItem]
    receivedItems: List[ReceivedItem]

    def n_spent(self) -> int:
        return len(self.spentItems)

    def n_received(self) -> int:
        return len(self.receivedItems)

    def e_spent(self) -> Set[str]:
        return set(ItemTypes[e.itemType] for e in self.spentItems)

    def e_received(self) -> Set[str]:
        return set(ItemTypes[e.itemType] for e in self.receivedItems)

    def u_spent(self) -> int:
        return len(self.e_spent())

    def u_received(self) -> int:
        return len(self.e_received())

    def u_spent_token_address(self, itemTypes: Optional[Set[str]] = None) -> int:
        return len(
            set(
                e.token_address
                for e in self.spentItems
                if itemTypes is None or ItemTypes[e.itemType] in itemTypes
            )
        )

    def u_received_token_address(self, itemTypes: Optional[Set[str]] = None) -> int:
        return len(
            set(
                e.token_address
                for e in self.receivedItems
                if itemTypes is None or ItemTypes[e.itemType] in itemTypes
            )
        )

    def pattern(self) -> str:
        return "s{ns}:{us}r{nr}:{ur}".format(
            ns=self.n_spent(),
            us=self.u_spent(),
            nr=self.n_received(),
            ur=self.u_received(),
        )


def decode_seaport_order(topics: List[str], data: str):
    x, y = eth_decode_log(ABI, topics, data)
    assert isinstance(x, tuple) and isinstance(y, tuple)

    return OrderFulfilled(
        orderHash=y[0],
        offerer=to_normalized_address(x[0]),
        zone=to_normalized_address(x[1]),
        recipient=to_normalized_address(y[1]),
        spentItems=[SpentItem(*e) for e in y[2]],
        receivedItems=[ReceivedItem(*e) for e in y[3]],
    )


class SeaportOrderbookExtractor(NopExtractor):
    def _extract_orderbook(
        self, topics: List[str], data, **kwargs
    ) -> Optional[Union[List[Dict], Dict]]:
        txhash = kwargs.get("txhash")
        logpos = kwargs.get("logpos")
        log_item = f"(txhash: {txhash} logpos: {logpos} nop: Seaport)"

        o = decode_seaport_order(topics, data)

        # ignore not matched paires, especially in Ether
        # {
        #     "_st": 1655272297,
        #     "_st_day": "2022-06-15",
        #     "blknum": 14965946,
        #     "logpos": 331,
        #     "txhash": "0xdca47eaf296dd6306c7725d1f96ed6e57c2c13db995741db8cb822faa00f33ea",
        #     "offerer": "0x556ca7014504a0e133eb66a8a36dc5501d682a82",
        #     "zone": "0x004c00500000ad104d7dbd00e3ae0a5c00560c00",
        #     "recipient": "0x0000000000000000000000000000000000000000",
        #     "spent_items": [
        #         {
        #             "itemType": 0,
        #             "token_address": "0x0000000000000000000000000000000000000000",
        #             "token_id": 0,
        #             "amount": 50000000000000000,
        #         }
        #     ],
        #     "received_items": "[]",
        #     "n_spent": 1,
        #     "n_receive": 0,
        #     "u_receive": 0,
        #     "u_spent": 1,
        #     "e_spent": "[0]",
        #     "e_receive": "[]",
        # }
        if o.n_spent() == 0 or o.n_received() == 0:
            logging.info(
                f"Ignore {log_item} zero items (n_spent: {o.n_spent()}, n_received: {o.n_received()})"
            )
            return None

        offerer = to_normalized_address(o.offerer)
        recipient = to_normalized_address(o.recipient)

        # see [docs/seaport.md](./docs/seaport.md) for more detail
        # 1. Spent NFT, Recv Ether/ERC20

        # In [74]: xf = df.query("n_spent == 1 and (e_spent == '[3]' or e_spent == '[2]')")
        # In [75]: xf.groupby(by=["e_received"])["txhash"].count()
        # Out[75]:
        # e_received
        # [1, 2]         1
        # [1, 3]        13
        # [0, 3]       424
        # [0, 2]       491
        # [1]          690
        # [0]       277840
        if (
            o.n_spent() == 1
            and o.e_spent() in ({"erc721"}, {"erc1155"})
            and len(o.e_received().intersection({"ether", "erc20"})) > 0
            and o.u_received_token_address(itemTypes={"ether", "erc20"}) == 1
        ):

            token = o.spentItems[0]
            erc20s = [
                e
                for e in o.receivedItems
                if ItemTypes[e.itemType] in ("ether", "erc20")
            ]

            return dict(
                maker=offerer,
                taker=recipient,
                token_address=to_normalized_address(token.token_address),
                token_id=token.token_id,
                token_value=token.amount,
                token_type=token.token_type(),
                currency=to_normalized_address(erc20s[0].token_address),
                price=sum(e.amount for e in erc20s),
                pack_index=0,
                pack_count=1,
                action="OrderFulfilled",
                pattern="p1-" + o.pattern(),
            )

        # 2. Spent ERC20, Recv NFT
        elif (
            o.n_spent() == 1
            and o.e_spent() == {"erc20"}
            and len(o.e_received().intersection({"erc721", "erc1155"})) > 0
            and o.u_received_token_address(itemTypes={"ether", "erc20"}) == 1
        ):
            erc20 = o.spentItems[0]
            tokens = [
                e
                for e in o.receivedItems
                if ItemTypes[e.itemType] in ("erc721", "erc1155")
            ]

            orderbooks = []
            for idx, token in enumerate(tokens):
                orderbooks.append(
                    dict(
                        maker=recipient,
                        taker=offerer,
                        token_address=to_normalized_address(token.token_address),
                        token_id=token.token_id,
                        token_value=token.amount,
                        token_type=ItemTypes[token.itemType],
                        currency=to_normalized_address(erc20.token_address),
                        price=erc20.amount,
                        pack_index=idx,
                        pack_count=len(tokens),
                        action="OrderFulfilled",
                        pattern="p2-" + o.pattern(),
                    )
                )
            return orderbooks

        # 3. Spent batch NFT, Recv Ether
        if (
            o.n_spent() > 1
            and o.e_spent() in ({"erc721"}, {"erc1155"}, {"erc721", "erc1155"})
            and o.e_received() == {"ether"}
        ):
            price = sum([e.amount for e in o.receivedItems])

            orderbooks = []
            for idx, token in enumerate(o.spentItems):
                orderbooks.append(
                    dict(
                        maker=offerer,
                        taker=recipient,
                        token_address=to_normalized_address(token.token_address),
                        token_id=token.token_id,
                        token_value=token.amount,
                        token_type=ItemTypes[token.itemType],
                        currency=ZERO_ADDR,
                        price=price,
                        pack_index=idx,
                        pack_count=o.n_spent(),
                        action="OrderFulfilled",
                        pattern="p3-" + o.pattern(),
                    )
                )
            return orderbooks

        # 4. Spent batch NFT, Recv Ether
        if (
            o.n_spent() > 1
            and o.e_spent() in ({"erc721"}, {"erc1155"}, {"erc721", "erc1155"})
            and len(o.e_received().intersection({"ether"})) > 0
        ):
            price = sum(
                [e.amount for e in o.receivedItems if ItemTypes[e.itemType] == "ether"]
            )

            orderbooks = []
            for idx, token in enumerate(o.spentItems):
                orderbooks.append(
                    dict(
                        maker=offerer,
                        taker=recipient,
                        token_address=to_normalized_address(token.token_address),
                        token_id=token.token_id,
                        token_value=token.amount,
                        token_type=ItemTypes[token.itemType],
                        currency=ZERO_ADDR,
                        price=price,
                        pack_index=idx,
                        pack_count=o.n_spent(),
                        action="OrderFulfilled",
                        pattern="p4-" + o.pattern(),
                    )
                )
            return orderbooks

        return None

    def _allowed_orderbook_topics(self) -> Set[str]:
        return set([ORDER_FULFILLED_TOPIC])

    def _check_topic_data_length(self) -> bool:
        return False

    def _known_platform_apps(self) -> Dict[str, str]:
        return Seaport_Apps

    def _calculate(
        self,
        tx_df: pd.DataFrame,  # transaction
        ob_df: pd.DataFrame,  # orderbook
        tf_df: pd.DataFrame,  # token transfer
        ef_df: pd.DataFrame,  # erc1155 transfer
    ):
        return calculate_seaport_orderbooks(tx_df, ob_df, tf_df, ef_df)

    @staticmethod
    def platform():
        return "seaport"


# 2022.07.11 currently those patterns may result in data duplication:
# wrong result: N Order + M Xfer => N*M Orderbook
# expected result: N Order + M Xfer => N Orderbook
S11_PS = ["p1-s1:1r2:1", "p1-s1:1r3:1", "p2-s1:1r2:2", "p2-s1:1r3:2"]


def calculate_seaport_orderbooks(
    tx_df: pd.DataFrame,  # transaction
    ob_df: pd.DataFrame,  # orderbook
    tf_df: pd.DataFrame,  # token transfer
    ef_df: pd.DataFrame,  # erc1155 transfer
):
    # ob_df.to_json("seaport_orderbooks.json", indent=2, orient="records")
    # tf_df.to_json("seaport_token_xfers.json", indent=2, orient="records")
    # ef_df.to_json("seaport_erc1155_xfers.json", indent=2, orient="records")

    tx_df = tx_df
    merge_key = ["blknum", "txpos", "txhash", "_st"]

    # README: Order may be above or below Transfer events
    # Order first, then Transfer:
    #   https://etherscan.io/tx/0x4516bf50ac1d037f3b4f9e81af0130c662132ce8723a3557aa1fd0e177e24487
    # Transfer first, then Order:
    #   https://etherscan.io/tx/0x7b3d5c4c25590a35793cdbd046cb1cd4cc11134442f1d22e27a8d64fcc353842
    erc721_df = (
        ob_df[ob_df.token_type.str.startswith("erc721")]
        .merge(tf_df, how="inner", on=merge_key)
        .query("token_address == x_token_address")
        .query("token_id == x_token_id")
        .query("token_value == x_token_value")
        .rename(
            columns={
                "x_from_address": "from_address",
                "x_to_address": "to_address",
            }
        )
    )

    erc1155_df = (
        ob_df[ob_df.token_type.str.startswith("erc1155")]
        .merge(ef_df, how="inner", on=merge_key)
        .query("token_address == s_token_address")
        .query("token_id == s_token_id")
        .query("token_value == s_token_value")
        .rename(
            columns={
                "s_from_address": "from_address",
                "s_to_address": "to_address",
                "sfer_logpos": "xfer_logpos",
            }
        )
    )

    df = pd.concat([erc721_df, erc1155_df], ignore_index=True)

    df_s11 = df[df["pattern"].isin(S11_PS)]
    df = df[~df["pattern"].isin(S11_PS)]

    partition_rank(df_s11, ["txhash", "order_logpos"], "_order_rank")
    partition_rank(df_s11, ["txhash", "xfer_logpos"], "_xfer_rank")

    df_s11 = df_s11.query("_order_rank == _xfer_rank")
    df_s11 = df_s11.drop(columns=["_order_rank", "_xfer_rank"])

    df = pd.concat([df, df_s11], ignore_index=True)
    # README: kick out the BRUN events
    # if the Order has only one BURN event, then this order is not included
    df = df[df["to_address"] != ZERO_ADDR]

    df["value"] = df["price"] / df["pack_count"]
    df["_st_day"] = df["_st"].apply(as_st_day)

    return df
