import pandas as pd
import logging
from time import time
from sqlalchemy.engine import Engine


from typing import Dict, Set, List, Union, Optional
from nop.columns import ORDERBOOK_COLUMNS, TF_COLUMNS, EF_COLUMNS, TX_COLUMNS
from nop.utils import split_to_words, to_normalized_address, as_st_day
from nop.misc.check_trace_ready_template import CHECK_TRACE_READY_TEMPLATE

logger = logging.getLogger(__name__)


class NopExtractor(object):
    @staticmethod
    def chain() -> str:
        return "ethereum"

    @staticmethod
    def extract_via_log() -> bool:
        return True

    @staticmethod
    def platform() -> bool:
        raise NotImplementedError

    def extract_orderbooks(
        self,
        logs: List[Dict],
        only_known_platform: bool = True,
        db_engine: Optional[Engine] = None,
        block_range: Optional[List[Dict]] = None,
    ):
        if self.extract_via_log() is True:
            return self.extract_orderbook_from_logs(logs, only_known_platform)
        else:
            assert db_engine is not None and block_range is not None
            return self.extract_orderbook_from_traces(db_engine, block_range)

    def extract_orderbook_from_logs(
        self, logs: List[Dict], only_known_platform: bool = True
    ):
        for log in logs:
            orderbook = self.extract_orderbook_from_log(log, only_known_platform)
            if orderbook is None:
                continue
            elif isinstance(orderbook, dict):
                yield orderbook
            elif isinstance(orderbook, list):
                for od in orderbook:
                    yield od

    def extract_orderbook_from_log(self, log: Dict, only_known_platform: bool = True):
        assert isinstance(log, dict)

        topics = log.get("topics")
        if topics is None or len(topics) < 1:
            return None

        topics_0 = topics[0]
        if topics_0 not in self._allowed_orderbook_topics():
            return None

        topics_with_data = topics + split_to_words(log.get("data"))
        n_topics = len(topics_with_data)
        if (
            self._check_topic_data_length() is True
            and n_topics != self._allowed_topic_data_length()
        ):
            logger.warning(
                "The number of topics and data parts "
                "is not equal to {} in log {} of transaction {}".format(
                    self._allowed_topic_data_length(),
                    log["log_index"],
                    log["transaction_hash"],
                )
            )
            return None

        platform = to_normalized_address(log["address"])
        if (
            platform is None
            or only_known_platform is True
            and platform not in self._known_platform_apps()
        ):
            return None

        orderbook = self._extract_orderbook(
            topics_with_data=topics_with_data,
            topics=topics,
            data=log.get("data"),
            txhash=log["transaction_hash"],
            logpos=log["log_index"],
        )
        if orderbook is None:
            return None

        base = dict(
            platform=platform,
            app=self._known_platform_apps().get(platform, "Unknown"),
            txhash=log["transaction_hash"],
            txpos=log["transaction_index"],
            order_logpos=log["log_index"],
            blknum=log["block_number"],
            _st=log.get("block_timestamp"),
        )
        if isinstance(orderbook, dict):
            orderbook.update(base)
        elif isinstance(orderbook, list):
            for od in orderbook:
                od.update(base)
        return orderbook

    def extract_orderbook_from_traces(self, db_engine: Engine, block_range: List[Dict]):
        st_blknum = min(b["number"] for b in block_range)
        et_blknum = max(b["number"] for b in block_range)
        st = min(b["timestamp"] for b in block_range)
        et = max(b["timestamp"] for b in block_range)
        st_day = as_st_day(st)
        et_day = as_st_day(et)

        # assume the old(<1.5day) traces were ready
        if et >= int(time()) - 1.5 * 86400:
            check_sql = CHECK_TRACE_READY_TEMPLATE.format(
                chain=self.chain(),
                st_blknum=st_blknum,
                et_blknum=et_blknum,
                st_day=st_day,
                et_day=et_day,
            )
            rows = db_engine.execute(check_sql).fetchall()
            assert rows is not None

            trace_blocks = set(e["blknum"] for e in rows)
            block_blocks = set(
                b["number"] for b in block_range if b["transaction_count"] > 0
            )
            if trace_blocks != block_blocks:
                raise ValueError(
                    f"{self.chain()}.traces for [{st_blknum}, {et_blknum}] were not ready "
                    f"trace_blocks +: {trace_blocks - block_blocks} block_blocks +: {block_blocks - trace_blocks}"
                )

        return self._extract_orderbook_from_traces(
            db_engine, st_blknum, et_blknum, st_day, et_day
        )

    def calculate(
        self,
        tx_df: pd.DataFrame,  # transaction
        ob_df: pd.DataFrame,  # orderbook
        tf_df: pd.DataFrame,  # token transfer
        ef_df: pd.DataFrame,  # erc1155 transfer
    ):
        if tx_df.empty or ob_df.empty or len(tf_df) + len(ef_df) == 0:
            return pd.DataFrame(columns=ORDERBOOK_COLUMNS)

        tx_df.rename(
            columns={
                "value": "ether",
                "to_address": "tx_to",
            },
            inplace=True,
        )
        tx_df = tx_df[["blknum", "txpos", "txhash", "_st", "ether", "tx_to"]]
        if tx_df.empty:
            tx_df = pd.DataFrame(columns=TX_COLUMNS)

        if "order_logpos" in ob_df.columns:
            ob_df["prev_order_logpos"] = (
                ob_df.sort_values(by=["blknum", "txpos"], ascending=True)
                .groupby(["txhash"])["order_logpos"]  # type: ignore
                .shift(1, fill_value=-1)
            )
            ob_df["next_order_logpos"] = (
                ob_df.sort_values(by=["blknum", "txpos"], ascending=True)
                .groupby(["txhash"])["order_logpos"]  # type: ignore
                .shift(-1, fill_value=2**32)
            )

        tf_df.rename(
            columns={
                "logpos": "xfer_logpos",
                "token_address": "x_token_address",
                "from_address": "x_from_address",
                "to_address": "x_to_address",
                "value": "x_token_id",
            },
            inplace=True,
        )
        tf_df["x_token_value"] = 1
        if tf_df.empty:
            tf_df = pd.DataFrame(columns=TF_COLUMNS)

        ef_df.drop(
            columns=["operator", "xfer_type", "id_pos", "id_cnt"],
            inplace=True,
            errors="ignore",
        )
        ef_df.rename(
            columns={
                "logpos": "sfer_logpos",
                "token_address": "s_token_address",
                "from_address": "s_from_address",
                "to_address": "s_to_address",
                "id": "s_token_id",
                "value": "s_token_value",
            },
            inplace=True,
        )
        if ef_df.empty:
            ef_df = pd.DataFrame(columns=EF_COLUMNS)

        df = self._calculate(tx_df, ob_df, tf_df, ef_df)

        # fill missing columns to None
        for c in set(ORDERBOOK_COLUMNS) - set(df.columns):
            df[c] = None
        return df

    def _allowed_orderbook_topics(self) -> Set[str]:
        raise NotImplementedError

    def _allowed_topic_data_length(self) -> int:
        raise NotImplementedError

    def _known_platform_apps(self) -> Dict[str, str]:
        raise NotImplementedError

    def _extract_orderbook(self, **kwargs) -> Optional[Union[Dict, List[Dict]]]:
        raise NotImplementedError

    def _check_topic_data_length(self) -> bool:
        return True

    def _calculate(
        self,
        tx_df: pd.DataFrame,  # transaction
        ob_df: pd.DataFrame,  # orderbook
        tf_df: pd.DataFrame,  # token transfer
        ef_df: pd.DataFrame,  # erc1155 transfer
    ) -> pd.DataFrame:
        raise NotImplementedError

    def _extract_orderbook_from_traces(
        self,
        db_engine: Engine,
        st_blknum: int,
        et_blknum: int,
        st_day: str,
        et_day: str,
    ):
        raise NotImplementedError
