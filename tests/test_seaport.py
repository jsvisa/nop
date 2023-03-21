import os
import pandas as pd
import json
from nop.extractor.seaport_orderbook_extractor import calculate_seaport_orderbooks

TEST_DIR = os.path.dirname(os.path.abspath(__file__))


class TestSeaport:
    def test_n2n(self):
        ob_df = pd.DataFrame(
            json.load(open(f"{TEST_DIR}/testdata/seaport_orderbooks.json"))
        )
        tf_df = pd.DataFrame(
            json.load(open(f"{TEST_DIR}/testdata/seaport_token_xfers.json"))
        )
        ef_df = pd.DataFrame(
            json.load(open(f"{TEST_DIR}/testdata/seaport_erc1155_xfers.json"))
        )

        calculate_seaport_orderbooks(None, ob_df, tf_df, ef_df)  # type: ignore
