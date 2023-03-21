from datetime import datetime
from typing import Optional, Union, List

import pandas as pd


def hex_to_dec(hex_string: Optional[str]) -> Optional[Union[str, int]]:
    if hex_string is None:
        return None
    try:
        return int(hex_string, 16)
    except ValueError:
        print("Not a hex string %s" % hex_string)
        return hex_string


def as_st_day(st: int) -> str:
    return datetime.utcfromtimestamp(st).strftime("%Y-%m-%d")


def to_normalized_address(address: Optional[str]) -> Optional[str]:
    if address is None or not isinstance(address, str):
        return address
    return address.lower()


def chunk_string(string, length):
    return (string[0 + i : length + i] for i in range(0, len(string), length))


def split_to_words(data):
    if data and len(data) > 2:
        data_without_0x = data[2:]
        words = list(chunk_string(data_without_0x, 64))
        words_with_0x = list(map(lambda word: "0x" + word, words))
        return words_with_0x
    return []


def word_to_address(param):
    if param is None:
        return None
    elif len(param) >= 40:
        return to_normalized_address("0x" + param[-40:])
    else:
        return to_normalized_address(param)


def isnamedtupleinstance(x):
    _type = type(x)
    bases = _type.__bases__
    if len(bases) != 1 or bases[0] != tuple:
        return False
    fields = getattr(_type, "_fields", None)
    if not isinstance(fields, tuple):
        return False
    return all(type(i) == str for i in fields)


def unpack(obj):
    if isinstance(obj, dict):
        return {key: unpack(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [unpack(value) for value in obj]
    elif isnamedtupleinstance(obj):
        return {key: unpack(value) for key, value in obj._asdict().items()}
    elif isinstance(obj, tuple):
        return tuple(unpack(value) for value in obj)
    else:
        return obj


def partition_rank(
    df: pd.DataFrame, group_by: List, rank_column="_rank"
) -> pd.DataFrame:
    df[rank_column] = df.groupby(group_by).cumcount()
    df_rank: pd.DataFrame = df.groupby(group_by).agg({rank_column: [min, max, "count"]})
    df_rank.columns = ["_".join(c) for c in df_rank.columns]  # type: ignore
    df_rank.reset_index(inplace=True)

    return df.merge(df_rank, on=group_by)
