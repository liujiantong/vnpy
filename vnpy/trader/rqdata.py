from datetime import datetime, timedelta
from typing import List, Optional

from numpy import ndarray

from rqdatac import init as rqdata_init
from rqdatac.services.basic import all_instruments as rqdata_all_instruments
from rqdatac.services.get_price import get_price as rqdata_get_price
from rqdatac.share.errors import AuthenticationFailed

import jqdatasdk as jq

from .setting import SETTINGS
from .constant import Exchange, Interval
from .object import BarData, HistoryRequest


INTERVAL_VT2RQ = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "60m",
    Interval.DAILY: "1d",
}

INTERVAL_ADJUSTMENT_MAP = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta()         # no need to adjust for daily bar
}

EX_VT2JQ_DICT = {
    Exchange.CFFEX: 'CCFX', Exchange.SHFE: 'XSGE', Exchange.CZCE: 'XZCE', Exchange.DCE: 'XDCE',
    Exchange.INE: 'XINE', Exchange.SSE: 'XSHG', Exchange.SZSE: 'XSHE', Exchange.SGE: 'XSGE'
}


class RqdataClient:
    """
    Client for querying history data from RQData.
    """

    def __init__(self):
        """"""
        self.username: str = SETTINGS["rqdata.username"]
        self.password: str = SETTINGS["rqdata.password"]

        self.inited: bool = False
        self.symbols: ndarray = None

    def init(self, username: str = "", password: str = "") -> bool:
        """"""
        if self.inited:
            return True

        if username and password:
            self.username = username
            self.password = password

        if not self.username or not self.password:
            return False

        try:
            rqdata_init(
                self.username,
                self.password,
                ("rqdatad-pro.ricequant.com", 16011),
                use_pool=True,
                max_pool_size=1
            )

            df = rqdata_all_instruments()
            self.symbols = df["order_book_id"].values
        except (RuntimeError, AuthenticationFailed):
            return False

        self.inited = True
        return True

    def to_rq_symbol(self, symbol: str, exchange: Exchange) -> str:
        """
        CZCE product of RQData has symbol like "TA1905" while
        vt symbol is "TA905.CZCE" so need to add "1" in symbol.
        """
        # Equity
        if exchange in [Exchange.SSE, Exchange.SZSE]:
            if exchange == Exchange.SSE:
                rq_symbol = f"{symbol}.XSHG"
            else:
                rq_symbol = f"{symbol}.XSHE"
        # Futures and Options
        elif exchange in [Exchange.SHFE, Exchange.CFFEX, Exchange.DCE, Exchange.DCE, Exchange.INE]:
            for count, word in enumerate(symbol):
                if word.isdigit():
                    break

            product = symbol[:count]
            time_str = symbol[count:]

            # Futures
            if time_str.isdigit():
                if exchange is not Exchange.CZCE:
                    return symbol.upper()

                # Check for index symbol
                if time_str in ["88", "888", "99"]:
                    return symbol

                year = symbol[count]
                month = symbol[count + 1:]

                if year == "9":
                    year = "1" + year
                else:
                    year = "2" + year

                rq_symbol = f"{product}{year}{month}".upper()
            # Options
            else:
                if exchange in [Exchange.CFFEX, Exchange.DCE, Exchange.SHFE]:
                    rq_symbol = symbol.replace("-", "").upper()
                elif exchange == Exchange.CZCE:
                    year = symbol[count]
                    suffix = symbol[count + 1:]

                    if year == "9":
                        year = "1" + year
                    else:
                        year = "2" + year

                    rq_symbol = f"{product}{year}{suffix}".upper()
        else:
            rq_symbol = f"{symbol}.{exchange.value}"

        return rq_symbol

    def query_history(self, req: HistoryRequest) -> Optional[List[BarData]]:
        """
        Query history bar data from RQData.
        """
        symbol = req.symbol
        exchange = req.exchange
        interval = req.interval
        start = req.start
        end = req.end

        rq_symbol = self.to_rq_symbol(symbol, exchange)
        if rq_symbol not in self.symbols:
            return None

        rq_interval = INTERVAL_VT2RQ.get(interval)
        if not rq_interval:
            return None

        # For adjust timestamp from bar close point (RQData) to open point (VN Trader)
        adjustment = INTERVAL_ADJUSTMENT_MAP[interval]

        # For querying night trading period data
        end += timedelta(1)

        # Only query open interest for futures contract
        fields = ["open", "high", "low", "close", "volume"]
        if not symbol.isdigit():
            fields.append("open_interest")

        df = rqdata_get_price(
            rq_symbol,
            frequency=rq_interval,
            fields=fields,
            start_date=start,
            end_date=end,
            adjust_type="none"
        )

        data: List[BarData] = []

        if df is not None:
            for ix, row in df.iterrows():
                bar = BarData(
                    symbol=symbol,
                    exchange=exchange,
                    interval=interval,
                    datetime=row.name.to_pydatetime() - adjustment,
                    open_price=row["open"],
                    high_price=row["high"],
                    low_price=row["low"],
                    close_price=row["close"],
                    volume=row["volume"],
                    open_interest=row.get("open_interest", 0),
                    gateway_name="RQ"
                )

                data.append(bar)

        return data


class JqdataClient:
    """
    Client for querying history data from RQData.
    """

    def __init__(self):
        self.username: str = SETTINGS["jqdata.username"]
        self.password: str = SETTINGS["jqdata.password"]
        self.inited: bool = False
        self.symbols = None

    def init(self, username: str = "", password: str = "") -> bool:
        if jq.is_auth():
            return True

        if username and password:
            self.username = username
            self.password = password

        if not self.username or not self.password:
            return False

        try:
            jq.auth(username, password)
            df = jq.get_all_securities(date=datetime.today())
            self.symbols = df.index.values
        except RuntimeError:
            # TODO: logging here
            return False

        self.inited = True
        return True

    @staticmethod
    def to_jq_symbol(symbol: str, exchange: Exchange) -> str:
        """
        CZCE product of RQData has symbol like "TA1905" while
        vt symbol is "TA905.CZCE" so need to add "1" in symbol.
        """
        jq_exchange = EX_VT2JQ_DICT.get(exchange, exchange.value)
        if exchange == Exchange.CZCE:
            # 郑商所 的合约代码年份只有三位 需要特殊处理
            idx = 0
            for idx, word in enumerate(symbol):
                if word.isdigit():
                    break

            # Check for index symbol
            time_str = symbol[idx:]
            if time_str in ["88", "888", "99", "8888"]:
                return symbol

            product = symbol[:idx]
            year = symbol[idx]
            month = symbol[idx + 1:]
            year = "1" + year if year == "9" else "2" + year
            jq_symbol = f"{product}{year}{month}.{jq_exchange}"
        else:
            jq_symbol = f"{symbol}.{jq_exchange}"

        return jq_symbol.upper()

    def query_history(self, req: HistoryRequest) -> Optional[List[BarData]]:
        """
        Query history bar data from JQDataSDK.
        """
        symbol = req.symbol
        exchange = req.exchange
        interval = req.interval
        start = req.start
        end = req.end

        jq_symbol = self.to_jq_symbol(symbol, exchange)
        if jq_symbol not in self.symbols:
            return None

        jq_interval = INTERVAL_VT2RQ.get(interval)
        if not jq_interval:
            return None

        # For adjust timestamp from bar close point (RQData) to open point (VN Trader)
        adjustment = INTERVAL_ADJUSTMENT_MAP[interval]

        # For querying night trading period data
        now = datetime.now()
        end = now if end >= now or (end.year == now.year and end.month == now.month and end.day == now.day) else end

        # Only query open interest for futures contract
        fields = ["open", "high", "low", "close", "volume"]
        if not symbol.isdigit():
            fields.append("open_interest")

        df = jq.get_price(
            jq_symbol,
            frequency=jq_interval,
            fields=fields,
            start_date=start,
            end_date=end,
            skip_paused=True
        )

        if df is None:
            return []

        return [BarData(
                symbol=symbol,
                exchange=exchange,
                interval=interval,
                datetime=row.name.to_pydatetime() - adjustment,
                open_price=row["open"],
                high_price=row["high"],
                low_price=row["low"],
                close_price=row["close"],
                volume=row["volume"],
                open_interest=row.get("open_interest", 0),
                gateway_name="JQ") for _, row in df.iterrows()]


class VtDataClient:
    def __init__(self):
        u, p = SETTINGS["rqdata.username"], SETTINGS["rqdata.password"]
        self.instance = RqdataClient() if (u and u != '') and (p and p != '') else JqdataClient()


# rqdata_client = RqdataClient()
rqdata_client = VtDataClient().instance


