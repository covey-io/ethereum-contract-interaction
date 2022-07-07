import sys
import time
import asyncio
import pandas as pd
import nest_asyncio
from enum import Enum
import alpaca_trade_api as tradeapi
from alpaca_trade_api.rest import TimeFrame, URL
from alpaca_trade_api.rest_async import  gather_with_concurrency, AsyncRest
from dotenv import load_dotenv

NY = 'America/New_York'

class DataType(str, Enum):
    Bars = "Bars"
    Trades = "Trades"
    Quotes = "Quotes"

class Pricer():

    def __init__(self, **kwargs):
        # in case running with nested async CLIs (i.e. Jupyter Notebook, Pycharm IDE (cough))
        nest_asyncio.apply()

        # load environment variables (aplaca private and public keys)
        load_dotenv()

        # initialize the 'df' variable, the data frame with final output depending on what we ask (prices, quotes)
        self.df = pd.DataFrame(columns=['symbol','timestamp','vwap'])

        # initiate the async_rest class, which will initialize the keys, no params is fine it will find the keys
        # if they are in the environment (hence we did the load_dotenv())
        self.rest = AsyncRest()

        # set the start date (if not provided)
        self.start = kwargs.get('start',pd.Timestamp('2021-12-31', tz=None).date().isoformat())

        # set the end date (if not provided) - None defaults to latest possible date
        self.end = kwargs.get('end', pd.Timestamp('2022-07-05', tz=None).date().isoformat())

        # US Equity Tickers
        self.us_equity_symbols = kwargs.get('us_equity_symbols',['ARKK'])

        # Crypto Tickers
        self.crypto_symbols = kwargs.get('crypto_symbols',['SKLUSD'])

        # set the time frame - default to Hour
        self.timeframe = kwargs.get('timeframe',TimeFrame.Hour)


    def get_data_method(self,data_type: DataType):
        if data_type == DataType.Bars:
            return self.rest.get_bars_async
        elif data_type == DataType.Trades:
            return self.rest.get_trades_async
        elif data_type == DataType.Quotes:
            return self.rest.get_quotes_async
        else:
            raise Exception(f"Unsupoported data type: {data_type}")


    async def get_historic_data_base(self,symbols, data_type: DataType, start, end,
                                     timeframe: TimeFrame = None, asset_type : str = 'stock'):
        """
        base function to use with all
        :param symbols:
        :param start:
        :param end:
        :param timeframe:
        :return:
        """
        major = sys.version_info.major
        minor = sys.version_info.minor
        if major < 3 or minor < 6:
            raise Exception('asyncio is not support in your python version')
        msg = f"Getting {data_type} data for {len(symbols)} symbols"
        msg += f", timeframe: {timeframe}" if timeframe else ""
        msg += f" between dates: start={start}, end={end}"
        print(msg)
        step_size = 1000
        results = []
        for i in range(0, len(symbols), step_size):
            tasks = []
            for symbol in symbols[i:i+step_size]:
                args = [symbol, start, end, timeframe.value, asset_type] if timeframe else \
                    [symbol, start, end,asset_type]
                tasks.append(self.get_data_method(data_type)(*args))

            if minor >= 8:
                results.extend(await asyncio.gather(*tasks, return_exceptions=True))
            else:
                results.extend(await gather_with_concurrency(500, *tasks))

        bad_requests = 0
        for response in results:
            if isinstance(response, Exception):
                print(f"Got an error: {response}")

            elif not len(response[1]):
                bad_requests += 1

            # append to main price data frame
            if len(response[1].index) == 0:
                response_df = pd.DataFrame(columns=['symbol','timestamp','vwap'])
            else:
                response_df = response[1].reset_index()
                response_df['symbol'] = response[0]

            self.df = pd.concat([self.df,response_df[['symbol','timestamp','vwap']]])

        #print(results)
        #print(f"Total of {len(results)} {data_type}, and {bad_requests} "
               #f"empty responses.")


    async def get_historic_bars(self,symbols, start, end, timeframe: TimeFrame, asset_type : str = 'stock'):
        await self.get_historic_data_base(symbols, DataType.Bars, start, end, timeframe, asset_type)


    async def fill_price_data(self):
        await self.get_historic_bars(self.us_equity_symbols, self.start, self.end, self.timeframe)
        await self.get_historic_bars(self.crypto_symbols, self.start, self.end, self.timeframe,'crypto')



if __name__ == '__main__':
    start_time = time.time()
    # in case running with nested async CLIs (i.e. Jupyter Notebook, Pycharm IDE (cough))
    nest_asyncio.apply()

    # load environment variables (aplaca private and public keys)
    load_dotenv()

    # initialize alpaca rest library, for async of course
    rest = AsyncRest()

    # initialize 'the pricer' (naming done by VS it may not be his best work, open to changing!)
    # the class has default variables for required start,end,tickers,timeframe so we don't need to pass anything to test
    p = Pricer()

    # fill the dataframe of prices
    loop = asyncio.get_event_loop()
    loop.run_until_complete(p.fill_price_data())

    # check the dataframe
    print(p.df)

    print(f"took {time.time() - start_time} sec")