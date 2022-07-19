import sys
import time
import asyncio
import pandas as pd
import nest_asyncio
from enum import Enum
from datetime import datetime
from dotenv import load_dotenv
import alpaca_trade_api as tradeapi
from alpaca_trade_api.rest import TimeFrame, URL
from alpaca_trade_api.rest_async import  gather_with_concurrency, AsyncRest



NY = 'America/New_York'

class DataType(str, Enum):
    Bars = "Bars"
    Trades = "Trades"
    Quotes = "Quotes"

class Pricer:
    def __init__(self, **kwargs):
        # in case running with nested async CLIs (i.e. Jupyter Notebook, Pycharm IDE (cough))
        nest_asyncio.apply()

        # initialize the 'df' variable, the data frame with final output depending on what we ask (prices, quotes)
        self.df = pd.DataFrame(columns=['symbol','timestamp','vwap'])

        # load environment variables (aplaca private and public keys)
        load_dotenv()

        # initiate the async_rest class, which will initialize the keys, no params is fine it will find the keys
        # if they are in the environment (hence we did the load_dotenv())
        self.rest = AsyncRest()

        # set the start date (if not provided)
        self.start = kwargs.get('start',pd.Timestamp('2022-07-05', tz=None).date().isoformat())

        # set the end date (if not provided) - None defaults to latest possible date
        self.end = kwargs.get('end', pd.Timestamp(datetime.now().strftime('%Y-%m-%d'),
                                                  tz=None).date().isoformat())

        # set the time frame - default to Hour
        self.timeframe = kwargs.get('timeframe',TimeFrame.Hour)

        # the entire symbol list
        self.symbols = kwargs.get('symbols', ['AAPL', 'ETHUSDT'])

        # hard crypto exclusions
        self.crypto_exclusions = kwargs.get('crypto_exclusions', ['1INCHUSDT'])

        # hard us equity exclusions - no exclusions by default
        self.us_equity_exclusions = kwargs.get('us_equity_exclusions', [])

        # US Equity Tickers
        self.us_equity_symbols = self.get_us_equity_symbols()

        # Crypto Tickers
        self.crypto_symbols = self.get_crypto_symbols()
        print(self.crypto_symbols)

        # generate price key
        self.price_key = self.get_price_key()


    def get_us_equity_symbols(self):
        return [e for e in self.symbols if not e.endswith('USDT') and e not in self.us_equity_exclusions]


    def get_crypto_symbols(self):
        return [c.replace('USDT', 'USD') for c in self.symbols if c.endswith('USDT')
                and c not in self.crypto_exclusions]


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


    def get_price_key(self):
        # start the timer
        start_time = time.time()

        # fill the dataframe of prices
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.fill_price_data())

        # make a copy of original df
        price_key = self.df.copy()

        # conversion for future date operations
        price_key['timestamp'] = pd.to_datetime(price_key['timestamp'])

        # remove time zone awareness
        price_key['timestamp'] = price_key['timestamp'].dt.tz_localize(None)

        # add in the delayed trade date column for future joining
        price_key['delayed_trade_date'] = price_key['timestamp'].dt.strftime('%Y-%m-%d')

        # conversion for merge
        price_key['delayed_trade_date'] = pd.to_datetime(price_key['delayed_trade_date'])

        print('Price Key finished in {} seconds'.format(time.time() - start_time))

        return price_key



if __name__ == '__main__':
    start_time = time.time()

    # initialize 'the pricer' (naming done by VS it may not be his best work, open to changing!)
    # the class has default variables for required start,end,tickers,timeframe so we don't need to pass anything to test
    p = Pricer(symbols = ['AAPL','GOOGL','IBM'])

    print(p.price_key)

    print(f"took {time.time() - start_time} sec")