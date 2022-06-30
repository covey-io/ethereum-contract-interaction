import os
import json
import time
import pytz
import logging
import asyncio
import pandas as pd
import nest_asyncio
from web3 import Web3
from pricer import Pricer
from typing import Optional
from dotenv import load_dotenv
from alpaca_trade_api.rest import REST
from datetime import datetime,timedelta
from web3.middleware import geth_poa_middleware

# so that everything including the info will print
#logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

# load environment variables (used below) that live in the .env file at the root of this project (
# ethereum-contract-interaction)
load_dotenv()

# environment variables, pulled from the .env file
WALLET = os.getenv('WALLET')
INFURA_PROJECT_ID = os.getenv('INFURA_PROJECT_ID')
INFURA_URL = os.getenv('INFURA_URL')
POLYGON_CHAIN_ID = os.getenv('POLYGON_CHAIN_ID')
COVEY_LEDGER_POLYGON_ADDRESS = os.getenv('COVEY_LEDGER_POLYGON_ADDRESS')
COVEY_LEDGER_SKALE_ADDRESS = os.getenv('COVEY_LEDGER_SKALE_ADDRESS')
SKALE_URL = os.getenv('SKALE_URL')
IEX_TOKEN = os.getenv('IEX_TOKEN')

# initialize alpaca
api = REST()

# Opening JSON file
f = open('CoveyLedger.json')

# returns JSON object as a dictionary
ledger_info = json.load(f)

# check if string is numeric : https://stackoverflow.com/questions/354038/how-do-i-check-if-a-string-is-a-number-float
def is_number_repl_isdigit(s):
    """ Returns True is string is a number. """
    try:
        return s.lstrip("-").replace('.', '', 1).isdigit()
    except AttributeError:
        return False

# add BLANK ticker, 0 Target Position for wallets with no activity
def cleanup_trade_cells(s):
    return s if len(s) > 0 else 'BLANK:0'

# output format [('address', 'position string', unix time),('address', 'position string', unix time),...]
def view_trades_skale(address):
    w3 = Web3(Web3.HTTPProvider(SKALE_URL))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    covey_ledger = w3.eth.contract(address=COVEY_LEDGER_SKALE_ADDRESS, abi=ledger_info['abi'])
    my_address = w3.toChecksumAddress(address)
    result = covey_ledger.functions.getAnalystContent(my_address).call()
    return result

# output format [('address', 'position string', unix time),('address', 'position string', unix time),...]
def view_trades_polygon(address):
    w3 = Web3(Web3.HTTPProvider(f'{INFURA_URL}/{INFURA_PROJECT_ID}'))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    covey_ledger = w3.eth.contract(address=COVEY_LEDGER_POLYGON_ADDRESS, abi=ledger_info['abi'])
    my_address = w3.toChecksumAddress(address)
    result = covey_ledger.functions.getAnalystContent(my_address).call()
    return result

# scalable function to stack transactions (trades) from multiple chains
def get_trades_double_chain(df):
    # isolate address
    address = df['eth_cust_address'].unique()[0]

    # grab the trades
    result_skl = view_trades_skale(address)
    result_poly = view_trades_polygon(address)

    # result list of tuples, output format [('address', 'position string', unix time),('address', 'position string',
    # unix time),...], to dataframe
    result_skl_df = pd.DataFrame(result_skl, columns=['address', 'trades', 'date_time'])
    result_skl_df.insert(0,'chain','SKL')
    result_poly_df = pd.DataFrame(result_poly, columns=['address', 'trades', 'date_time'])
    result_poly_df.insert(0, 'chain', 'MATIC')

    # concatenate the separate chain dataframes into one
    result_df = pd.concat([result_skl_df,result_poly_df])

    # convert unix time to datetime
    result_df['date_time'] = pd.to_datetime(result_df['date_time'], unit='s')

    # split trades column into multiple rows by delimiter, resulting in each row having one ticker : position combo
    result_df = result_df.assign(trades=result_df['trades'].str.split(',')).explode('trades')

    # clean up blank trade cells, empty string
    result_df['trades'] = result_df['trades'].apply(lambda x: cleanup_trade_cells(x))

    # split trades column into symbol, position columns
    try:
        result_df[['symbol', 'target_position_value']] = result_df['trades'].str.split(':', expand=True).iloc[:, 0:2]
    except ValueError:
        result_df[['symbol', 'target_position_value']] = ['BLANK',0]

    # clean up the covey-reset, the target_position value should be numeric
    result_df['target_position_value'] = result_df['target_position_value'].apply(
        lambda x: x if is_number_repl_isdigit(x) else 0)

    # return result
    return result_df[['chain','address', 'symbol', 'target_position_value', 'date_time']].set_index('address')

# deriving next business day logic utilizing alpacas calendar api
def get_delayed_trade_calendar(trade_key):
    # get min trade key date
    start_date = pd.to_datetime(trade_key['date_time']).dt.date.min()

    # alpaca calendar to denote business days
    delayed_trade_date_time_df = pd.DataFrame({'delayed_trade_date_time':
                                                   [datetime.combine(x.date, x.open).astimezone(pytz.utc).replace(
                                                       tzinfo=None)
                                                    for x in api.get_calendar(start_date)],
        'delayed_trade_date_closing_time':
            [datetime.combine(x.date, x.close).astimezone(pytz.utc).replace(
                tzinfo=None)
                for x in api.get_calendar(start_date)]

    })

    # agnostic date range of all days between min trade key date and max alpaca business date
    date_df = pd.DataFrame({'date': pd.date_range(start=start_date,
                                                  end=delayed_trade_date_time_df['delayed_trade_date_time'].max())})

    # conversion for merging
    delayed_trade_date_time_df['delayed_trade_date'] = \
        pd.to_datetime(delayed_trade_date_time_df['delayed_trade_date_time']).dt.date

    # conversion for merging
    delayed_trade_date_time_df['delayed_trade_date'] = pd.to_datetime(
        delayed_trade_date_time_df['delayed_trade_date'])

    # merging agnostic dates with business dates
    bus_date_key_df = pd.merge(left=date_df, right=delayed_trade_date_time_df,
                               how='left', left_on='date',
                               right_on='delayed_trade_date')

    # back-filling empty merge results so that the next business day propgates backwards for non business days
    bus_date_key_df.fillna(method='bfill', inplace=True)
    bus_date_key_df.dropna(inplace=True)

    return bus_date_key_df

# attempt to condense the trades into tickers and dates so that we don't have to call the alpaca api extra times
def generate_symbol_day_key(trade_key):
    # make a copy to not modify the original
    df = trade_key

    # isolate date portion of date_time
    df['date'] = pd.to_datetime(df['date_time']).dt.date

    # conversion for merge
    df['date'] = pd.to_datetime(df['date'])

    # generate business date key to join on, to grab the delayed_trade_date,
    # the delayed trade date will be the one that has prices provided by alpaca, since it is a business day
    bus_date_key_df = get_delayed_trade_calendar(df)

    # merge on bus date key, with df, on 'date'
    merge_df = pd.merge(left = df, right = bus_date_key_df, on = 'date', how = 'inner')

    # unique combinations of [symbol | date | delayed_trade_date]
    symbol_dates_df = merge_df.groupby(['symbol','date','delayed_trade_date']).size().reset_index().rename(columns={0:'count'})

    return symbol_dates_df

# generating the price key using the custom 'pricer' module that uses async methodology on top of alpaca library
def generate_price_key(symbol_dates_df):
    # copy so as to not modify
    df = symbol_dates_df

    global_start_date = df['delayed_trade_date'].min().strftime('%Y-%m-%d')
    global_end_date = '2022-06-23' #df['delayed_trade_date'].max().strftime('%Y-%m-%d')

    # get unique set of tickers
    tickers = df['symbol'].unique()

    hard_crypto_exclusions = ['1INCHUSDT']
    crypto_tickers = [c for c in tickers if c.endswith('USDT') and c not in hard_crypto_exclusions]

    #replace the USDT with USD
    crypto_tickers = [c.replace('USDT','USD') for c in crypto_tickers]

    hard_equity_exclusions = []
    equity_tickers = [e for e in tickers if not e.endswith('USDT') and e not in hard_equity_exclusions]

    # initialize pricer class (custom class built on top of rest_async library from alpaca
    p = Pricer(start = global_start_date, end = global_end_date, us_equity_symbols= equity_tickers,
               crypto_symbols = crypto_tickers)

    # fill the dataframe of prices
    loop = asyncio.get_event_loop()
    loop.run_until_complete(p.fill_price_data())

    # get the price dataframe
    price_df = p.df

    # conversion for future date operations
    price_df['timestamp'] = pd.to_datetime(price_df['timestamp'])

    # add in the delayed trade date column for future joining
    price_df['delayed_trade_date'] = price_df['timestamp'].dt.strftime('%Y-%m-%d')

    return price_df

# for debugging sets the reference time as of which we look at prices starting from that time
def set_ref_trade_date_time(row):
    date_cols_to_convert = ['date_time','date','delayed_trade_date',
                            'delayed_trade_date_time','delayed_trade_date_closing_time','max_time_stamp']
    # adjust it to be the next hour where we will take the VWAP of that hour
    row[date_cols_to_convert] = pd.to_datetime(row[date_cols_to_convert], utc=True)
    trade_date_time_adj = row['date_time'] + timedelta(minutes=61)
    trade_date_time_adj = trade_date_time_adj.replace(minute=0, second=0)

    # trade on holiday or non business day, return the next possible
    if row['date'] != row['delayed_trade_date']:
        new_dt = row['delayed_trade_date_time'] + timedelta(minutes=61)
    # trade during pre-market hours but still on a business day
    elif trade_date_time_adj < row['delayed_trade_date_time']:
        new_dt = row['delayed_trade_date_time'] + timedelta(minutes=61)
    # trade during post-market hours but still on a business day
    elif trade_date_time_adj > row['delayed_trade_date_closing_time']:
        new_dt = row['delayed_trade_date_closing_time']
    else:
        new_dt = trade_date_time_adj

    # price history doesn't go all the way up to the floor timestamp (delayed trade time adj)
    if row['max_time_stamp'] < trade_date_time_adj:
        new_dt = row['max_time_stamp']

    return new_dt.replace(minute=0, second=0)

# a check to see which crypto tickers that should be priced, that have not been
def check_crypto_tickers(trading_key):
    # crypto ticker check for alpaca
    alpaca_crypto_tickers = ['AAVEUSD','ALGOUSD','BATUSD','BTCUSD ','BCHUSD ','LINKUSD ','DAIUSD ',
                             'DOGEUSD ','ETHUSD ','GRTUSD ','LTCUSD ','MKRUSD ','MATICUSD ','NEARUSD ',
                             'PAXGUSD ','SHIBUSD ','SOLUSD ','SUSHIUSD ','USDTUSD ','TRXUSD ','UNIUSD ',
                             'WBTCUSD ','YFIUSD ']

    # go over the trading key and check if it's crypto, if it's in the alpaca list, and if it doesn't have a price
    # copy to not modify
    df = trading_key

    # mask for tickers desired
    df =df[df['symbol'].isin(alpaca_crypto_tickers)]

    # find the NaN prices
    df = df[df['vwap'].isnull()]
    return df

# generates 'entry_price' and 'market_entry_date' (aka delayed_trade_date) for the trading_key based off wallets inputed
def generate_trades_with_prices(address: Optional[str] = None):

    if address is not None:
        wallets_df = pd.DataFrame({'eth_cust_address' : address}, index = [0])
    else:
        wallets_df = pd.read_csv('data/allWallets.csv')

    # get the trades per wallet
    trading_key = wallets_df.groupby('eth_cust_address').apply(get_trades_double_chain).reset_index()

    # set the index to be the ID
    trading_key['trade_id'] = [x for x in range(1, len(trading_key.values)+1)]

    # for sanity checking
    pre_price_row_count = len(trading_key.index)

    # unique combination of tickers and trade dates those tickers were traded on
    symbol_day_key = generate_symbol_day_key(trading_key)

    # use the calendar key to get delayed_trade_date, and delayed_trade_date_time
    calendar_key = get_delayed_trade_calendar(trading_key)

    # add date only column for the merge
    trading_key['date'] = pd.to_datetime(trading_key['date_time']).dt.date

    # conversion for merge
    trading_key['date'] = pd.to_datetime(trading_key['date'])

    # merge trading key with calendar key on date
    trading_key = pd.merge(left = trading_key, right=calendar_key, how = 'inner', on='date')

    # clean up crypto tokens ending is USDT
    trading_key['symbol'] = trading_key['symbol'].apply(lambda x : x.replace('USDT','USD'))

    # generate price key
    price_key = generate_price_key(symbol_day_key)

    # conversion for merge
    price_key['delayed_trade_date'] = pd.to_datetime(price_key['delayed_trade_date'])
    price_key.to_csv('price_key.csv')
    print('Price Key finished in {} seconds'.format(time.time() - start_time))

    trading_key = pd.merge(left = trading_key, right = price_key, how='left',
                           left_on=['symbol','delayed_trade_date'], right_on=['symbol','delayed_trade_date'])


    # set max timestamp of prices per trade id, just in case it doesn't show all history between expected open and close times
    # aka RUSL
    trading_key['max_time_stamp'] = trading_key.groupby('trade_id')['timestamp'].transform('max')

    # set the reference trade date time - adjusting for pre market and post market trade times
    trading_key['date_time_adj'] = trading_key.apply(lambda x : set_ref_trade_date_time(x), axis = 1)

    # fill in the blank timestamps (no prices returned for that ticker/date) so that it won't be filtered out in the next step
    # we want to see the un-priced items
    trading_key['timestamp'].fillna(trading_key['date_time_adj'], inplace=True)

    # filter trading key to have trade timestamps fall after the date time adj threshold
    trading_key_mask = (trading_key['timestamp'] >= trading_key['date_time_adj'])
    trading_key = trading_key[trading_key_mask]

    # add a ranking to grab the next hours only price
    trading_key['price_rank'] = trading_key.groupby('trade_id')['timestamp'].rank('dense', ascending=True)
    trading_key = trading_key[trading_key['price_rank'] == 1]

    # don't need price rank anymore
    trading_key.drop(columns = ['price_rank'], inplace=True)

    # update the CoveyReset vwap prices to 0
    crypto_check_df = check_crypto_tickers(trading_key)
    crypto_check_df.to_csv('checks/crypto_pricing_' + datetime.now().strftime("%m_%d_%Y_%H_%M_%S") + '.csv', index = False)

    # making sure we did not lose any trades in the price merge
    post_price_row_count = len(trading_key.index)

    assert (pre_price_row_count == post_price_row_count)

    return trading_key, price_key

# portfolio math helpers

def recursive_portfolio_filler(df):

    if df['usd_value_prev'].isnull().values.any():

        #keep_cols = [x + '_x' for x in df.columns]

        # left join to itself on previous day's snapshot
        port = pd.merge(left=df, right=df, how='left', left_on=['symbol','dta_rank_prev'],
                        right_on=['symbol','dta_rank'])
        port['usd_value_prev_x'] = port.apply(lambda x : x['usd_value_y']
                                                if pd.isnull(x['usd_value_prev_x'])
                                              else x['usd_value_prev_x'], axis=1)


        port = port[port.columns[~port.columns.str.endswith('_y')]]

        # rename back to the original column names so we can work with them
        df = port.rename(columns=lambda x: x.replace('_x',''))

        # drop duplicates that we get on portfolio (non ticker level) joins
        df = df.drop_duplicates()

        # positions target amount in USD = [previous portfolio value (usd_value_prev) * target_position]
        df['positions_usd'] = df['target_position_value'].astype(float) * df['usd_value_prev'].astype(float)

        # present cash value of portfolio, sum of positions_usd per snapshot (depends on what we classif as granularity
        # right now it is by date_time_adjusted (aka market entry date)
        df['usd_value'] = df.groupby('dta_rank')['positions_usd'].transform('sum')

        return recursive_portfolio_filler(df)

    else :

        return df


# portfolio math
def calculate_portfolio(address, startCash):
    # grab the trades, prices
    trades, prices = generate_trades_with_prices(address)

    # get the earliest trade date, and trade_date_time
    entry_date = trades['delayed_trade_date'].min() + timedelta(days=-1)
    entry_date_time = trades.iloc[0,11].replace(minute=0, second=0) + timedelta(days=-1)

    # initialize the portfolio
    firstRow = {"delayed_trade_date": entry_date, "user_id": address, "cash": startCash, "usd_value": startCash,
                "usd_value_prev" :0 ,
                "positions_usd": 0,"inception_return": 0, "gross_exposure_usd": 0.0, "long_exposure_usd": 0.0,
                "short_exposure_usd": 0.0, "net_exposure_usd": 0.0, "gross_exposure_percent": 0.0,
                "long_exposure_percent": 0.0, "short_exposure_percent": 0.0, "net_exposure_percent": 0.0,
                "gross_traded_usd": 0.0, "net_traded_usd": 0.0, "gross_traded_percent": 0.0,
                "net_traded_percent": 0.0, "unrealized_long_pnl": 0.0, "unrealized_short_pnl": 0.0,
                "unrealized_pnl": 0.0,"realized_long_pnl": 0.0, "realized_short_pnl": 0.0, "realized_pnl": 0.0,
                "total_long_pnl": 0.0, "total_short_pnl": 0.0, "total_pnl": 0.0}

    portfolio = pd.DataFrame(firstRow, index = [0])

    # create dummy row for trades, that will join to the first row of the portfolio df
    first_trade_row = {'eth_cust_address' : trades.iloc[0,0], 'address' : trades.iloc[0,1], 'chain' : trades.iloc[0,2],
                       'symbol' : 'CASH',
                       'target_position_value' : 1,
                       'date_time' : trades.iloc[0,5] + timedelta(days = -1),
                       'trade_id' : 0,
                       'date' : trades.iloc[0,7] + timedelta(days = -1),
                       'delayed_trade_date_time' : trades.iloc[0, 8] + timedelta(days=-1),
                       'delayed_trade_date_closing_time' : trades.iloc[0, 9] + timedelta(days=-1),
                       'delayed_trade_date' : entry_date,
                       'timestamp' : entry_date_time,
                       'vwap' : 1,
                       'max_time_stamp' : entry_date_time,
                       'date_time_adj' : entry_date_time
                       }

    trades.loc[-1] = first_trade_row
    trades.index = trades.index + 1  # shifting index
    trades.sort_index(inplace=True)
    trades['date_time'] = pd.to_datetime(trades['date_time'])

    # fill in the empty vwap prices, for now, just so we can calculate in peace
    trades['vwap'].fillna(0,inplace=True)

    # getting the last trade per date_time_adj (since that's the time stamp of the price effectively, we don't need
    # multiple of these per ticker if it is traded daily
    # neat solution : https://stackoverflow.com/questions/37997668/pandas-number-rows-within-group-in-increasing-order
    #trades = trades.sort_values(by = 'trade_id', ascending=False)
    trades['ticker_date_time_rank'] = trades.sort_values(by = 'trade_id', ascending=False).groupby(['symbol', 'date_time_adj']).cumcount()+1

    # filter trades to only have latest ticker (one line) per date_time_adj (DTA)
    # for example in Brooker's portfolio, he had set FB allocations multiple times for DTA : 2022-03-02 21:00:00+00:00
    # no need to have all of those lines since the entry_price is the same (due to same DTA) so no true portfolio MV change
    trades = trades[trades['ticker_date_time_rank'] == 1]

    # left join trades to portfolio
    portfolio = pd.merge(left = trades, right = portfolio ,how = 'outer',
                         left_on= 'delayed_trade_date', right_on= 'delayed_trade_date')

    portfolio = portfolio.rename(columns={'delayed_trade_date': 'market_entry_date',
                                          'date_time_adj': 'market_entry_date_time'})


    # creating the cartesian map or tickers and dates
    tickers = pd.DataFrame(trades['symbol'].drop_duplicates())
    tickers = tickers[tickers['symbol'] != 'CASH']
    dates = trades[['delayed_trade_date','date_time_adj']].drop_duplicates()

    cartesian = tickers.merge(dates, how = 'cross')

    cartesian = cartesian.rename(columns = {'delayed_trade_date' : 'market_entry_date',
                                             'date_time_adj' : 'market_entry_date_time'})

    cartesian = pd.merge(left= cartesian, right = prices[['symbol', 'timestamp', 'vwap']], how = 'left',
                         left_on= ['symbol','market_entry_date_time'],
                         right_on = ['symbol','timestamp'])

    cartesian.groupby('symbol')['vwap'].ffill().bfill()

    cartesian['vwap'] = cartesian.groupby('symbol')['vwap'].transform(lambda  x : x.ffill().bfill().fillna(0))

    cartesian = cartesian.rename(columns={'vwap': 'entry_price'})

    cartesian.drop(columns = 'timestamp', inplace=True)


    # merging the cartesian key with the portfolio

    cartesian_portfolio = pd.merge(left = cartesian,
                                   right = portfolio[['symbol','market_entry_date','market_entry_date_time',
                                                      'target_position_value',
                                                      'cash',
                                                      'usd_value',
                                                      'usd_value_prev',
                                                      'positions_usd'
                                                      ]], how='left',
                                   left_on=['symbol','market_entry_date','market_entry_date_time'],
                                   right_on=['symbol','market_entry_date','market_entry_date_time'])

    # calculation helpers, day_rank and ticker_rank
    cartesian_portfolio['dta_rank'] = cartesian_portfolio['market_entry_date_time'].rank(method='dense', ascending=True) - 1
    cartesian_portfolio['dta_rank_prev'] = cartesian_portfolio['dta_rank'] - 1
    cartesian_portfolio['target_position_value'].fillna(0, inplace=True)

    # initialize the first row
    cartesian_portfolio.loc[cartesian_portfolio['dta_rank'] == 0,['cash','usd_value']] = startCash
    cartesian_portfolio.loc[cartesian_portfolio['dta_rank'] == 0, ['usd_value_prev', 'positions_usd']] = 0
    # recursively fill it out
    cartesian_portfolio = recursive_portfolio_filler(cartesian_portfolio)

    cartesian_portfolio = cartesian_portfolio.sort_values(by='market_entry_date_time', ascending=True)

    cartesian_portfolio.to_csv('output/cartesian_portfolio.csv', index = False)



    return portfolio

if __name__ == '__main__':
    start_time = time.time()
    #result = generate_trades_with_prices()
    #print(result[0]['symbol'].unique())
    # calculate portfolio for particular address
    port_df = calculate_portfolio('0x594F56D21ad544F6B567F3A49DB0F9a7B501FF37',10000)
    print(port_df)
    print("---Trades with prices finished in %s seconds ---" % (time.time() - start_time))
