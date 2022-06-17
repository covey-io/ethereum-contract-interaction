from dotenv import load_dotenv
import os
from web3 import Web3
import eth_keys
from eth_account import account
from web3.middleware import geth_poa_middleware
import json
import pandas as pd
import requests
from datetime import datetime,timedelta, timezone
import time
import pytz
from alpaca_trade_api.rest import REST, TimeFrame, TimeFrameUnit
import numpy as np

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


# checking if string is numeric
# https://stackoverflow.com/questions/354038/how-do-i-check-if-a-string-is-a-number-float
def is_number_repl_isdigit(s):
    """ Returns True is string is a number. """
    try:
        return s.lstrip("-").replace('.', '', 1).isdigit()
    except AttributeError:
        return False

def cleanup_trade_cells(s):
    return s if len(s) > 0 else 'BLANK:0'

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

# #
# def get_prices(symbols, exactDate):
#     # docs:  '''https://iexcloud.io/docs/api/#historical-prices'''
#     root_url = 'https://cloud.iexapis.com/stable/stock/market/batch?symbols='
#     method = '&types=intraday-prices&'
#     exactDate = pd.to_datetime(exactDate, infer_datetime_format=True)
#     exactDate = exactDate.strftime('%Y%m%d')
#     quotes = dict()
#     for xOf100 in range(0, len(symbols), 100):
#         urlVersion = symbols[xOf100:xOf100 + 100]
#         urlVersion = ",".join(urlVersion)
#         url = root_url + urlVersion + method + '&token=' + IEX_TOKEN + '&exactDate=' + exactDate
#         data = json.loads(requests.get(url).text)
#         quotes.update(data)
#     return quotes


def get_delayed_trade_calendar(trade_key):
    # get min trade key date
    start_date = pd.to_datetime(trade_key['date_time']).dt.date.min()

    # alpaca calendar to denote business days
    delayed_trade_date_time_df = pd.DataFrame({'delayed_trade_date_time':
                                                   [datetime.combine(x.date, x.open).astimezone(pytz.utc).replace(
                                                       tzinfo=None)
                                                    for x in api.get_calendar(start_date)]})

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

    # backfilling empty merge results so that the next business day propgates backwards for non business days
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

# custom get bars function to return alpacas results into a dataframe so we can use it in our apply in the generate price key
def get_bars(df):

    # if converting a group of dates, then we at the .dt. before the strftime
    day = df['delayed_trade_date'].dt.strftime('%Y-%m-%d')

    try:
        price_df = api.get_bars(df['symbol'], TimeFrame(1, TimeFrameUnit.Minute),day,day
                        ,adjustment='raw').df.reset_index()
        # strip timezone off the time stamp since everything will be in UTC anyhow
        price_df['timestamp'] = price_df['timestamp'].apply(lambda x : x.replace(tzinfo=None))

        # pseudo re create delayed trade date since we used it to join back to the trades table
        price_df['delayed_trade_date'] = price_df['timestamp'].dt.strftime('%Y-%m-%d')

        return price_df[['symbol','delayed_trade_date','timestamp','close']]

    except:
        return pd.DataFrame({'symbol': df['symbol'], 'delayed_trade_date' : [datetime(1970,1,1)],
                             'timestamp':[datetime(1970,1,1)],'close': [0]})

# creating a price history for each ticker, based off the min and max dates of their appearance,
def generate_price_key(symbol_dates_df):
    # copy so as to not modify
    df = symbol_dates_df

    # name the index to group by on for ease
    df.index.name ='ID'

    # get the bars for each row
    df_expanded = df.groupby('ID',group_keys=False).apply(get_bars)


    return df_expanded

# calling the alpaca api (keys are in the .env file it picks it up automatically
# going line by line in trading key, and isolating the five minute post trade window to return the avg close price
# def calc_avg_entry_by_ticker(df):
#
#     df['date_time'] = pd.to_datetime(df['date_time'])
#     symbol = df['symbol']
#     print(symbol)
#     day = df['date_time'].strftime('%Y-%m-%d')
#
#     try:
#         bars_df = api.get_bars(symbol, TimeFrame(1, TimeFrameUnit.Minute), day, day, adjustment='raw').df.reset_index()
#     # assuming here that it was a ticker with no results, i.e. a currency cross
#     except:
#         return -888
#
#     bars_df['timestamp_no_tz'] = bars_df['timestamp'].apply(lambda x : x.replace(tzinfo =None))
#     bars_df['max_time'] = df['date_time'] + timedelta(minutes=5)
#     bars_df['min_time'] = df['date_time']
#     bars_df_date_mask = (bars_df['timestamp_no_tz'] >= bars_df['min_time']) & (bars_df['timestamp_no_tz'] <= bars_df['max_time'])
#     bars_df = bars_df[bars_df_date_mask]
#     #print(bars_df)
#     try:
#         return bars_df['close'].mean()
#     except KeyError:
#         return -999

def calculate_portfolio(address, startCash):
    trades = view_trades(address)

    portfolio = pd.DataFrame(columns=["date_time", "user_id", "cash", "usd_value", "positions_usd",
                                      "inception_return", "gross_exposure_usd", "long_exposure_usd",
                                      "short_exposure_usd", "net_exposure_usd", "gross_exposure_percent",
                                      "long_exposure_percent", "short_exposure_percent", "net_exposure_percent",
                                      "gross_traded_usd", "net_traded_usd", "gross_traded_percent",
                                      "net_traded_percent", "unrealized_long_pnl", "unrealized_short_pnl",
                                      "unrealized_pnl",
                                      "realized_long_pnl", "realized_short_pnl", "realized_pnl",
                                      "total_long_pnl", "total_short_pnl", "total_pnl"])

    firstRow = {"date_time": firstTrade, "user_id": address, "cash": startCash, "usd_value": startCash,
                "positions_usd": 0,
                "inception_return": 1.0, "gross_exposure_usd": 0.0, "long_exposure_usd": 0.0,
                "short_exposure_usd": 0.0, "net_exposure_usd": 0.0, "gross_exposure_percent": 0.0,
                "long_exposure_percent": 0.0, "short_exposure_percent": 0.0, "net_exposure_percent": 0.0,
                "gross_traded_usd": 0.0, "net_traded_usd": 0.0, "gross_traded_percent": 0.0,
                "net_traded_percent": 0.0, "unrealized_long_pnl": 0.0, "unrealized_short_pnl": 0.0,
                "unrealized_pnl": 0.0,
                "realized_long_pnl": 0.0, "realized_short_pnl": 0.0, "realized_pnl": 0.0,
                "total_long_pnl": 0.0, "total_short_pnl": 0.0, "total_pnl": 0.0}

    ''' CALCULATE PORTFOLIO HERE '''

    return portfolio


# view_trades_skale('0x211fe601e24ce89cb443356f687c67fbf7708412')
# view_trades_polygon('0x0d97A0E7e42eB70d013a2a94179cEa0E815dAE41')
# get_prices(['FB'],'2022-03-29')

start_time = time.time()

# wallets_df = pd.read_csv('data/allWallets.csv')
# wallets_df = pd.read_csv('data/wallet_neg_sign_test.csv')
wallets_df = pd.read_csv('data/most_active_wallets.csv')
wallets_df = wallets_df.iloc[:20,:]

trading_key = wallets_df.groupby('eth_cust_address').apply(get_trades_double_chain).reset_index()

# set the index to be the ID
trading_key['trade_id'] = [x for x in range(1, len(trading_key.values)+1)]

symbol_day_key = generate_symbol_day_key(trading_key)
#symbol_day_key.to_csv('symbol_day_key.csv', index=False)

# use the calendar key to get delayed_trade_date, and delayed_trade_date_time
calendar_key = get_delayed_trade_calendar(trading_key)

# add date only column for the merge
trading_key['date'] = pd.to_datetime(trading_key['date_time']).dt.date

# conversion for merge
trading_key['date'] = pd.to_datetime(trading_key['date'])

# merge trading key with calendar key on date
trading_key =pd.merge(left =trading_key, right=calendar_key, how = 'inner', on='date')

# add the 5 min threshold for the averaging of the price, unless of course the trade date not equal delayed_trade date
# in which case the trade happend on a non business day and we set the time window to just be that delayed_trade_date_time
trading_key['max_date_time'] = trading_key.apply(lambda x : x['delayed_trade_date_time'] if x['date'] != x['delayed_trade_date']
                                                 else x['date_time'] + timedelta(minutes=10), axis = 1)

# generate price key
price_key = generate_price_key(symbol_day_key)

# conversion for merge
price_key['delayed_trade_date'] = pd.to_datetime(price_key['delayed_trade_date'])

# make a copy for final output
trading_key_final = trading_key.copy(deep=True)

trading_key = pd.merge(left = trading_key, right = price_key, how='inner', on=['symbol','delayed_trade_date'])

# filter trading key to have trade timestamps fall between date_time and max_date_time
trading_key_mask = (trading_key['timestamp'] >= trading_key['date_time']) & (trading_key['timestamp'] <= trading_key['max_date_time'])
trading_key = trading_key[trading_key_mask]

# take the average close price grouped on the initial index (ID), which we will set
avg_price = trading_key[['trade_id','close']].groupby('trade_id').mean()
avg_price.rename(columns={"close": "entry_price"}, inplace=True)

# merge back to trading key
trading_key_final = pd.merge(left = trading_key_final, right = avg_price, how='inner', on=['trade_id'])

trading_key_final.to_csv('output/trading_key_' + datetime.now().strftime("%m_%d_%Y_%H_%M_%S") + '.csv', index = False)


print("--- %s seconds ---" % (time.time() - start_time))
