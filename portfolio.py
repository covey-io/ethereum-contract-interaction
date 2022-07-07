import os
import json
import time
import pytz
import logging
import asyncio
import numpy as np
import pandas as pd
import nest_asyncio
from web3 import Web3
from pricer import Pricer
from typing import Optional
from dotenv import load_dotenv
from alpaca_trade_api.rest import REST
from datetime import datetime,timedelta
from web3.middleware import geth_poa_middleware
import warnings
warnings.filterwarnings("ignore")

# load environment variables (used below) that live in the .env file at the root of this project
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
    #global_end_date = df['delayed_trade_date'].max().strftime('%Y-%m-%d')
    today = datetime.today()
    yesterday = today + timedelta(days = -1)
    global_end_date  = yesterday.strftime('%Y-%m-%d')

    # get unique set of tickers
    tickers = df['symbol'].unique()

    hard_crypto_exclusions = ['1INCHUSDT']
    crypto_tickers = [c for c in tickers if c.endswith('USDT') and c not in hard_crypto_exclusions]

    #replace the USDT with USD
    crypto_tickers = [c.replace('USDT','USD') for c in crypto_tickers]
    print(crypto_tickers)

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

# trade processing for portfolio context id
def process_trade_snapshot(context,cartesian_df):

    # current block/day
    context_df = cartesian_df[cartesian_df['portfolio_context_id'] == context].copy()

    # previous state
    prev_df = cartesian_df[cartesian_df['portfolio_context_id'] ==  context - 1].copy()

    # first true trading block
    if context == 2:
        # get prior portfolio value (postions + cash) - for block 2, aka the first trading block it will be all cash
        prior_usd_value = prev_df['post_cash'].unique()[0]
        # set positions usd based on target position value
        context_df['positions_usd'] = context_df['target_position_value'].astype(float) * prior_usd_value
        # set the prior cash, we know this as the start cash
        context_df['prior_cash'] = prior_usd_value
        # update share counts
        context_df['shares'] = context_df['positions_usd'] / context_df['entry_price']
        # update the cash after trades
        context_df['post_cash'] =  prior_usd_value - context_df['positions_usd'].sum()
        # since it's the first trade block, share will equal post cumulative shares
        context_df['post_cumulative_shares'] = context_df['shares']

    # further trading blocks
    elif context > 2:
        # create the current | previous context frame merges
        context_df = pd.merge(left=context_df.reset_index(), right=prev_df.reset_index(),
                              left_on=['symbol', 'prev_portfolio_context_id'],
                              right_on=['symbol', 'portfolio_context_id'], how='left')
        context_df.columns = context_df.columns.str.rstrip('_x')
        # if there are no trades, just drag everything forward from previous df, except for some columns
        if context_df['target_position_value'].isna().sum() == len(context_df.index):
            #print("No trades for context {}".format(context))
            # share count 0 since we didn't trade anything
            context_df['shares'] = 0
            # post cumulative share count remains the same
            context_df['post_cumulative_shares'] = context_df['post_cumulative_shares_y']
            # no change in cash since no trading activity
            context_df['post_cash'] = context_df['post_cash_y']
            # due to no change in cash, the prior cash now is the post cash of the earlier block
            context_df['prior_cash'] = context_df['post_cash_y']
            # set the prior shares, to be the current shares
            context_df['prior_shares'] = context_df['post_cumulative_shares_y']
            # the positions usd may change since prices have updated
            context_df['positions_usd'] = context_df['post_cumulative_shares'] * context_df['close_price']
            # get what the prior positions were worth right before the current trading block happened
            # this will be helpful for cash adjustments when we resize positions
            context_df['prior_positions_usd'] = context_df['prior_shares'] * context_df['prior_close']

        # we have trades
        else:
            # there are three scenarios that can happen when we have a trade
            # there are three scenarios that can happen when we have a trade
            # 1) Open (target position > 0 && previous share count == 0
            # 2) Size (increase or decrease)
            # 3) Close (target position of 0 && previous share count > 0

            # set the prior shares, the shares in the previous context
            context_df['prior_shares'] = context_df['post_cumulative_shares_y']
            # get what the prior positions were worth right before the current trading block happened
            # this will be helpful for cash adjustments when we resize positions
            context_df['prior_positions_usd'] = context_df.apply(lambda x : x['prior_shares'] * (x['prior_close'] if
                                                                 pd.isnull(x['entry_price']) else x['entry_price'])
                                                                 , axis = 1)
            # prior cash so we can calculated the prior usd value
            prior_cash = context_df['post_cash_y'].unique()[0]
            context_df['prior_cash'] = prior_cash
            # get the previous portfolio value so we can allocated the new trade
            prior_usd_value = context_df['prior_positions_usd'].sum() + prior_cash
            # set the trade that needs an allocation
            context_df['positions_usd'] = context_df.apply(lambda x : (x['prior_shares'] * x['close_price']) if
                                                        pd.isnull(x['target_position_value']) else
            (float(x['target_position_value']) * prior_usd_value)
                                                            , axis = 1)

            # set the new cash amount after all the trading activity
            post_cash_df = context_df.loc[context_df['target_position_value'].notnull()]
            trading_amount = post_cash_df['positions_usd'].sum() - post_cash_df['prior_positions_usd'].sum()
            context_df['post_cash'] = prior_cash - trading_amount

            # update share counts
            context_df['shares'] = context_df.apply(lambda x: 0
                                        if pd.isnull(x['target_position_value'])
                            else (x['positions_usd'] - x['prior_positions_usd']) / x['entry_price'],
                                                    axis =1)

            # summarize the post cumulative share amount (shares traded + prior share count)
            context_df['post_cumulative_shares'] = context_df['shares'] + context_df['prior_shares']


            # print("We have {} trades for context {}".format(len(context_df.index)
            #                                                 - context_df['target_position_value'].isna().sum(),context))

        # remove the _y
        to_drop = [x for x in context_df if x.endswith('_y')]
        context_df = context_df.drop(to_drop, axis = 1)
        context_df.set_index('ID', inplace=True)

    # set the end of day closing portfolio value
    context_df['post_positions_usd'] = context_df['post_cumulative_shares'] * context_df['close_price']

    # long exposure (dollar)
    context_df.loc[:,'long_exposure_usd'] = context_df.apply(lambda x:
                                                       x['post_positions_usd'] if x[
                                                                                      'post_positions_usd'] >= 0 else 0,
                                                       axis=1)
    # short exposure (dollar)
    context_df.loc[:,'short_exposure_usd'] = context_df.apply(lambda x:
                                                        x['post_positions_usd'] if x[
                                                                                       'post_positions_usd'] < 0 else 0,
                                                        axis=1)

    # traded amount
    context_df.loc[:,'long_traded_usd'] = context_df.apply(lambda x:
                                                        x['entry_price'] * x['shares'] if x[
                                                                                    'post_positions_usd'] >= 0 else 0,
                                                        axis=1)
    context_df.loc[:,'short_traded_usd'] = context_df.apply(lambda x:
                                                        x['entry_price'] * x['shares'] if x[
                                                                                    'post_positions_usd'] < 0 else 0,
                                                        axis=1)

    # realized pnl
    context_df.loc[:,'realized_long_pnl'] = context_df.apply(lambda x :     (x['entry_price'] - x['last_known_entry']) *
                                                                                abs(x['shares'])
                                                                            # positive prior shares * negative shares (selling)
                                                                            # should be < 0
                                                                            if x['prior_shares'] * x['shares'] < 0 else 0,
                                                        axis=1)

    context_df.loc[:, 'realized_short_pnl'] = context_df.apply(lambda x : (x['entry_price'] - x['last_known_entry']) *
                                                                                abs(x['shares'])
                                                                            if x['prior_shares'] * x['shares'] > 0 else 0,
                                                        axis=1)

    # unrealized pnl (change in market value) + (change in cash)
    context_df.loc[:, 'unrealized_long_pnl'] = context_df.apply(lambda x: (x['close_price'] - x['prior_close'])
                                                                        * (x['post_cumulative_shares'])
                                                                        if x[
                                                                                     'post_positions_usd'] > 0 else 0,
                                                              axis=1)

    context_df.loc[:, 'unrealized_short_pnl'] = context_df.apply(lambda x:  (x['close_price'] - x['prior_close'])
                                                                        * (x['post_cumulative_shares'])
                                                                        if x[
                                                                                     'post_positions_usd'] < 0 else 0,
                                                              axis=1)



    # update the main cartesian
    cartesian_df.update(context_df)

    #print("processed trades for portfolio for context id : {}".format(context))

# portfolio math
def calculate_portfolio(address, startCash):
    # grab the trades, prices
    trades, prices = generate_trades_with_prices(address)

    # get the earliest trade date, and trade_date_time
    entry_date = trades['delayed_trade_date'].min() + timedelta(days=-1)

    # remove trades without prices (for now), we will log this later and work on having tighter pricing
    trades = trades[~pd.isnull(trades['vwap'])]

    # getting the last trade per date_time_adj (since that's the time stamp of the price effectively, we don't need
    # multiple of these per ticker if it is traded daily
    # neat solution : https://stackoverflow.com/questions/37997668/pandas-number-rows-within-group-in-increasing-order
    # trades = trades.sort_values(by = 'trade_id', ascending=False)
    # temporal_scope = 'delayed_trade_date'
    temporal_scope = 'date_time_adj'
    trades['ticker_date_time_rank'] = trades.sort_values(by = 'trade_id', ascending=False).groupby(['symbol', temporal_scope]).cumcount()+1

    # filter trades to only have latest ticker (one line) per date_time_adj (DTA)
    # for example in Brooker's portfolio, he had set FB allocations multiple times for DTA : 2022-03-02 21:00:00+00:00
    # no need to have all of those lines since the entry_price is the same (due to same DTA) so no true portfolio MV change
    trades = trades[trades['ticker_date_time_rank'] == 1]

    # set up cartesian
    # dates = [entry_date + timedelta(days=x) for x in range(0, (datetime.today() - entry_date).days)]
    dates = pd.period_range(start = entry_date, end = datetime.today().strftime('%Y-%m-%d'),
                            freq = 'D' if temporal_scope == 'delayed_trade_date'
                            else 'H' if temporal_scope == 'date_time_adj' else 'D').to_list()
    tickers = trades['symbol'].unique()
    cartesian_list = [(d,t) for d in dates for t in tickers]
    cartesian_df = pd.DataFrame.from_records(cartesian_list, columns =['date', 'symbol'])
    cartesian_df['date'] = cartesian_df['date'].astype('datetime64[ns]')


    # set the 'portfolio context id' which will be the foreign key for the portfolio ID primary key
    # it will start with 1, while the portfolio ID starts with (first row) of 0 - that way we will always have a lookback
    cartesian_df['portfolio_context_id'] = cartesian_df['date'].rank(method='dense', ascending=True).astype(int)
    cartesian_df['prev_portfolio_context_id'] = cartesian_df['date'].rank(method='dense', ascending=True).astype(int) - 1

    # initialize columns starting at zero : shares, exposure, pnl, traded amounts
    cartesian_df[['shares','post_cumulative_shares',
                  'long_exposure_usd','short_exposure_usd','long_traded_usd','short_traded_usd',
                  'unrealized_long_pnl','unrealized_short_pnl','realized_long_pnl','realized_short_pnl'
                  ]] = 0

    # get eod prices
    price_scope = 'delayed_trade_date' if temporal_scope == 'dalayed_trade_date' else 'timestamp'
    prices['max_timestamp'] = prices.groupby(['symbol',price_scope])['timestamp'].transform('max')
    prices[['max_timestamp','timestamp']] = prices[['max_timestamp','timestamp']].applymap(lambda x: x.replace(tzinfo=None))

    #prices.to_csv('latest_prices.csv')

    latest_prices = prices[prices['timestamp'] == prices['max_timestamp']]
    # attach pricing
    cartesian_df = pd.merge(left=cartesian_df, right=latest_prices[['symbol', price_scope, 'vwap']], how='left',
                         left_on=['date', 'symbol'],
                         right_on=[price_scope, 'symbol'])

    cartesian_df['vwap'] = cartesian_df.groupby('symbol')['vwap'].transform(lambda x: x.ffill().bfill())

    cartesian_df = cartesian_df.rename(columns={'vwap': 'close_price'})

    # conversion for the merge
    trades[temporal_scope] = trades[temporal_scope].apply(lambda x: x.replace(tzinfo=None))

    # merge with trading key to see which days tickers had trading activity

    cartesian_df = pd.merge(left=cartesian_df,right=trades[['symbol',temporal_scope,'target_position_value','vwap']],
                            left_on=['date','symbol'], right_on=[temporal_scope, 'symbol'], how = 'left')

    cartesian_df = cartesian_df.rename(columns={'vwap': 'entry_price'})

    cartesian_df = cartesian_df.drop([x for x in cartesian_df if x.endswith('_y')], axis=1)

    cartesian_df.loc[cartesian_df['portfolio_context_id'] <=2,['prior_shares','positions_usd','prior_close','prior_positions_usd']] = 0

    cartesian_df.loc[cartesian_df['portfolio_context_id'] == 1,['prior_cash', 'post_cash']] = startCash

    cartesian_df.loc[cartesian_df['portfolio_context_id'] == 1, ['target_position_value']] = 0

    cartesian_df.loc[cartesian_df['portfolio_context_id'] <= 2, ['entry_price']] = cartesian_df['close_price']

    cartesian_df['prior_close'] = cartesian_df.groupby('symbol')['close_price'].shift()

    cartesian_df['last_known_entry'] = cartesian_df.groupby('symbol')['entry_price'].shift()

    # initialize portfolio post position usd, this is the final portfolio value of the day
    # needed when we have trading at an entry price intra day and need to update its value at the end of the day
    cartesian_df['post_positions_usd'] = 0

    cartesian_df.insert(loc =0,column = 'ID', value =[x for x in range(1, len(cartesian_df.values) + 1)])

    cartesian_df.set_index('ID', inplace=True)

    # testing
    for i in range(2, cartesian_df['portfolio_context_id'].max()+1):
        process_trade_snapshot(i, cartesian_df)

    #cartesian_df = cartesian_df.groupby('portfolio_context_id')

    cartesian_df.to_csv('cartesian_df.csv')

    # collapse cartesian into portfolio and calculate inception to date returns
    portfolio_df = cartesian_df.groupby('date').agg({'post_cash':['mean'],
                                                     'post_positions_usd': 'sum',
                                                     'long_exposure_usd' : 'sum',
                                                     'short_exposure_usd' : 'sum',
                                                     'long_traded_usd': 'sum',
                                                     'short_traded_usd': 'sum',
                                                     'realized_long_pnl': 'sum',
                                                     'realized_short_pnl': 'sum',
                                                     'unrealized_long_pnl': 'sum',
                                                     'unrealized_short_pnl': 'sum'
                                                     })

    # flatten the dual index
    portfolio_df.columns = [' '.join(col).strip() for col in portfolio_df.columns.values]

    portfolio_df = portfolio_df.rename(columns={'post_cash mean': 'cash_usd',
                                                'post_positions_usd sum': 'positions_usd',
                                                'long_exposure_usd sum' : 'long_exposure_usd',
                                                'short_exposure_usd sum' : 'short_exposure_usd'})

    portfolio_df['portfolio_usd'] = portfolio_df['cash_usd'] + portfolio_df['positions_usd']

    portfolio_df = portfolio_df.reset_index()

    portfolio_df['user_id'] = address

    # compounding
    compounding_period = 'day'

    compounding_format_dict = {'day' : '%Y-%m-%d', 'hour' : '%Y-%m-%d %H:00:00'}

    portfolio_df['period'] = portfolio_df.apply(lambda x : x['date'].strftime(compounding_format_dict.get(compounding_period,'%Y-%m-%d')),axis=1)

    # get the latest per period
    portfolio_df['latest'] = portfolio_df.groupby('period')['date'].transform('max')

    # sum up the long traded and short traded since they show up not necessarily on the 'latest' day of the portfolio agg.
    portfolio_df['long_traded_usd'] = portfolio_df.groupby('period')['long_traded_usd sum'].transform('sum')
    portfolio_df['short_traded_usd'] = portfolio_df.groupby('period')['short_traded_usd sum'].transform('sum')

    # sum up the long/short realized/unrealized pnls
    portfolio_df['realized_long_pnl'] = portfolio_df.groupby('period')['realized_long_pnl sum'].transform('sum')
    portfolio_df['realized_short_pnl'] = portfolio_df.groupby('period')['realized_short_pnl sum'].transform('sum')
    portfolio_df['unrealized_long_pnl'] = portfolio_df.groupby('period')['unrealized_long_pnl sum'].transform('sum')
    portfolio_df['unrealized_short_pnl'] = portfolio_df.groupby('period')['unrealized_short_pnl sum'].transform('sum')

    # filter for latest per period
    portfolio_df = portfolio_df[portfolio_df['date'] == portfolio_df['latest']]

    portfolio_df['daily_returns'] = portfolio_df['portfolio_usd'].pct_change()

    portfolio_df['itd_return'] = (portfolio_df['daily_returns'] +1).cumprod() - 1

    # second order exposure $ columns : dollar exposure
    portfolio_df['gross_exposure_usd'] = portfolio_df['long_exposure_usd'] + abs(portfolio_df['short_exposure_usd'])
    portfolio_df['net_exposure_usd'] = portfolio_df['long_exposure_usd'] + portfolio_df['short_exposure_usd']

    # second order exposure % columns : % exposure of NAV (portfolio_usd)
    portfolio_df['gross_exposure_percent'] = portfolio_df['gross_exposure_usd'] / portfolio_df['portfolio_usd']
    portfolio_df['long_exposure_percent'] = portfolio_df['long_exposure_usd'] / portfolio_df['portfolio_usd']
    portfolio_df['short_exposure_percent'] = portfolio_df['short_exposure_usd'] / portfolio_df['portfolio_usd']
    portfolio_df['net_exposure_percent'] = portfolio_df['net_exposure_usd'] / portfolio_df['portfolio_usd']

    # second order traded columns
    portfolio_df['gross_traded_usd'] = portfolio_df['long_traded_usd'] + abs(portfolio_df['short_traded_usd'])
    portfolio_df['net_traded_usd'] = portfolio_df['long_traded_usd'] + portfolio_df['short_traded_usd']
    portfolio_df['gross_traded_percent'] = portfolio_df['gross_traded_usd'] / portfolio_df['portfolio_usd']
    portfolio_df['net_traded_percent'] = portfolio_df['net_traded_usd'] / portfolio_df['portfolio_usd']

    # second order PnL Columns
    portfolio_df['unrealized_pnl'] = portfolio_df['unrealized_long_pnl'] + portfolio_df['unrealized_short_pnl']
    portfolio_df['realized_pnl'] = portfolio_df['realized_long_pnl'] + portfolio_df['realized_short_pnl']
    portfolio_df['total_long_pnl'] = portfolio_df['unrealized_long_pnl'] + portfolio_df['realized_long_pnl']
    portfolio_df['total_short_pnl'] = portfolio_df['unrealized_short_pnl'] + portfolio_df['realized_short_pnl']
    portfolio_df['total_pnl'] = portfolio_df['total_long_pnl'] + portfolio_df['total_short_pnl']




    portfolio_df = portfolio_df[['date','user_id','cash_usd','portfolio_usd','daily_returns','itd_return',
                                 'gross_exposure_usd','long_exposure_usd','short_exposure_usd','net_exposure_usd',
                                 'gross_exposure_percent','long_exposure_percent','short_exposure_percent','net_exposure_percent',
                                 'gross_traded_usd','net_traded_usd','gross_traded_percent','net_traded_percent',
                                 'unrealized_long_pnl','unrealized_short_pnl','unrealized_pnl',
                                 'realized_long_pnl','realized_short_pnl','realized_pnl',
                                 'total_long_pnl','total_short_pnl','total_pnl']]


    portfolio_df.to_csv('portfolio.csv', index=False)


if __name__ == '__main__':
    start_time = time.time()
    # calculate portfolio for particular address
    # BB Portfolio
    # port_df = calculate_portfolio('0x594F56D21ad544F6B567F3A49DB0F9a7B501FF37',10000)
    # Vadim Portfolio
    # port_df = calculate_portfolio('0x55E580d9e296f9Ef7F02fe1516A0925629726801',10000)
    # ecd
    port_df = calculate_portfolio('0xf66aD6E503F8632c85C82027c9Df12FAE205e916',10000)
    print("---Portfolio finished in %s seconds ---" % (time.time() - start_time))
