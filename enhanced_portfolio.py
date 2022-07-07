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

def getMostRecentTrade(dateAsOf, tradingKey):
    tradingKeyCopy = tradingKey.fillna(0)
    shares = []
    entryPrice = []
    ids = []
    currentPosition = []
    realizedProfits = []
    asOf = (tradingKeyCopy['date_time'] < dateAsOf)
    recent = tradingKeyCopy.loc[asOf]
    tickerList = list(set(recent['symbol']))

    ## first get rid of the zeroed out positions
    newTickerList = []
    for trade in tickerList:
        lastTradesForTicker = recent.loc[recent['symbol'] == trade]
        cumShares = lastTradesForTicker['post_cumulative_share_count'].iat[-1]  ## take the last row == most recent
        if round(cumShares, 8) != 0:
            newTickerList.append(trade)
            shares.append(cumShares)

            # now iterate through the newtickerList that only contains active positions
    for trade in newTickerList:
        lastTrade = recent.loc[recent['symbol'] == trade]

        onePrice = lastTrade['vwap'].iat[-1]  ## take the last row == most recent
        entryPrice.append(onePrice)

        oneId = lastTrade.index[-1]  ## take the last row == most recent
        ids.append(oneId)

        oneCurrent = lastTrade['current_position'].iat[-1]  ## take the last row == most recent
        currentPosition.append(oneCurrent)

        oneRealized = lastTrade['realized_profit'].sum() - lastTrade['realized_profit'].iat[
            -1]  ## take the sum of all realized up to this point but don't include realized profits from
        realizedProfits.append(oneRealized)

    shareList = dict(zip(newTickerList, shares))
    priceList = dict(zip(newTickerList, entryPrice))
    idList = dict(zip(newTickerList, ids))
    currentList = dict(zip(newTickerList, currentPosition))
    realizedList = dict(zip(newTickerList, realizedProfits))

    return shareList, priceList, idList, currentList, realizedList

# now get the most recent rolling prices for the unique tickers and date
def getRollingPrices(date, prices):
    allTickers = prices['symbol'].at[date]
    allPrices = prices['vwap'].at[date]
    try:
        oneTwoThree = dict(zip(allTickers, allPrices))
    except:
        oneTwoThree = {allTickers: allPrices}

    return oneTwoThree

def getDayDividends(endDate):
    df = pd.read_csv('data/dividend_split.csv')
    df = df[(df['div_or_split'] == 'dividend') & (df['record_date'] == endDate)][['symbol','amount']]
    df.set_index('symbol',inplace=True)
    df = df.to_dict()
    return df['amount']

def getCashChangeFromDividends(endDate,activeTrades):
    activeDividends = getDayDividends(endDate)
    dividendCash = 0
    for trade in activeTrades.keys() :
        if trade in activeDividends :
            dividendPayment = float(activeTrades[trade]) *float(activeDividends[trade])
        else :
            dividendPayment = 0
        dividendCash += dividendPayment
    return dividendCash

def getDaySplits(endDate):
    df = pd.read_csv('data/dividend_split.csv')
    df = df[(df['div_or_split'] == 'split') & (df['record_date'] == endDate)][['symbol', 'amount']]
    df.set_index('symbol', inplace=True)
    df = df.to_dict()
    return df['amount']

def getEntryAndPostCumShareFromSplits(tradingKey,endDate,activeIds):
    activeSplits = getDaySplits(endDate)
    for trade in activeIds.keys() :
        if trade in activeSplits :
            # update adjusted_entry
            tradingKey['adjusted_entry'].at[activeIds[trade]] = tradingKey['entry_price'].at[activeIds[trade]]
            tradingKey['entry_price'].at[activeIds[trade]] = tradingKey['entry_price'].at[activeIds[trade]] * activeSplits[trade]
            tradingKey['post_cumulative_share_count'].at[activeIds[trade]] = tradingKey['post_cumulative_share_count'].at[activeIds[trade]] / activeSplits[trade]
    return tradingKey

def updatePortfolioMath(userId, startCash, ann_interest = 0.02):

    trading_key, prices = generate_trades_with_prices(userId)

    trading_key = trading_key[~trading_key['vwap'].isnull()]

    # for testing
    #trading_key = trading_key[trading_key['trade_id'] == 1]

    trading_key[['post_cumulative_share_count','realized_profit','prior_portfolio_value',
                 'current_position',
                 'prior_position_value',
                 'cash_used',
                 'share_count',
                 'prior_cumulative_share_count',
                 'post_cumulative_share_count'
                 ]] = 0

    trading_key.to_csv('trading_key.csv')
    prices.to_csv('prices.csv')

    # earliest trade date as start date
    startDate = trading_key['delayed_trade_date'].min()

    # initialize new portfolio
    portfolio = pd.DataFrame({"date_time": startDate, "user_id": userId, "cash": startCash, "usd_value": startCash,
                "positions_usd": 0,
                "inception_return": 1.0, "gross_exposure_usd": 0.0, "long_exposure_usd": 0.0,
                "short_exposure_usd": 0.0, "net_exposure_usd": 0.0, "gross_exposure_percent": 0.0,
                "long_exposure_percent": 0.0, "short_exposure_percent": 0.0, "net_exposure_percent": 0.0,
                "gross_traded_usd": 0.0, "net_traded_usd": 0.0, "gross_traded_percent": 0.0,
                "net_traded_percent": 0.0, "unrealized_long_pnl": 0.0, "unrealized_short_pnl": 0.0,
                "unrealized_pnl": 0.0,
                "realized_long_pnl": 0.0, "realized_short_pnl": 0.0, "realized_pnl": 0.0,
                "total_long_pnl": 0.0, "total_short_pnl": 0.0, "total_pnl": 0.0, "marker": 'FIRST'}, index = [0])

    portfolio['date_time'] = pd.to_datetime(portfolio['date_time'])
    portfolio.set_index('date_time', inplace=True)


    # generate dates based off price key
    new_portfolio = prices.copy()
    new_portfolio.drop(columns=['vwap','symbol','delayed_trade_date'], inplace=True)
    new_portfolio.drop_duplicates(inplace=True)
    new_portfolio['timestamp'] = new_portfolio['timestamp'].dt.tz_localize(None)
    new_portfolio.set_index('timestamp', inplace=True)
    portfolio = pd.concat([portfolio,new_portfolio])
    portfolio['user_id'] = portfolio['user_id'].ffill()
    prices.set_index('timestamp', inplace=True)


    #portfolio.sort_index(ascending=True, inplace=True)

    #portfolio.to_csv('blank_portfolio.csv')
    # positions accumulator and tracker
    positions = pd.DataFrame(columns=['date_time', 'user_id', 'symbol',
                                      'usd_position_value', 'percent_position',
                                      'post_cumulative_share_count', 'price',
                                      'entry_price', 'unrealized_pnl',
                                      'pos_pnl' ,'pos_roi','marker'])

    # iterate through the new portfolio object with the recent trading activity being included
    # perform the math for portfolio
    for row in range(1, 30): #len(portfolio.index)):
        portfolio.sort_index(ascending=True, inplace=True)
        # get get period
        end_date = portfolio.index[row] #.replace(tzinfo=None)
        start_date = portfolio.index[row - 1] #.replace(tzinfo=None)
        prior_cash = portfolio['cash'].iat[row - 1]
        daily_interest = ann_interest * (end_date - start_date) / timedelta(days=365)

        # set up a new starting cash balance and calculate any interest cost for leverage
        cash_interest_payment = 0 if prior_cash > 0 else prior_cash * daily_interest

        cash_change = 0
        gross_cash_change = 0
        long_realized_profit = 0
        short_realized_profit = 0
        new_cash = prior_cash + cash_interest_payment

        # grab the new trades (in scope of the time period)
        new_trades = trading_key[(trading_key['date_time'] > start_date) & (trading_key['date_time'] <= end_date)]

        # get the previous portfolio value
        prior_portfolio_usd = portfolio['usd_value'].iat[row - 1]

        # if we have trades
        if len(new_trades.index) > 0:
            for trade in range(0, len(new_trades.index)):
                # first figure out the old vs. new position size and proposed cash impact
                ticker = new_trades['symbol'].iat[trade]
                price = new_trades['vwap'].iat[trade]
                percent = new_trades['target_position_value'].iat[trade]
                tradeId = new_trades.index[trade]

                # Check if there are prior positions:
                prior_positions = trading_key.loc[(trading_key['symbol'] == ticker) & (trading_key.index < tradeId)]
                prior_positions.sort_index(inplace=True)
                if len(prior_positions) > 0:
                    prior_shares = prior_positions['post_cumulative_share_count'].iat[-1]  ## grabbing the most recent one.
                    prior_price = prior_positions['vwap'].iat[-1]  ## grabbing the most recent one.
                    prior_trade_id = prior_positions.index[-1]  # grabbing most recent
                    prior_profit = (price - prior_price) * prior_shares
                    trading_key['realized_profit'].at[prior_trade_id] = prior_profit
                    if prior_shares > 0:
                        long_realized_profit += prior_profit
                    else:
                        short_realized_profit += prior_profit
                else:
                    prior_shares = 0
                    prior_profit = 0

                prior_value = price * prior_shares

                #print(type(percent), type(prior_portfolio_usd))
                new_value = float(percent) * prior_portfolio_usd
                cash_hit = prior_value - new_value
                # now get feeAdj price
                change_in_shares = (cash_hit * -1) / (price)

                # now save the trade info
                trading_key['prior_portfolio_value'].at[tradeId] = prior_portfolio_usd
                trading_key['target_position_value'].at[tradeId] = new_value
                trading_key['current_position'].at[tradeId] = new_value
                trading_key['prior_position_value'].at[tradeId] = prior_value
                trading_key['cash_used'].at[tradeId] = cash_hit
                trading_key['share_count'].at[tradeId] = change_in_shares
                trading_key['prior_cumulative_share_count'].at[tradeId] = prior_shares
                trading_key['post_cumulative_share_count'].at[tradeId] = prior_shares + change_in_shares

                # now tally the cash impact of the trades
                cash_change += cash_hit
                gross_cash_change += abs(cash_hit)

            # once all trades have been calculated save the trade data
            new_cash += cash_change
            new_cash += (gross_cash_change * -0.0005)  # assume a 5bps fee
            portfolio['gross_traded_usd'].iat[row] = gross_cash_change
            portfolio['net_traded_usd'].iat[row] = -cash_change
            portfolio['gross_traded_percent'].iat[row] = gross_cash_change / prior_portfolio_usd
            portfolio['net_traded_percent'].iat[row] = -cash_change / prior_portfolio_usd

        # work with the active trades
        activeTrades, activeEntryPrice, activeIds, activePositions, activeRealized = getMostRecentTrade(end_date,
                                                                                                        trading_key)
        #print(activeTrades)
        # get all prices for trades that have happened
        activePrices = getRollingPrices(end_date, prices)

        positionsUsd = 0.0
        shortPositions = 0.0
        longPositions = 0.0
        longUnrealizedPnl = 0.0
        shortUnrealizedPnl = 0.0

        lastUsdValue = portfolio['usd_value'].iat[row - 1]
        marker = portfolio['marker'].iat[row]
        # check if there are any trades to roll over

        # Dividend math at day turnover
        if end_date.day == start_date.day:
            dividendCash = 0

        else:
            # check for dividends
            dividendCash = getCashChangeFromDividends(end_date.replace(hour=0, minute=0, microsecond=0, second=0),
                                                         activeTrades)
            # check for splits
            trading_key = getEntryAndPostCumShareFromSplits(trading_key, end_date, activeIds)
            print('EOD :' + str(startDate) + ' BOD :' + str(end_date))

        new_cash += dividendCash

        for trade in activeTrades:
            # bring down position from last row
            cum_shares = activeTrades[trade]
            entry_price = activeEntryPrice[trade]
            trade_id = activeIds[trade]
            # not get the new rollingPrices
            try:
                price = activePrices[trade]
            except:
                price = activePositions[trade] / cum_shares
            # Calculate each position value

            position = (cum_shares * price)
            unrealized_pnl = (price - entry_price) * cum_shares
            # oneSymbolOnly = (tradingKey['symbol'] == trade)
            realized_pnl = activeRealized[trade]
            posPnl = realized_pnl + unrealized_pnl
            posRoi = posPnl / (abs(position) - unrealized_pnl)

            positionsUsd += position
            percentPosition = position / lastUsdValue

            # now save the position
            # create some array that is appended to the DF
            new_row = {'date_time': end_date, 'user_id': userId, 'symbol': trade,
                       'usd_position_value': position, 'percent_position': percentPosition,
                       'post_cumulative_share_count': cum_shares, 'price': price,
                       'entry_price': entry_price, 'unrealized_pnl': unrealized_pnl,
                       'pos_pnl': posPnl, 'pos_roi': posRoi, 'marker': marker}

            positions = positions.append(new_row, ignore_index=True)

            if position > 0:
                longPositions += position
                longUnrealizedPnl += unrealized_pnl
            else:
                shortPositions += position
                shortUnrealizedPnl += unrealized_pnl

            # update current position
            trading_key['current_position'].at[tradeId] = position

        # Now save the new portfolio
        portfolio['positions_usd'].iat[row] = positionsUsd
        portfolio['cash'].iat[row] = new_cash
        portfolio['usd_value'].iat[row] = new_cash + positionsUsd
        portfolio['inception_return'].iat[row] = portfolio['usd_value'].iat[row] / portfolio['usd_value'].iat[
            row - 1] * portfolio['inception_return'].iat[row - 1]
        portfolio['gross_exposure_usd'].iat[row] = longPositions - shortPositions
        portfolio['long_exposure_usd'].iat[row] = longPositions
        portfolio['short_exposure_usd'].iat[row] = shortPositions
        portfolio['net_exposure_usd'].iat[row] = longPositions + shortPositions
        portfolio['gross_exposure_percent'].iat[row] = (longPositions - shortPositions) / lastUsdValue
        portfolio['long_exposure_percent'].iat[row] = longPositions / lastUsdValue
        portfolio['short_exposure_percent'].iat[row] = shortPositions / lastUsdValue
        portfolio['net_exposure_percent'].iat[row] = (longPositions + shortPositions) / lastUsdValue

        portfolio['unrealized_long_pnl'].iat[row] = longUnrealizedPnl
        portfolio['unrealized_short_pnl'].iat[row] = shortUnrealizedPnl
        portfolio['unrealized_pnl'].iat[row] = longUnrealizedPnl + shortUnrealizedPnl
        portfolio['realized_long_pnl'].iat[row] = cash_interest_payment + dividendCash + long_realized_profit + portfolio['realized_long_pnl'].iat[row - 1]
        portfolio['realized_short_pnl'].iat[row] = short_realized_profit + portfolio['realized_short_pnl'].iat[
            row - 1]
        portfolio['realized_pnl'].iat[row] = cash_interest_payment + dividendCash + long_realized_profit + short_realized_profit + \
                                                portfolio['realized_pnl'].iat[row - 1]

        portfolio['total_long_pnl'].iat[row] = longUnrealizedPnl + \
                                                portfolio['realized_long_pnl'].iat[row]
        portfolio['total_short_pnl'].iat[row] = shortUnrealizedPnl + \
                                                portfolio['realized_short_pnl'].iat[row]
        portfolio['total_pnl'].iat[row] = longUnrealizedPnl + shortUnrealizedPnl + portfolio['realized_pnl'].iat[row]

        print(row)




    portfolio.to_csv('portfolio.csv', index=True)


if __name__ == '__main__':
    start_time = time.time()
    # calculate portfolio for particular address
    # BB Portfolio
    # port_df = updatePortfolioMath('0x594F56D21ad544F6B567F3A49DB0F9a7B501FF37',10000)
    # Vadim Portfolio
    # port_df = updatePortfolioMath('0x55E580d9e296f9Ef7F02fe1516A0925629726801',10000)
    # ecd
    # port_df = calculate_portfolio('0xf66aD6E503F8632c85C82027c9Df12FAE205e916',10000)
    # user id 1
    # port_df = updatePortfolioMath('0x49649d164e7aa196ef7bb6ad8eab00d658305eaa',10000)
    # 7/6/22 test
    port_df = updatePortfolioMath('0xd019955e5Db68ebd41CE5A7A327DdD5f2658e8D9', 10000)

    print("---Portfolio finished in %s seconds ---" % (time.time() - start_time))
