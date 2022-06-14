from dotenv import load_dotenv
import os
from web3 import Web3
import eth_keys
from eth_account import account
from web3.middleware import geth_poa_middleware
import json
import pandas as pd
import requests
from datetime import datetime
import time

load_dotenv()

WALLET = os.getenv('WALLET')
INFURA_PROJECT_ID = os.getenv('INFURA_PROJECT_ID')
INFURA_URL = os.getenv('INFURA_URL')
POLYGON_CHAIN_ID = os.getenv('POLYGON_CHAIN_ID')
COVEY_LEDGER_POLYGON_ADDRESS = os.getenv('COVEY_LEDGER_POLYGON_ADDRESS')
COVEY_LEDGER_SKALE_ADDRESS = os.getenv('COVEY_LEDGER_SKALE_ADDRESS')
SKALE_URL = os.getenv('SKALE_URL')
IEX_TOKEN = os.getenv('IEX_TOKEN')

# Opening JSON file
f = open('CoveyLedger.json')

# returns JSON object as
# a dictionary
ledger_info = json.load(f)


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


def view_trades_polygon(address):
    w3 = Web3(Web3.HTTPProvider(f'{INFURA_URL}/{INFURA_PROJECT_ID}'))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    covey_ledger = w3.eth.contract(address=COVEY_LEDGER_POLYGON_ADDRESS, abi=ledger_info['abi'])
    my_address = w3.toChecksumAddress(address)
    result = covey_ledger.functions.getAnalystContent(my_address).call()
    # output format [('address', 'position string', unix time),('address', 'position string', unix time),...]
    #print(result)
    return result


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


def get_prices(symbols, exactDate):
    # docs:  '''https://iexcloud.io/docs/api/#historical-prices'''
    root_url = 'https://cloud.iexapis.com/stable/stock/market/batch?symbols='
    method = '&types=intraday-prices&'
    exactDate = pd.to_datetime(exactDate, infer_datetime_format=True)
    exactDate = exactDate.strftime('%Y%m%d')
    quotes = dict()
    for xOf100 in range(0, len(symbols), 100):
        urlVersion = symbols[xOf100:xOf100 + 100]
        urlVersion = ",".join(urlVersion)
        url = root_url + urlVersion + method + '&token=' + IEX_TOKEN + '&exactDate=' + exactDate
        data = json.loads(requests.get(url).text)
        quotes.update(data)
    return quotes


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

final_output_columns = ["date_time", "user_id", "cash", "usd_value", "positions_usd",
                                      "inception_return", "gross_exposure_usd", "long_exposure_usd",
                                      "short_exposure_usd", "net_exposure_usd", "gross_exposure_percent",
                                      "long_exposure_percent", "short_exposure_percent", "net_exposure_percent",
                                      "gross_traded_usd", "net_traded_usd", "gross_traded_percent",
                                      "net_traded_percent", "unrealized_long_pnl", "unrealized_short_pnl",
                                      "unrealized_pnl","realized_long_pnl", "realized_short_pnl", "realized_pnl",
                                      "total_long_pnl", "total_short_pnl", "total_pnl"]

wallets_df = pd.read_csv('data/allWallets.csv')
#wallets_df = pd.read_csv('data/wallet_neg_sign_test.csv')
wallets_df = wallets_df.iloc[:100,:]
trading_key = wallets_df.groupby('eth_cust_address').apply(get_trades_double_chain)

trading_key_out = pd.DataFrame()
trading_key.to_csv('output/trading_key_' + datetime.now().strftime("%m_%d_%Y_%H_%M_%S") + '.csv')

print("--- %s seconds ---" % (time.time() - start_time))
