import json
import time
import asyncio
import pandas as pd
from web3 import Web3
from web3.middleware import geth_poa_middleware

class Trade():
    def __init__(self, **kwargs):
        # get the address, default to Brooker
        self.address = kwargs.get('address', '0x594F56D21ad544F6B567F3A49DB0F9a7B501FF37')

        # skale url
        self.skale_url = kwargs.get('skale_url', 'https://api.skalenodes.com/v1/rhythmic-tegmen')

        # infura urls - should already come in as a package deal of the infura url + infura project id
        self.infura_url = kwargs.get('infura_url',
                                     'https://polygon-mainnet.infura.io/v3/83add7805f9441e08cc04d9f7d0fce08')

        # covey ledger address (SKALE)
        self.covey_ledger_skale_address = kwargs.get('covey_ledger_skale_address',
                                                     '0xC93643aF734Ce80BC078643191c072bFd216468f')

        # covey ledger address (polygon)
        self.covey_ledger_polygon_address = kwargs.get('covey_ledger_polygon_address',
                                                     '0x587Ec5a7a3F2DE881B15776BC7aaD97AA44862Be')

        # set the abi - must pass in the covey ledger file here otherwise will not work
        self.abi = json.load(open('CoveyLedger.json'))['abi']

        # set up the empty dataframe that all of the trades from all chains will append to
        self.trades = pd.DataFrame(columns=['address', 'trades', 'date_time'])


    # output format [('address', 'position string', unix time),('address', 'position string', unix time),...]
    async def get_trades_skale(self):
        w3 = Web3(Web3.HTTPProvider(self.skale_url))
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        covey_ledger = w3.eth.contract(address=self.covey_ledger_skale_address, abi=self.abi)
        my_address = w3.toChecksumAddress(self.address)
        result = covey_ledger.functions.getAnalystContent(my_address).call()
        result_df = pd.DataFrame(result, columns=['address', 'trades', 'date_time'])
        result_df.insert(0, 'chain', 'SKL')
        self.trades = pd.concat([self.trades,result_df])
        return 0


    # output format [('address', 'position string', unix time),('address', 'position string', unix time),...]
    async def get_trades_polygon(self):
        w3 = Web3(Web3.HTTPProvider(self.infura_url))
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        covey_ledger = w3.eth.contract(address=self.covey_ledger_polygon_address, abi=self.abi)
        my_address = w3.toChecksumAddress(self.address)
        result = covey_ledger.functions.getAnalystContent(my_address).call()
        result_df = pd.DataFrame(result, columns=['address', 'trades', 'date_time'])
        result_df.insert(0, 'chain', 'MATIC')
        self.trades = pd.concat([self.trades, result_df])
        return 0


    async def gather_trades(self):
        await asyncio.gather(self.get_trades_skale(),self.get_trades_polygon())

    # add BLANK ticker, 0 Target Position for wallets with no activity
    def cleanup_trade_cells(self,s):
        return s if len(s) > 0 else 'BLANK:0'

    # check if string is numeric : https://stackoverflow.com/questions/354038/how-do-i-check-if-a-string-is-a-number-float
    def is_number_repl_isdigit(self,s):
        """ Returns True is string is a number. """
        try:
            return s.lstrip("-").replace('.', '', 1).isdigit()
        except AttributeError:
            return False

    # transformations to the trades data frame including the actual splitting out of the trades lists
    def transform_trades(self):
        # make sure the trades are actually filled first
        if len(self.trades.index) > 0:
            # convert unix time to datetime
            self.trades['date_time'] = pd.to_datetime(self.trades['date_time'], unit='s')
            # split trades column into multiple rows by delimiter, resulting in each row having one ticker : position combo
            self.trades = self.trades.assign(trades=self.trades['trades'].str.split(',')).explode('trades')
            # clean up blank trade cells, empty string
            self.trades['trades'] = self.trades['trades'].apply(lambda x: self.cleanup_trade_cells(x))
            # split trades column into symbol, position columns
            try:
                self.trades[['symbol', 'target_position_value']] = self.trades['trades'].str.split(':', expand=True).iloc[:,
                                                                 0:2]
            except ValueError:
                self.trades[['symbol', 'target_position_value']] = ['BLANK', 0]

            # clean up the covey-reset, the target_position value should be numeric
            self.trades['target_position_value'] = self.trades['target_position_value'].apply(
                lambda x: x if self.is_number_repl_isdigit(x) else 0)

            # remove timezone awareness
            self.trades['date_time'] = self.trades['date_time'].dt.tz_localize(None)
        else:
            print("The dataframe has not been filled yet")

    # export to csv
    def export_to_csv(self):
        self.trades.to_csv('trades.csv')


if __name__ == '__main__':
    # start the timer
    start_time = time.time()
    # initialize trade data object, default will be BB portfolio
    t = Trade()
    # run the asynchronous code to gather trades from all chains
    asyncio.run(t.gather_trades())
    # transform the trade date
    t.transform_trades()
    # export
    t.export_to_csv()
    # log how long it took
    print('---Trades for address {} finished in {} seconds ---'.format(t.address,time.time() - start_time))