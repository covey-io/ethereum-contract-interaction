import time
import pandas as pd
from trade import Trade
from covey_calendar import CoveyCalendar
import covey_checks as covey_checks
from datetime import datetime,timedelta

class Portfolio(Trade):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # default start cash to 10000
        self.start_cash = kwargs.get('start_cash', 10000)
        # default annual interest to 0.2 %
        self.ann_interest = kwargs.get('ann_interest', 0.02)
        # generate the portfolio
        self.portfolio = self.updatePortfolioMath()
        # generate crypto pricing error report
        self.unpriced_crypto = covey_checks.check_crypto_tickers(self.trading_key)


    def get_recent_trade_stats(self, portfolio_date, trading_key):
        df = trading_key.copy()
        df = df[(df['market_entry_date_time'] < portfolio_date) ]
        df['symbol_date_rank'] = df.groupby('symbol')['market_entry_date_time'].rank('dense', ascending=False)
        # realized profit sum partitioned by symbol
        df['realized_profit_symbol_agg'] = df.groupby('symbol')['realized_profit'].transform(sum)
        df['realized_profit_final'] = df['realized_profit_symbol_agg'] - df['realized_profit']
        df = df[(df['symbol_date_rank'] == 1) & (df['post_cumulative_share_count'] != 0)]
        df['current_position'].fillna(0, inplace=True)

        # dividend logic 
        dividends_df = pd.read_csv('data/dividend_split.csv')
        dividends_df = dividends_df[(dividends_df['div_or_split'] == 'dividend') & (dividends_df['payment_date'] == portfolio_date)]
        dividends_df['payment_date'] = pd.to_datetime(dividends_df['payment_date'])
        # merge to main df (trading key) and calculate dividends
        df = pd.merge(left=df, right=dividends_df, on = 'symbol', how='left')
        df['dividend_cash'] = df['amount'] * df['post_cumulative_share_count']


        return df[['symbol', 'post_cumulative_share_count', 'vwap', 'current_position', 'realized_profit']]


    # now get the most recent rolling prices for the unique tickers and date

    # def getDayDividends(self, endDate):
    #     df = pd.read_csv('data/dividend_split.csv')
    #     df['payment_date'] = pd.to_datetime(df['payment_date'])
    #     df = df[(df['div_or_split'] == 'dividend') & (df['payment_date'] == endDate)][['symbol','amount']]
    #     df.set_index('symbol',inplace=True)
    #     df = df.to_dict()
    #     return df['amount']

    # def getCashChangeFromDividends(self, endDate, activeTrades):
    #     activeDividends = self.getDayDividends(endDate)
    #     dividendCash = 0
    #     for trade in activeTrades:
    #         if trade in activeDividends:
    #             dividendPayment = float(activeTrades[trade]) * float(activeDividends[trade])
    #         else :
    #             dividendPayment = 0
    #         dividendCash += dividendPayment
    #     return dividendCash

    def getDaySplits(self, endDate):
        df = pd.read_csv('data/dividend_split.csv')
        df['payment_date'] = pd.to_datetime(df['payment_date'])
        df = df[(df['div_or_split'] == 'split') & (df['payment_date'] == endDate)][['symbol', 'amount']]
        df.set_index('symbol', inplace=True)
        df = df.to_dict()
        return df['amount']

    def getEntryAndPostCumShareFromSplits(self, tradingKey, endDate, activeIds):
        activeSplits = self.getDaySplits(endDate)
        for trade in activeIds:
            if trade in activeSplits:
                # update adjusted_entry
                tradingKey['adjusted_entry'].at[activeIds[trade]] = tradingKey['vwap'].at[activeIds[trade]]
                tradingKey['vwap'].at[activeIds[trade]] = tradingKey['vwap'].at[activeIds[trade]] * activeSplits[trade]
                tradingKey['post_cumulative_share_count'].at[activeIds[trade]] = tradingKey['post_cumulative_share_count'].at[activeIds[trade]] / activeSplits[trade]
        return tradingKey

    def updatePortfolioMath(self):

        # for testing
        #trading_key = trading_key[trading_key['trade_id'] == 1]

        self.trading_key[[
            'post_cumulative_share_count',
            'realized_profit',
            'prior_portfolio_value',
            'current_position',
            'prior_position_value',
            'cash_used',
            'share_count',
            'prior_cumulative_share_count',
            'post_cumulative_share_count',
            'adjusted_entry'
        ]] = 0

        trading_key = self.trading_key.copy()

        # earliest trade date as start date
        start_date = trading_key['market_entry_date_time'].min() + timedelta(days=-1)

        trading_key = trading_key[~trading_key['vwap'].isnull()]

        # initialize new portfolio
        portfolio = pd.DataFrame({'date_time': start_date.strftime('%Y-%m-%d'),
                                  'user_id': self.address, 'cash': self.start_cash,
                                  'usd_value': self.start_cash ,'positions_usd': 0,
                                  'inception_return': 1.0, 'gross_exposure_usd': 0.0,
                                  'long_exposure_usd': 0.0,'short_exposure_usd': 0.0,
                                  'net_exposure_usd': 0.0, 'gross_exposure_percent': 0.0,
                                  'long_exposure_percent': 0.0, 'short_exposure_percent': 0.0,
                                  'net_exposure_percent': 0.0, 'gross_traded_usd': 0.0,
                                  'net_traded_usd': 0.0, 'gross_traded_percent': 0.0, 'net_traded_percent': 0.0,
                                  'unrealized_long_pnl': 0.0, 'unrealized_short_pnl': 0.0, 'unrealized_pnl': 0.0,
                                  'realized_long_pnl': 0.0, 'realized_short_pnl': 0.0, 'realized_pnl': 0.0,
                                  'total_long_pnl': 0.0, 'total_short_pnl': 0.0, 'total_pnl': 0.0,
                                  'marker': 'FIRST'}, index = [0])

        portfolio['date_time'] = pd.to_datetime(portfolio['date_time'])
        portfolio.set_index('date_time', inplace=True)

        # generate dates based off price key
        prices = self.price_key

        # calendar key 
        c = CoveyCalendar(start_date = prices['delayed_trade_date'].min())
        calendar_key = c.set_business_dates()
        calendar_key_df = pd.DataFrame(calendar_key[calendar_key['date'] < datetime.now().replace(hour=0,minute=0, second=0, microsecond =0)]['next_market_close'].unique()).set_index(0)
        calendar_key_df.index = calendar_key_df.index.append(pd.Index([prices.reset_index()['timestamp'].max()]))
        portfolio = pd.concat([portfolio, calendar_key_df])
        portfolio['user_id'] = portfolio['user_id'].ffill()
        prices.set_index('timestamp', inplace=True)

        # create an empty list that will be populated by dictionaries, after which
        # we will use a pd.from_records method to finalize the positions dataframe
        positions_list = []


        # iterate through the new portfolio object with the recent trading activity being included
        # perform the math for portfolio
        for row in range(1, len(portfolio.index)):
            portfolio.sort_index(ascending=True, inplace=True)
            # get get period
            end_date = portfolio.index[row] #.replace(tzinfo=None)
            start_date = portfolio.index[row - 1] #.replace(tzinfo=None)
            prior_cash = portfolio['cash'].iat[row - 1]
            daily_interest = self.ann_interest * (end_date - start_date) / timedelta(days=365)

            # set up a new starting cash balance and calculate any interest cost for leverage
            cash_interest_payment = 0 if prior_cash > 0 else prior_cash * daily_interest

            cash_change = 0
            gross_cash_change = 0
            long_realized_profit = 0
            short_realized_profit = 0
            new_cash = prior_cash + cash_interest_payment
            # get the previous portfolio value
            prior_portfolio_usd = portfolio['usd_value'].iat[row - 1]

            # grab the new trades (in scope of the time period)
            trading_key['market_entry_date_time'] = pd.to_datetime(trading_key['market_entry_date_time'])
            new_trades = trading_key[(trading_key['market_entry_date_time'] > start_date) &
                                     (trading_key['market_entry_date_time'] <= end_date)]

            new_trades = new_trades.sort_values(by = 'entry_date_time')

            # if we have trades
            if len(new_trades.index) > 0:
                for trade in range(0, len(new_trades.index)):
                    # first figure out the old vs. new position size and proposed cash impact
                    ticker = new_trades['symbol'].iat[trade]
                    price = new_trades['vwap'].iat[trade]
                    percent = new_trades['target_percentage'].iat[trade]
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

                    new_value = float(percent) * prior_portfolio_usd
                    cash_hit = prior_value - new_value
                    # now get feeAdj price
                    change_in_shares = (cash_hit * -1) / (price)

                    # now save the trade info
                    trading_key['prior_portfolio_value'].at[tradeId] = prior_portfolio_usd
                    #trading_key['target_position_value'].at[tradeId] = new_value
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


            active_trade_stats_df = self.get_recent_trade_stats(end_date, trading_key)

            active_prices_df = prices.loc[prices.delayed_trade_date == end_date.replace(hour=0)]

            active_prices_df = active_prices_df.sort_index().groupby('symbol').tail(1)

            active_prices_dict = active_prices_df[['symbol', 'vwap']].set_index('symbol').to_dict().get('vwap')


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
                dividendCash = self.getCashChangeFromDividends(end_date.replace(hour= 0, minute= 0, microsecond=0,
                                                                                second=0),
                                                             active_trade_stats_df['symbol'].unique())

                # check for splits
                trading_key = self.getEntryAndPostCumShareFromSplits(trading_key,
                                                                     end_date.replace(hour=0, minute=0, microsecond=0,
                                                                                      second=0), active_trade_stats_df.index)
                print('div-splits - EOD :' + str(start_date) + ' BOD :' + str(end_date))

            if dividendCash > 0:
                print("{} added in dividend cash on {}".format(dividendCash,end_date))

            new_cash += dividendCash


            active_trade_stats_df['price'] = active_trade_stats_df.apply(lambda x: active_prices_dict.get(x['symbol'],
                                                            x['current_position']/x['post_cumulative_share_count']), axis =1)


            active_trade_stats_df['position'] = active_trade_stats_df['price'] * \
                                                            active_trade_stats_df['post_cumulative_share_count']
            active_trade_stats_df['unrealized_pnl'] = (active_trade_stats_df['price'] - active_trade_stats_df['vwap'])*\
                                                      active_trade_stats_df['post_cumulative_share_count']

            active_trade_stats_df['position_pnl'] = active_trade_stats_df['unrealized_pnl'] + \
                                                    active_trade_stats_df['realized_profit']

            active_trade_stats_df['position_roi'] = active_trade_stats_df['position_pnl'] / \
                                                    (abs(active_trade_stats_df['position']) - active_trade_stats_df['unrealized_pnl'])

            active_trade_stats_df['percent_position'] = active_trade_stats_df['position'] / lastUsdValue

            active_trade_stats_df['position_long'] = active_trade_stats_df.apply(lambda x : x['position'] if x['position'] > 0 else 0, axis =1)

            active_trade_stats_df['unrealized_pnl_long'] = active_trade_stats_df.apply(lambda x: x['unrealized_pnl'] if x['position'] > 0 else 0, axis =1)

            active_trade_stats_df['position_short'] = active_trade_stats_df.apply(lambda x: x['position'] if x['position'] <= 0 else 0, axis =1)

            active_trade_stats_df['unrealized_pnl_short'] = active_trade_stats_df.apply(lambda x: x['unrealized_pnl'] if x['position'] <= 0 else 0, axis =1)


            trading_key['current_position'] = trading_key.update(active_trade_stats_df['current_position'])

            position = active_trade_stats_df['position'].sum()
            long_position = active_trade_stats_df['position_long'].sum()
            short_position = active_trade_stats_df['position_short'].sum()
            long_unrealized_pnl = active_trade_stats_df['unrealized_pnl_long'].sum()
            short_unrealized_pnl = active_trade_stats_df['unrealized_pnl_short'].sum()

            # Now save the new portfolio
            portfolio['positions_usd'].iat[row] = position
            portfolio['cash'].iat[row] = new_cash
            portfolio['usd_value'].iat[row] = new_cash + position
            portfolio['inception_return'].iat[row] = portfolio['usd_value'].iat[row] / portfolio['usd_value'].iat[
                row - 1] * portfolio['inception_return'].iat[row - 1]
            portfolio['gross_exposure_usd'].iat[row] = long_position - short_position
            portfolio['long_exposure_usd'].iat[row] = long_position
            portfolio['short_exposure_usd'].iat[row] = short_position
            portfolio['net_exposure_usd'].iat[row] = long_position + short_position
            portfolio['gross_exposure_percent'].iat[row] = (longPositions - shortPositions) / lastUsdValue
            portfolio['long_exposure_percent'].iat[row] = longPositions / lastUsdValue
            portfolio['short_exposure_percent'].iat[row] = shortPositions / lastUsdValue
            portfolio['net_exposure_percent'].iat[row] = (longPositions + shortPositions) / lastUsdValue

            portfolio['unrealized_long_pnl'].iat[row] = long_unrealized_pnl
            portfolio['unrealized_short_pnl'].iat[row] = short_unrealized_pnl
            portfolio['unrealized_pnl'].iat[row] = long_unrealized_pnl + short_unrealized_pnl

            portfolio['realized_long_pnl'].iat[row] = cash_interest_payment + dividendCash + long_realized_profit \
                                                      + portfolio['realized_long_pnl'].iat[row - 1]
            portfolio['realized_short_pnl'].iat[row] = short_realized_profit + portfolio['realized_short_pnl'].iat[
                row - 1]
            portfolio['realized_pnl'].iat[row] = cash_interest_payment + dividendCash + long_realized_profit + short_realized_profit + \
                                                    portfolio['realized_pnl'].iat[row - 1]


            portfolio['total_long_pnl'].iat[row] = long_unrealized_pnl + \
                                                    portfolio['realized_long_pnl'].iat[row]

            portfolio['total_short_pnl'].iat[row] = short_unrealized_pnl + \
                                                    portfolio['realized_short_pnl'].iat[row]

            portfolio['total_pnl'].iat[row] = long_unrealized_pnl + short_unrealized_pnl + portfolio['realized_pnl'].iat[row]

        #positions = pd.DataFrame.from_records(positions_list)

        self.trading_key = trading_key

        return portfolio

        # export to csv
    def export_to_csv(self, key: str = 'trading'):
        if key == 'trading':
            self.trading_key.to_csv('output/trading_key_test.csv', index=False)
        elif key == 'price':
            self.price_key.to_csv('output/price_key_test.csv')
        elif key == 'portfolio':
            self.portfolio.to_csv('output/portfolio_test.csv')
        elif key == 'crypto_check':
            self.unpriced_crypto.to_csv('checks/unpriced_crypto_test_{}.csv'.format(self.address))

if __name__ == '__main__':
    # start the timer
    start_time = time.time()
    # load environment variables (used below) that live in the .env file at the root of this project
    # load_dotenv()
    # environment variables, pulled from the .env file
    # address = os.getenv('WALLET')
    # infura_url = os.getenv('INFURA_URL') + '/' + os.getenv('INFURA_PROJECT_ID')
    # covey_ledger_polygon_address = os.getenv('COVEY_LEDGER_POLYGON_ADDRESS')
    # covey_ledger_skale_address = os.getenv('COVEY_LEDGER_SKALE_ADDRESS')
    # skale_url = os.getenv('SKALE_URL')
    # p = Portfolio(address='0xd019955e5Db68ebd41CE5A7A327DdD5f2658e8D9',
    #               infura_url=infura_url,
    #               skale_url=skale_url,
    #               covey_ledger_polygon_address=covey_ledger_polygon_address,
    #               covey_ledger_skale_address=covey_ledger_skale_address)

    #p = Portfolio(address='0x0d97A0E7e42eB70d013a2a94179cEa0E815dAE41')

    p = Portfolio(address='0x594F56D21ad544F6B567F3A49DB0F9a7B501FF37')

    p.export_to_csv(key='trading')
    p.export_to_csv(key='price')
    p.export_to_csv(key='portfolio')
    p.export_to_csv(key='crypto_check')

    print("---Portfolio finished in %s seconds ---" % (time.time() - start_time))