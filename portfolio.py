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

    def getMostRecentTrade(self, dateAsOf, tradingKey):
        tradingKeyCopy = tradingKey.fillna(0)
        shares = []
        entryPrice = []
        ids = []
        currentPosition = []
        realizedProfits = []
        asOf = (tradingKeyCopy['market_entry_date_time'] < dateAsOf)
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

        df = {'tickers':shares, 'prices': entryPrice , 'trade_id' : ids,
              'position_usd' : currentPosition, 'realized_profit' : realizedProfits}

        #print(dateAsOf)
        #print(pd.DataFrame(df))


        return shareList, priceList, idList, currentList, realizedList

    # now get the most recent rolling prices for the unique tickers and date
    def getRollingPrices(self, date, prices):
        allTickers = prices['symbol'].at[date]
        allPrices = prices['vwap'].at[date]
        try:
            oneTwoThree = dict(zip(allTickers, allPrices))
        except:
            oneTwoThree = {allTickers: allPrices}

        return oneTwoThree

    def getDayDividends(self, endDate):
        df = pd.read_csv('data/dividend_split.csv')
        df['payment_date'] = pd.to_datetime(df['payment_date'])
        df = df[(df['div_or_split'] == 'dividend') & (df['payment_date'] == endDate)][['symbol','amount']]
        df.set_index('symbol',inplace=True)
        df = df.to_dict()
        return df['amount']

    def getCashChangeFromDividends(self, endDate, activeTrades):
        activeDividends = self.getDayDividends(endDate)
        dividendCash = 0
        for trade in activeTrades.keys():
            if trade in activeDividends:
                dividendPayment = float(activeTrades[trade]) * float(activeDividends[trade])
            else :
                dividendPayment = 0
            dividendCash += dividendPayment
        return dividendCash

    def getDaySplits(self, endDate):
        df = pd.read_csv('data/dividend_split.csv')
        df['payment_date'] = pd.to_datetime(df['payment_date'])
        df = df[(df['div_or_split'] == 'split') & (df['payment_date'] == endDate)][['symbol', 'amount']]
        df.set_index('symbol', inplace=True)
        df = df.to_dict()
        return df['amount']

    def getEntryAndPostCumShareFromSplits(self, tradingKey, endDate, activeIds):
        activeSplits = self.getDaySplits(endDate)
        for trade in activeIds.keys():
            if trade in activeSplits:
                # update adjusted_entry
                tradingKey['adjusted_entry'].at[activeIds[trade]] = tradingKey['vwap'].at[activeIds[trade]]
                tradingKey['vwap'].at[activeIds[trade]] = tradingKey['vwap'].at[activeIds[trade]] * activeSplits[trade]
                tradingKey['post_cumulative_share_count'].at[activeIds[trade]] = tradingKey['post_cumulative_share_count'].at[activeIds[trade]] / activeSplits[trade]
        return tradingKey

    def updatePortfolioMath(self):

        # for testing
        #trading_key = trading_key[trading_key['trade_id'] == 1]

        trading_key = self.trading_key.copy()

        trading_key[[#'target_position_value',
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
        #new_portfolio = prices.copy()

        # calendar key 
        c = CoveyCalendar(start_date = prices['delayed_trade_date'].min())
        calendar_key = c.set_business_dates()
        calendar_key_df = pd.DataFrame(calendar_key[calendar_key['date'] < datetime.now().replace(hour=0,minute=0, second=0, microsecond =0)]['next_market_close'].unique()).set_index(0)
        calendar_key_df.index = calendar_key_df.index.append(pd.Index([prices.reset_index()['timestamp'].max()]))

        #new_portfolio.drop(columns=['vwap', 'symbol'], inplace=True)
        #new_portfolio.drop_duplicates(inplace=True)
        # new_portfolio['timestamp'] = new_portfolio['timestamp'].dt.tz_localize(None)
        # new_portfolio['max_symbol_timestamp'] = new_portfolio.groupby(['delayed_trade_date','symbol'])['timestamp'].transform(max)
        # new_portfolio['common_timestamp_denominator'] = new_portfolio.groupby('delayed_trade_date')['max_symbol_timestamp'].transform(min)
        # new_portfolio.drop(columns=['delayed_trade_date', 'timestamp','max_symbol_timestamp','vwap', 'symbol'], inplace=True)
        # new_portfolio.drop_duplicates(inplace=True)
        # new_portfolio.set_index('common_timestamp_denominator', inplace=True)
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

            # work with the active trades
            activeTrades, activeEntryPrice, activeIds, activePositions, activeRealized = self.getMostRecentTrade(end_date,
                                                                                                            trading_key)

            # get all prices for trades that have happened
            activePrices = self.getRollingPrices(end_date, prices)

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
                dividendCash = self.getCashChangeFromDividends(end_date.replace(hour=0, minute=0, microsecond=0,
                                                                                second=0),
                                                             activeTrades)

                # check for splits
                trading_key = self.getEntryAndPostCumShareFromSplits(trading_key,
                                                                     end_date.replace(hour=0, minute=0, microsecond=0,
                                                                                      second=0), activeIds)

                # check for mergers
                print('div-splits - EOD :' + str(start_date) + ' BOD :' + str(end_date))

            if dividendCash > 0:
                print("{} added in dividend cash on {}".format(dividendCash,end_date))

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
                new_row = {'date_time': end_date, 'user_id': self.address, 'symbol': trade,
                           'usd_position_value': position, 'percent_position': percentPosition,
                           'post_cumulative_share_count': cum_shares, 'price': price,
                           'entry_price': entry_price, 'unrealized_pnl': unrealized_pnl,
                           'pos_pnl': posPnl, 'pos_roi': posRoi, 'marker': marker}

                #positions = positions.append(new_row, ignore_index=True)
                positions_list.append(new_row)

                if position > 0:
                    longPositions += position
                    longUnrealizedPnl += unrealized_pnl
                else:
                    shortPositions += position
                    shortUnrealizedPnl += unrealized_pnl

                # update current position
                trading_key['current_position'].at[trade_id] = position

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
            portfolio['realized_long_pnl'].iat[row] = cash_interest_payment + dividendCash + long_realized_profit \
                                                      + portfolio['realized_long_pnl'].iat[row - 1]
            portfolio['realized_short_pnl'].iat[row] = short_realized_profit + portfolio['realized_short_pnl'].iat[
                row - 1]
            portfolio['realized_pnl'].iat[row] = cash_interest_payment + dividendCash + long_realized_profit + short_realized_profit + \
                                                    portfolio['realized_pnl'].iat[row - 1]

            portfolio['total_long_pnl'].iat[row] = longUnrealizedPnl + \
                                                    portfolio['realized_long_pnl'].iat[row]
            portfolio['total_short_pnl'].iat[row] = shortUnrealizedPnl + \
                                                    portfolio['realized_short_pnl'].iat[row]
            portfolio['total_pnl'].iat[row] = longUnrealizedPnl + shortUnrealizedPnl + portfolio['realized_pnl'].iat[row]

        positions = pd.DataFrame.from_records(positions_list)

        self.trading_key = trading_key

        return portfolio

        # export to csv
    def export_to_csv(self, key: str = 'trading'):
        if key == 'trading':
            self.trading_key.to_csv('output/trading_key.csv', index=False)
        elif key == 'price':
            self.price_key.to_csv('output/price_key.csv')
        elif key == 'portfolio':
            self.portfolio.to_csv('output/portfolio.csv')
        elif key == 'crypto_check':
            self.unpriced_crypto.to_csv('checks/unpriced_crypto_{}.csv'.format(self.address))

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
    p.export_to_csv(key='price')
    p.export_to_csv(key='trading')
    p.export_to_csv(key='portfolio')


    print("---Portfolio finished in %s seconds ---" % (time.time() - start_time))