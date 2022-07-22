import time
import pandas as pd
from trade import Trade
import covey_checks as covey_checks
from dataclasses import make_dataclass
from covey_calendar import CoveyCalendar
from datetime import date, datetime,timedelta


class Portfolio(Trade):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # default start cash to 10000
        self.start_cash = kwargs.get('start_cash', 10000)
        # default annual interest to 0.2 %
        self.ann_interest = kwargs.get('ann_interest', 0.02)
        # initialize the portfolio
        self.reset_portfolio()
        # initialize trading_key portfolio derived columns
        self.set_trading_key()
        # generate crypto pricing error report
        self.unpriced_crypto = covey_checks.check_crypto_tickers(self.trading_key)
    
    def get_start_date(self, day_offset : int = 0) -> datetime:
        if len(self.trades.index) > 1:
            start_date = self.trading_key['market_entry_date_time'].min() + timedelta(days= day_offset)
            return start_date.replace(hour=0)
        else:
            return datetime(year=2021,month=12,day=31)

    def set_trading_key(self):
        self.trading_key = self.trading_key[~self.trading_key['vwap'].isnull()]
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
        return 0

    def reset_portfolio(self):
        # create the portfolio row entry dataclass 
        portfolio_entry = make_dataclass('portfolio_entry',
        [('date_time',datetime),
        ('cash',float),
        ('usd_value',float), 
        ('positions_usd',float),
        ('long_exposure_usd',float),
        ('short_exposure_usd',float),
        ('gross_traded_usd',float),
        ('net_traded_usd',float),
        ('unrealized_long_pnl',float), 
        ('unrealized_short_pnl',float),
        ('realized_long_pnl',float), 
        ('realized_short_pnl',float)])
        
        # initialize the first row of the portfolio - start date, start cash, and remanining 9 zeros 
        self.portfolio = pd.DataFrame([portfolio_entry(self.get_start_date(-1), self.start_cash, self.start_cash,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0)])
        
        # set the index to be date_time
        self.portfolio.set_index('date_time', inplace=True)

        # fill in the rest of the date index using covey calender market close times
        c = CoveyCalendar(start_date = self.get_start_date().strftime('%Y-%m-%d'))
        calendar_key = c.set_business_dates()
        max_calendar_date = min(self.price_key.reset_index()['timestamp'].max(),calendar_key['next_market_close'].max())
        calendar_mask = calendar_key['next_market_close'] <= max_calendar_date
        calendar_key_df = pd.DataFrame(calendar_key[calendar_mask]['next_market_close'].unique())
        
        # set the calendar index to be datetime so we can concat easily
        calendar_key_df.set_index(0, inplace=True)

        # attach the dates back to the original portfolio row
        self.portfolio = pd.concat([self.portfolio, calendar_key_df])

        return 0

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
        dividends_df['payment_date'] = pd.to_datetime(dividends_df['payment_date'])
        dividends_df = dividends_df[(dividends_df['div_or_split'] == 'dividend') & (dividends_df['payment_date'] == portfolio_date)][['symbol','amount']]
        
        # merge to main df (trading key) and calculate dividends
        df = pd.merge(left=df, right=dividends_df, on = 'symbol', how='left')
        df.rename(columns={'amount':'div_amount'}, inplace=True)
        df['dividend_cash'] = df['div_amount'] * df['post_cumulative_share_count']

        # split logic
        splits_df = pd.read_csv('data/dividend_split.csv')
        splits_df['payment_date'] = pd.to_datetime(splits_df['payment_date'])
        splits_df = splits_df[(splits_df['div_or_split'] == 'split') & (splits_df['payment_date'] == portfolio_date)][['symbol', 'amount']]
        df = pd.merge(left=df, right=splits_df, on = 'symbol', how='left')
        df.rename(columns={'amount':'split_amount'}, inplace=True)
        df['adjusted_entry'] = df['vwap']
        df['vwap'] = df['vwap'] * df['split_amount'] 
        df['post_cumulative_share_count'] = df['post_cumulative_share_count'] / df['split_amount'] 

        return df[['symbol', 'post_cumulative_share_count', 'vwap', 'current_position', 'realized_profit', 'dividend_cash']]

    def update_trading_key(self,df, prior_portfolio_usd):
        df = df.sort_values(by='entry_date_time', ascending = True)
        df['rank'] = df['entry_date_time'].rank(method = 'dense', ascending=False)

        # get the previous price 
        df['vwap_prev'] = df['vwap'].shift(-1).fillna(df['vwap'])

        # prior share count
        df['prior_cumulative_share_count'] = df['post_cumulative_share_count'].shift(-1).fillna(0)

        # filter df to grab the most recent trading block - since we already took all the prev values we needed 
        df_latest = df.loc[df['rank']==1]

        # prior portfolio value
        df_latest['prior_portfolio_value'] = prior_portfolio_usd

        # current position
        df_latest['current_position'] = float(df_latest['target_percentage']) * df_latest['prior_portfolio_value']

        # prior position value 
        df_latest['prior_position_value'] = df_latest['prior_cumulative_share_count'] * df_latest['vwap']
        
        # cash used
        df_latest['cash_used'] = df_latest['prior_position_value'] - df_latest['current_position']

        # share count
        df_latest['share_count'] = (df_latest['cash_used'] * -1) / df_latest['vwap']

        # post cumulative share count
        df_latest['post_cumulative_share_count'] = df_latest['prior_cumulative_share_count'] + df_latest['share_count']

        # update df from df_latest findings
        df.update(df_latest)

        # update most recent trade before the current one (if it exists) for realized profit, otherwise don't do anything
        if len(df.loc[df['rank']==2].index) > 0:
            df.loc[df['rank']==2, 'realized_profit'] = (df.loc[df['rank']==2,'vwap'] - df.loc[df['rank']==2,'vwap_prev']) * df.loc[df['rank']==2,'prior_cumulative_share_count']
        else:
            df.loc[df['rank']==1, 'realized_profit'] = 0

        # update main trading key
        self.trading_key.update(df)

        # update the portfolio for the current market_entry_date 
        self.portfolio.loc[self.portfolio.index.date == pd.to_datetime(df['market_entry_date_time'].unique()[0]).date(),
        ['gross_traded_usd','net_traded_usd','gross_traded_percent','net_traded_percent']] = \
        [df['cash_used'].abs().sum(), -1* df['cash_used'].sum(),df['cash_used'].abs().sum()/df['prior_portfolio_value'],
         -1 * df['cash_used'].sum()/df['prior_portfolio_value']]

        return 0


    def evaluate_portfolio_row(self, row):
        self.portfolio.sort_index(ascending=True)
        current_loc= self.portfolio.index.get_loc(row.index[0])
        # start date is really the previous date
        start_date = self.portfolio.index[current_loc - 1]
        end_date = self.portfolio.index[current_loc]
        print(end_date)
        
        # daily interest
        daily_interest = self.ann_interest * (end_date - start_date) / timedelta(days=365)

        # get the previous portfolio value
        prior_portfolio_usd = self.portfolio.loc[start_date,'usd_value']

        # get prior cash
        prior_cash = self.portfolio.loc[start_date,'cash']

        # set up a new starting cash balance and calculate any interest cost for leverage
        cash_interest_payment = 0 if prior_cash > 0 else prior_cash * daily_interest

        # set up new cash - will pass it into the trade processing
        new_cash = prior_cash + cash_interest_payment

        # isolate trades made so far
        trades_in_scope = self.trading_key[self.trading_key['market_entry_date_time'] <= end_date]
 
        # go through the new trades and see if they had history, update trading key fields accordingly
        trades_in_scope.groupby(['symbol','market_entry_date_time']).apply(self.update_trading_key,(prior_portfolio_usd))
        
        # update cash after most recent trades processed/updated
        new_cash += trades_in_scope['cash_used'].sum() + trades_in_scope['cash_used'].abs().sum() * -0.0005
        
        # look through active trades - these are the trades before the 'trades in scope'
        # we are updating their metrics now that we handled the new trades
        active_trade_stats_df = self.get_recent_trade_stats(end_date, self.trading_key)

        # get the most up to date prices for the tickers we already had trades for
        active_prices_df = self.price_key.loc[self.price_key.timestamp == end_date]

        # merge active trades with active prices to get the up to date values on all the 
        # existing positions
        active_df = pd.merge(left=active_trade_stats_df, right=active_prices_df, how='inner', on='symbol')

        # update portfolio active position(s) value
        self.portfolio[current_loc,'positions_usd'] = active_df['current_position'].sum()

        # update portfolio cash amount = prior cash + cash used in trading + dividends
        self.portfolio[current_loc,'cash'] = new_cash + active_df['dividend_cash']

    def calculate_portfolio(self):
        self.portfolio.iloc[1:,:].groupby(self.portfolio.index[1:]).apply(self.evaluate_portfolio_row)


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

    p = Portfolio(address='0x594F56D21ad544F6B567F3A49DB0F9a7B501FF37')

    print(p.portfolio)

    print(p.trading_key)

    p.calculate_portfolio()


    print("---Portfolio finished in %s seconds ---" % (time.time() - start_time))