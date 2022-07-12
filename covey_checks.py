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
    df = df[df['symbol'].isin(alpaca_crypto_tickers)]

    # find the NaN prices
    df = df[df['vwap'].isnull()]

    return df