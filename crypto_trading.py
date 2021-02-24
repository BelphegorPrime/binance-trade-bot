#!python3
import configparser
import datetime
import json
import math
import os
import queue
import random
import time
import traceback

import requests
from binance.client import Client
from binance.exceptions import BinanceAPIException
from sqlalchemy.orm import Session
from crypto_logger import Logger

from database import set_coins, set_current_coin, get_current_coin, get_pairs_from, \
    db_session, create_database, get_pair
from models import Coin, Pair

# Config consts
CFG_FL_NAME = 'user.cfg'
USER_CFG_SECTION = 'binance_user_config'

# Init config
config = configparser.ConfigParser()
if not os.path.exists(CFG_FL_NAME):
    print('No configuration file (user.cfg) found! See README.')
    exit()
config.read(CFG_FL_NAME)

BRIDGE_SYMBOL = config.get(USER_CFG_SECTION, 'bridge')
BRIDGE = Coin(BRIDGE_SYMBOL)

Logger.log('Started')

supported_coin_list = []

# Get supported coin list from supported_coin_list file
with open('supported_coin_list') as f:
    supported_coin_list = f.read().upper().splitlines()

# Init config
config = configparser.ConfigParser()
if not os.path.exists(CFG_FL_NAME):
    print('No configuration file (user.cfg) found! See README.')
    exit()
config.read(CFG_FL_NAME)


def retry(howmany):
    def tryIt(func):
        def f(*args, **kwargs):
            time.sleep(1)
            attempts = 0
            while attempts < howmany:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print("Failed to Buy/Sell. Trying Again.")
                    if attempts == 0:
                        Logger.log(e)
                        attempts += 1

        return f

    return tryIt


def first(iterable, condition=lambda x: True):
    try:
        return next(x for x in iterable if condition(x))
    except StopIteration:
        return None


def get_all_market_tickers(client):
    '''
    Get ticker price of all coins
    '''
    return client.get_all_tickers()


def get_market_ticker_price(client, ticker_symbol):
    '''
    Get ticker price of a specific coin
    '''
    for ticker in client.get_symbol_ticker():
        if ticker[u'symbol'] == ticker_symbol:
            return float(ticker[u'price'])
    return None


def get_market_ticker_price_from_list(all_tickers, ticker_symbol):
    '''
    Get ticker price of a specific coin
    '''
    ticker = first(all_tickers, condition=lambda x: x[u'symbol'] == ticker_symbol)
    return float(ticker[u'price']) if ticker else None


def get_currency_balance(client: Client, currency_symbol: str):
    '''
    Get balance of a specific coin
    '''
    for currency_balance in client.get_account()[u'balances']:
        if currency_balance[u'asset'] == currency_symbol:
            return float(currency_balance[u'free'])
    return None


@retry(20)
def buy_alt(client: Client, alt: Coin, crypto: Coin):
    '''
    Buy altcoin
    '''
    alt_symbol = alt.symbol
    crypto_symbol = crypto.symbol
    ticks = {}
    for filt in client.get_symbol_info(alt_symbol + crypto_symbol)['filters']:
        if filt['filterType'] == 'LOT_SIZE':
            ticks[alt_symbol] = filt['stepSize'].find('1') - 2
            break

    order_quantity = ((math.floor(get_currency_balance(client, crypto_symbol) *
                                  10 ** ticks[alt_symbol] / get_market_ticker_price(client,
                                                                                    alt_symbol + crypto_symbol)) / float(
        10 ** ticks[alt_symbol])))
    Logger.log('BUY QTY {0}'.format(order_quantity))

    # Try to buy until successful
    order = None
    while order is None:
        try:
            order = client.order_limit_buy(
                symbol=alt_symbol + crypto_symbol,
                quantity=order_quantity,
                price=get_market_ticker_price(client, alt_symbol + crypto_symbol)
            )
            Logger.log(order)
        except BinanceAPIException as e:
            Logger.log(e)
            time.sleep(1)
        except Exception as e:
            Logger.log("Unexpected Error: {0}".format(e))

    order_recorded = False
    while not order_recorded:
        try:
            time.sleep(3)
            stat = client.get_order(symbol=alt_symbol + crypto_symbol, orderId=order[u'orderId'])
            order_recorded = True
        except BinanceAPIException as e:
            Logger.log(e)
            time.sleep(10)
        except Exception as e:
            Logger.log("Unexpected Error: {0}".format(e))
    while stat[u'status'] != 'FILLED':
        try:
            stat = client.get_order(
                symbol=alt_symbol + crypto_symbol, orderId=order[u'orderId'])
            time.sleep(1)
        except BinanceAPIException as e:
            Logger.log(e)
            time.sleep(2)
        except Exception as e:
            Logger.log("Unexpected Error: {0}".format(e))

    Logger.log('Bought {0}'.format(alt_symbol))

    return order


@retry(20)
def sell_alt(client: Client, alt: Coin, crypto: Coin):
    '''
    Sell altcoin
    '''
    alt_symbol = alt.symbol
    crypto_symbol = crypto.symbol
    ticks = {}
    for filt in client.get_symbol_info(alt_symbol + crypto_symbol)['filters']:
        if filt['filterType'] == 'LOT_SIZE':
            ticks[alt_symbol] = filt['stepSize'].find('1') - 2
            break

    order_quantity = (math.floor(get_currency_balance(client, alt_symbol) *
                                 10 ** ticks[alt_symbol]) / float(10 ** ticks[alt_symbol]))
    Logger.log('Selling {0} of {1}'.format(order_quantity, alt_symbol))

    bal = get_currency_balance(client, alt_symbol)
    Logger.log('Balance is {0}'.format(bal))
    order = None
    while order is None:
        order = client.order_market_sell(
            symbol=alt_symbol + crypto_symbol,
            quantity=(order_quantity)
        )

    Logger.log('order')
    Logger.log(order)

    # Binance server can take some time to save the order
    Logger.log("Waiting for Binance")
    time.sleep(5)
    order_recorded = False
    stat = None
    while not order_recorded:
        try:
            time.sleep(3)
            stat = client.get_order(symbol=alt_symbol + crypto_symbol, orderId=order[u'orderId'])
            order_recorded = True
        except BinanceAPIException as e:
            Logger.log(e)
            time.sleep(10)
        except Exception as e:
            Logger.log("Unexpected Error: {0}".format(e))

    Logger.log(stat)
    while stat[u'status'] != 'FILLED':
        Logger.log(stat)
        try:
            stat = client.get_order(
                symbol=alt_symbol + crypto_symbol, orderId=order[u'orderId'])
            time.sleep(1)
        except BinanceAPIException as e:
            Logger.log(e)
            time.sleep(2)
        except Exception as e:
            Logger.log("Unexpected Error: {0}".format(e))

    newbal = get_currency_balance(client, alt_symbol)
    while (newbal >= bal):
        newbal = get_currency_balance(client, alt_symbol)

    Logger.log('Sold {0}'.format(alt_symbol))

    return order


def transaction_through_tether(client: Client, source_coin: Coin, dest_coin: Coin):
    '''
    Jump from the source coin to the destination coin through tether
    '''
    result = None
    while result is None:
        result = sell_alt(client, source_coin, BRIDGE)
    result = None
    while result is None:
        result = buy_alt(client, dest_coin, BRIDGE)

    set_current_coin(dest_coin)
    update_trade_threshold(client)


def update_trade_threshold(client: Client):
    '''
    Update all the coins with the threshold of buying the current held coin
    '''

    all_tickers = get_all_market_tickers(client)

    current_coin = get_current_coin()

    current_coin_price = get_market_ticker_price_from_list(all_tickers, current_coin + BRIDGE)

    if current_coin_price is None:
        Logger.log("Skipping update... current coin {0} not found".format(current_coin + BRIDGE))
        return

    session: Session
    with db_session() as session:
        for pair in session.query(Pair).filter(Pair.to_coin == current_coin):
            from_coin_price = get_market_ticker_price_from_list(all_tickers, pair.from_coin + BRIDGE)

            if from_coin_price is None:
                Logger.log("Skipping update for coin {0} not found".format(pair.from_coin + BRIDGE))
                continue

            pair.ratio = from_coin_price / current_coin_price


def initialize_trade_thresholds(client: Client):
    '''
    Initialize the buying threshold of all the coins for trading between them
    '''

    all_tickers = get_all_market_tickers(client)

    session: Session
    with db_session() as session:
        for pair in session.query(Pair).filter(Pair.ratio == None).all():
            if not pair.from_coin.enabled or not pair.to_coin.enabled:
                continue
            Logger.log("Initializing {0} vs {1}".format(pair.from_coin, pair.to_coin))

            from_coin_price = get_market_ticker_price_from_list(all_tickers, pair.from_coin + BRIDGE)
            if from_coin_price is None:
                Logger.log("Skipping initializing {0}, symbol not found".format(pair.from_coin + BRIDGE))
                continue

            to_coin_price = get_market_ticker_price_from_list(all_tickers, pair.to_coin + BRIDGE)
            if to_coin_price is None:
                Logger.log("Skipping initializing {0}, symbol not found".format(pair.to_coin + BRIDGE))
                continue

            pair.ratio = from_coin_price / to_coin_price


def scout(client: Client, transaction_fee=0.001, multiplier=5):
    '''
    Scout for potential jumps from the current coin to another coin
    '''

    all_tickers = get_all_market_tickers(client)

    current_coin = get_current_coin()

    current_coin_price = get_market_ticker_price_from_list(all_tickers, current_coin + BRIDGE)

    if current_coin_price is None:
        Logger.log("Skipping scouting... current coin {0} not found".format(current_coin + BRIDGE))
        return

    for pair in get_pairs_from(current_coin):
        if not pair.to_coin.enabled:
            continue
        optional_coin_price = get_market_ticker_price_from_list(all_tickers, pair.to_coin + BRIDGE)

        if optional_coin_price is None:
            Logger.log("Skipping scouting... optional coin {0} not found".format(pair.to_coin + BRIDGE))
            continue

        # Obtain (current coin)/(optional coin)
        coin_opt_coin_ratio = current_coin_price / optional_coin_price

        if (coin_opt_coin_ratio - transaction_fee * multiplier * coin_opt_coin_ratio) > pair.ratio:
            Logger.log('Will be jumping from {0} to {1}'.format(
                current_coin, pair.to_coin))
            transaction_through_tether(
                client, current_coin, pair.to_coin)
            break


def migrate_old_state():
    if os.path.isfile('.current_coin'):
        with open('.current_coin', 'r') as f:
            coin = f.read().strip()
            Logger.log(f".current_coin file found, loading current coin {coin}")
            set_current_coin(coin)
        os.rename('.current_coin', '.current_coin.old')
        Logger.log(f".current_coin renamed to .current_coin.old - You can now delete this file")

    if os.path.isfile('.current_coin_table'):
        with open('.current_coin_table', 'r') as f:
            Logger.log(f".current_coin_table file found, loading into database")
            table: dict = json.load(f)
            session: Session
            with db_session() as session:
                for from_coin, to_coin_dict in table.items():
                    for to_coin, ratio in to_coin_dict.items():
                        if from_coin == to_coin:
                            continue
                        pair = session.merge(get_pair(from_coin, to_coin))
                        pair.ratio = ratio
                        session.add(pair)

        os.rename('.current_coin_table', '.current_coin_table.old')
        Logger.log(f".current_coin_table renamed to .current_coin_table.old - You can now delete this file")


def main():
    api_key = config.get(USER_CFG_SECTION, 'api_key')
    api_secret_key = config.get(USER_CFG_SECTION, 'api_secret_key')
    tld = config.get(USER_CFG_SECTION, 'tld') or 'com' # Default Top-level domain is 'com'

    client = Client(api_key, api_secret_key, tld=tld)

    if not os.path.isfile('data/crypto_trading.db'):
        Logger.log("Creating database schema")
        create_database()

    set_coins(supported_coin_list)

    migrate_old_state()

    initialize_trade_thresholds(client)

    if get_current_coin() is None:
        current_coin_symbol = config.get(USER_CFG_SECTION, 'current_coin')
        if not current_coin_symbol:
            current_coin_symbol = random.choice(supported_coin_list)

        Logger.log("Setting initial coin to {0}".format(current_coin_symbol))

        if current_coin_symbol not in supported_coin_list:
            exit("***\nERROR!\nSince there is no backup file, a proper coin name must be provided at init\n***")
        set_current_coin(current_coin_symbol)

        if config.get(USER_CFG_SECTION, 'current_coin') == '':
            current_coin = get_current_coin()
            Logger.log("Purchasing {0} to begin trading".format(current_coin))
            buy_alt(client, current_coin, BRIDGE)
            Logger.log("Ready to start trading")

    while True:
        try:
            time.sleep(5)
            scout(client)
        except Exception as e:
            Logger.log('Error while scouting...\n{}\n'.format(traceback.format_exc()))


if __name__ == "__main__":
    main()
