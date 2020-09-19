"""Simple trading algothim that tries to hedge stocks
that typically move together in clusters.  Uses the Alpaca API
in order to execute orders and get market data.
"""

from json import load
from math import floor
from time import sleep

import alpaca_trade_api as tradeapi


class ClusterTrader():
    """Cluster-based trading algorithm

    Uses 'groups' or 'clusters' of stocks generated by a different
    module and implements a hedging strategy which seeks to exploit
    differences between stocks and their group centers.

    Params:
    -- json_groups_fname: a file containing groups of ticker symbols
    -- above_thresh: the amount (%) a stock must be above its cluster's
        mean pct change for the day in order to sell it (in the hope it
        retreats closer to the group mean)
    -- below_thresh: opposite of above_thresh
    -- ping_seconds: time in seconds between retreiving new data and
        updating positions
    -- max_group_size: the maximum number of symbols a group should have
        in order for the algorithm to trade symbols in it (likely larger
        groups have less predictable qualities, e.g. mean variance)
    -- auto_sell_pct_gain: the pct gain for each symbol that triggers
       liquidating all shares of that symbol and ceasing to trade it for
       the current day
    """

    API_KEY = ""
    API_SECRET = ""
    APCA_API_BASE_URL = "https://paper-api.alpaca.markets"

    def __init__(self,
                 json_groups_fname,
                 above_thresh=.1,
                 below_thresh=.1,
                 ping_seconds=300,
                 max_group_size=20,
                 auto_sell_pct_gain=2.0):

        with open(json_groups_fname) as fp:
            self.groups = load(fp)
        self.above_thresh = above_thresh
        self.below_thresh = below_thresh
        self.ping_seconds = ping_seconds
        self.max_group_size = max_group_size
        self.alpaca = tradeapi.REST(self.API_KEY,
                                    self.API_SECRET,
                                    self.APCA_API_BASE_URL,
                                    'v2')
        self.positions = {}
        self.last_symbol_prices = {}

    def load_group_data(self):
        buy_orders = []
        short_orders = []
        buy_back_orders = []
        sell_orders = []
        for idx, group in enumerate(self.groups.values()):
            print(f'Group {idx+1} of {len(self.groups)}')
            if len(group) > self.max_group_size:
                continue
            # Calculate mean and relative pct change for this group
            group_pct_changes = self.get_percent_changes(group)
            mean_change = sum(group_pct_changes.values()) / max(len(group_pct_changes), 1)
            print(f'Mean % Change: {mean_change}')
            for symbol, change in group_pct_changes.items():
                below_thresh = mean_change - change > self.below_thresh
                above_thresh = change - mean_change > self.above_thresh
                # If we have a position in the current asset
                if self.positions.get(symbol):
                    unrealized_pct = float(self.positions[symbol].unrealized_plpc) * 100
                    if unrealized_pct >= 2.0:
                        print(f'\nWill liquidate {symbol} for {unrealized_pct}% gain')
                        if self.positions[symbol].side == 'long':
                            group.remove(symbol)
                            sell_orders.append(symbol)
                        elif self.positions[symbol].side == 'short':
                            group.remove(symbol)
                            buy_back_orders.append(symbol)
                    elif above_thresh and self.positions[symbol].side == 'long':
                        group.remove(symbol)
                        sell_orders.append(symbol)
                    elif below_thresh and self.positions[symbol].side == 'short':
                        group.remove(symbol)
                        buy_back_orders.append(symbol)
                else:
                    if below_thresh:
                        buy_orders.append((symbol, mean_change - change))
                    elif above_thresh:
                        short_orders.append((symbol, change - mean_change))
        # Sort for future prioritization
        buy_orders.sort(key=(lambda c: c[1]))
        short_orders.sort(key=(lambda c: c[1]))
        return (buy_orders, short_orders, buy_back_orders, sell_orders)

    # Liquidates all held shares of a given asset
    def change_position(self, symbol, side):
        if symbol not in self.positions:
            print(f'{symbol} not found in positions for attempt to liquidate')
            return
        qty = int(self.positions[symbol].qty)
        self.submitOrder(qty, symbol, side)

    def update_positions(self):
        self.positions = {p.symbol: p for p in self.alpaca.list_positions()}
        print("Curent Positions:\n", self.positions)

    # Submit an order if quantity is above 0.
    def submitOrder(self, qty, stock, side, limit=None):
        order_type = 'limit' if limit else 'market'
        if(qty > 0):
            try:
                self.alpaca.submit_order(stock, qty, side, order_type, "day", limit_price=limit)
                print(f'Market order of {str(qty)} {stock} {side} completed.')
            except tradeapi.rest.APIError as e:
                print(e._error)
                print(f'Market order of {str(qty)} {stock} {side} did not go through.')
        else:
            print(f'Quantity is 0, order of {str(qty)} {stock} {side} not completed.')

    # Get percent changes of the stock prices over the past hour (15min * 4).
    # symbols can't be longer than 200 for api
    def get_percent_changes(self, symbols):
        pct_changes = {}
        try:
            bars = self.alpaca.get_barset(symbols, '15Min', 4)
            for symbol in bars:
                if len(bars[symbol]) > 0:
                    open_price = bars[symbol][0].o
                    close_price = bars[symbol][len(bars[symbol])-1].c
                    price_change = (close_price - open_price)
                    pct_changes[symbol] = price_change / open_price
                    self.last_symbol_prices[symbol] = close_price
        except tradeapi.rest.APIError as e:
            print(e._error)
        return pct_changes

    def get_quote(self, symbol):
        return self.alpaca.get_last_quote(symbol)

    def clear_open_orders(self):
        orders = self.alpaca.list_orders(status="open")
        print("Clearing orders ")
        for order in orders:
            self.alpaca.cancel_order(order.id)

    def get_available_cash(self):
        return float(self.alpaca.get_account().cash)

    def get_num_shares(self, symbol, amount):
        return floor(amount / float(self.last_symbol_prices[symbol]))

    def run_epoch(self):
        self.clear_open_orders()
        cash = floor(.4 * self.get_available_cash())
        print(f'Cash for current run: ${cash}')
        self.update_positions()
        buys, shorts, buy_backs, sells = self.load_group_data()
        print('buys:\n', buys)
        print('shorts\n', shorts)
        print('buy_backs\n', buy_backs)
        print('sells\n', sells)
        total_num = len(buys) + len(sells)
        # Naive way of calculating $ to put in each asset
        cash_per_stock = floor(cash / max(total_num, 1))
        # Close positions if applicable
        for sell, buy_back in zip(sells, buy_backs):
            self.change_position(sell, 'sell')
            self.change_position(buy_back, 'buy')
        # Buy new
        for symbol, diff in buys:
            qty = self.get_num_shares(symbol, cash_per_stock)
            self.submitOrder(qty, symbol, 'buy', self.last_symbol_prices[symbol])
        # Short new
        for symbol, diff in shorts:
            qty = self.get_num_shares(symbol, cash_per_stock)
            self.submitOrder(qty, symbol, 'sell')

    # To be called in a thread (preferably)
    def run(self):
        while True:
            self.run_epoch()
            sleep(self.ping_seconds)