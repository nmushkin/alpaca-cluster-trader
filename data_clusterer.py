from datetime import datetime, timedelta
from os import path
import requests
import io
from json import dump

import pandas as pd
from numpy import sort as np_sort
from numpy import median as np_median
from numpy import mean as np_mean
from sklearn.neighbors import NearestNeighbors
from sklearn.cluster import DBSCAN
import matplotlib.pyplot as plt


class Scraper:
    def __init__(self):
        pass

    # Assuming NASDAQ symbol listing data txt files
    def load_symbols_nasdaq(self, filenames: list):
        symbol_list = []
        for filename in filenames:
            df = pd.read_csv(filename, sep="|")
            print(df.head())
            if "Symbol" in df.columns:
                symbol_list.extend(df["Symbol"])
            elif "ACT Symbol" in df.columns:
                symbol_list.extend(df["ACT Symbol"])
        return symbol_list

    def yahoo_history_link(self, symbol, from_timestamp, to_timestamp):
        return f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1={from_timestamp}&period2={to_timestamp}&interval=1d&events=history"

    def download_symbol_history(self, symbol, session, from_timestamp, to_timestamp):
        download_link = self.yahoo_history_link(symbol, from_timestamp, to_timestamp)
        response = session.get(download_link)
        if response:
            return pd.read_csv(io.StringIO(response.text))
        else:
            print(response)
            return pd.DataFrame()

    def download_all(
        self,
        prev_df=None,
        t_delta_seconds=None,
        symbol_list=None,
        filenames=None,
        out_dir="./data",
    ):
        prev_df = pd.read_csv(prev_df) if prev_df else pd.DataFrame()
        symbols = self.load_symbols_nasdaq(filenames) if filenames else symbol_list
        now_t = datetime.now().timestamp()
        to_timestamp = int(now_t - timedelta(days=1).total_seconds())
        period = (
            t_delta_seconds if t_delta_seconds else timedelta(days=365).total_seconds()
        )
        from_timestamp = int(now_t - period)
        session = requests.Session()
        for symbol in symbols:
            print(symbol)
            if symbol in prev_df.columns:
                continue
            history = self.download_symbol_history(
                symbol, session, from_timestamp, to_timestamp
            )
            if not history.empty:
                prev_df[symbol] = history["Adj Close"]
        fname = path.join(out_dir, f"{now_t}-out.csv")
        prev_df.to_csv(fname)
        return fname


class QuantClusterer:
    def __init__(self, history_csv_fpath):
        self.df = pd.read_csv(history_csv_fpath)
        self.groups = None
        self.pct_change_frame = None
        self._process_stock_dframe()

    def _process_stock_dframe(self):
        print("Cleaning DF")
        self.df.dropna(inplace=True, axis=1)
        self.df.drop(list(self.df.filter(regex="Unnamed")), axis=1, inplace=True)
        self.pct_change_frame = self.df.pct_change().drop(0)
        self.pct_change_frame = self.pct_change_frame.transpose()
        self.pct_change_frame = (self.pct_change_frame[:] > 0).astype(int)

    def _neighbor_distance(self):
        print("Finding Neighbor Distance")
        neighbors = NearestNeighbors(n_neighbors=2, n_jobs=8).fit(self.pct_change_frame)
        distances, indices = neighbors.kneighbors(self.pct_change_frame)
        distances = np_sort(distances, axis=0)
        distances = distances[:, 1]
        print(f"Median: {np_median(distances)}")
        print(f"Mean: {np_mean(distances)}")
        plot = plt.figure()
        plt.plot(distances)
        plot.show()
        chosen_dist = float(input("Choose Neighborhood Distance: "))
        print(chosen_dist)
        return chosen_dist

    def generate_clusters(self):
        neighbor_distance = self._neighbor_distance()
        print("Generating Clusters")
        self.pct_change_frame["cluster"] = (
            DBSCAN(eps=neighbor_distance, n_jobs=8).fit(self.pct_change_frame).labels_
        )
        # self.pct_change_frame['cluster'] = OPTICS().fit(self.pct_change_frame).labels_
        self.groups = self.pct_change_frame.groupby("cluster").groups
        return self.groups

    def save_groups(self, out_fpath):
        with open(out_fpath, "w") as json_file:
            dump(
                {
                    name: list(symbols) for name, symbols in self.groups.items() if name >= 0
                },
                json_file,
            )
