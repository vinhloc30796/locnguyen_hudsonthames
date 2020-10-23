# Import packages
import numpy as np
import pandas as pd
import yfinance as yf
import requests
from datetime import datetime

def get_sp500_tickers(filename='../data/sp500_tickers.txt'):
    """Download S&P500 tickers
    
    :param filename: str e.g. 'pricing.csv'
    :return sp500_tickers: list of ~500 strs e.g. ['AAPL', 'MSFT', etc.]
    """
    # Get file content from GitHub
    sp500_tickers_url = 'https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents_symbols.txt'
    request_sp500_tickers = requests.get(sp500_tickers_url)
    # Save file locally
    open(filename, 'wb').write(request_sp500_tickers.content)
    print(f'Saved file successfully at {filename}')
    # Return list of company tickers
    sp500_tickers = request_sp500_tickers.text.split('\n')
    return sp500_tickers

class StockUniverse:
    def __init__(self):
        self.indicator_translation = {
            'A': 'Adj Close',
            'O': 'Open',
            'H': 'High',
            'L': 'Low',
            'C': 'Close',
            'V': 'Volume'
        }
        return None
    
    def download_data(self, tickers, start="2017-01-01", end="2020-10-23"):
        """Using yfinance to download pricing data
        
        :param tickers: list of strings e.g. ['AAPL', 'MSFT', etc.]
        :return 
        """
        # Store characteristics
        self.tickers = tickers
        self.start = pd.Timestamp(start)
        self.end = pd.Timestamp(end)
        # Download the data
        tickers_str = ' '.join(tickers)
        self.df = yf.download(tickers_str, start=start, end=end)
        return self.df
    
    def store_pickle(
        self, 
        filename=f'../data/tickers_{datetime.today().strftime("%Y-%m-%d")}.pkl',
        indicator_select=['A','O','H','L','C','V']
    ):
        """Store self.df into a local pickle file
        
        :param filename: str e.g. 'pricing.pkl'
        :param indicator_select: list of str - which indicator(s) to save in the csv
            Possible strings are:
            A: Adj Close; O: Open; C: Close;
            H: High; L: Low, V: Volume
        :return filename
        """
        self.filename = filename
        # Select indicator(s) to save in the csv
        indicator_columns = [
            self.indicator_translation[each_selection] 
            for each_selection in indicator_select
        ]
        df_to_save = self.df.loc[:, indicator_columns]
        # Save to local
        df_to_save.to_pickle(self.filename)
        print(f'Saved file successfully at {self.filename}')
        return filename
    
    def read_pickle(
        self,
        filename=f'../data/tickers_{datetime.today().strftime("%Y-%m-%d")}.pkl',
        update_characteristics=True
    ):
        """Read a local pickle file into self.df
        
        :param filename: str e.g. 'pricing.pkl'
        :return self.df
        """
        # Read pickle
        self.filename = filename
        self.df = pd.read_pickle(filename)
        # Update characteristics if necessary
        if update_characteristics:
            self.tickers = self.df.columns.get_level_values(1) # Assumes that self.df.columns is MultiIndex
            self.start, self.end = self.df.index[[0,-1]] # Get start and end time
        return self.df

    def calc_normalized_returns(
        self,
        indicator_select=['A','O','H','L','C','V']
    ):
        """Read a local pickle file into self.df
        
        :param :param indicator_select: list of str - which indicator(s) 
            to calculate returns for.
            Possible strings are:
            A: Adj Close; O: Open; C: Close;
            H: High; L: Low, V: Volume
        :return self.df with the normalized returns
        accessible through e.g. self.df['Adj Close Returns']
        """
        # Select indicator(s) to save in the csv
        indicator_columns = [
            self.indicator_translation[each_selection] 
            for each_selection in indicator_select
        ]
        # Filter self.df on indicator_columns
        df_to_calc = self.df.loc[:, indicator_columns]
        # Calculate the returns
        returns_df = (df_to_calc - df_to_calc.shift())/(df_to_calc.shift())
        # Build new index and column names
        indicator_names = [
            each_indicator_name + ' Returns'
            for each_indicator_name 
            in df_to_calc.columns.get_level_values(0).unique()
        ]
        ticker_names = df_to_calc.columns.get_level_values(1).unique()
        index_names = df_to_calc.columns.names
        # Store returns_df in StockUniverse
        self.returns_df = pd.DataFrame(
            data=returns_df.values,
            index=df_to_calc.index,
            columns=pd.MultiIndex.from_product(
                [indicator_names, ticker_names],
                names=index_names
            )
        )
        return self.returns_df

if __name__ == "__main__":
    # Main
    sp500_tickers = get_sp500_tickers()
    sp500 = StockUniverse()
    sp500.download_data(sp500_tickers)
    sp500.store_pickle()