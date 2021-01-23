# /**
#  * Author: Arun Thomas Varughese
#  * Last Modified: Jan 22, 2021
#
#  This file contains two classes: 1. Data_Extractor # 2. Data_Transformer

#  */



import urllib.request, json, io
import pandas as pd
import numpy as np

from multiprocessing.pool import ThreadPool
from time import time as timer
import datetime as dt
import concurrent.futures


# Data_Extractor: Extracts data from url and loads it in json format.
#

class Data_Extractor(object):

    def __init__(self, url_list=["https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD"]):

        self.url_list = url_list
        self.url = None
        #         print(url_list)
        if len(self.url_list) == 1:
            self.url = self.url_list[0]
            print("Url to be loaded:", self.url)

        self.data = None
        self.load_data()

    # load a single url
    def load_url(self, link):
        start = timer()
        try:
            with urllib.request.urlopen(link) as url:
                data = json.loads(url.read().decode())

                print("%r fetched in %ss" % (link, timer() - start))
            return data, None
        except Exception as e:
            return None, e

    # function to be used incase of multiple urls
    def load_multi_url(self, links, load_func=None, n_threads=20):

        if load_func is None:
            load_func = self.load_url

        results = ThreadPool(n_threads).imap_unordered(load_func, links)

        for data, error in results:
            if error is None:
                print("All urls loaded")
                return results
            else:
                print("error : %s" % (error))

    # load data
    def load_data(self):
        if self.url is not None:

            data, err_string = self.load_url(self.url)
            if err_string == None:

                self.data = data

            else:
                print(err_string)

        else:
            print("Multiple urls not in scope for this project")

    def get_data(self):
        return self.data


# Data_Transformer: Transforms json file into pandas dataframe and groups data based on county. It also converts json data to appropriate data types.
#  input args: data (json format), column_needed(list)
#  final output : df_dict (a dictionary of pandas dataframes)

class Data_Transformer(object):

    def __init__(self, data, columns_needed):

        self.data = data
        self.meta_data = data["meta"]
        self.row_data = data["data"]
        self.columns_needed = columns_needed
        self.columns = None
        self.col_ids = None
        self.df = None
        self.df_dict = None
        self.set_column_info()
        self.create_dataframe()
        self.transform_data()
        self.create_dataframelist()

    # set column names
    def set_column_info(self):
        columns = []
        col_ids = {}
        col_data = self.meta_data["view"]["columns"]
        val = 0
        for i in col_data:
            col_name = i["name"]
            columns.append(col_name)
            col_ids[col_name] = val
            val += 1
        self.columns = columns
        #         print(columns)
        self.col_ids = col_ids

    # create dataframe from rows data
    def create_dataframe(self):
        self.df = pd.DataFrame.from_records(self.row_data, columns=self.columns)

    #         print(self.df.head())

    # The main function used to transform data and remove unwanted columns
    def transform_data(self):

        df = self.df.copy()
        df = df[self.columns_needed]

        tdate = dt.date.today()
        df["Load date"] = tdate
        for i in [*df]:
            if i not in ["Test Date", "Load date", "County"]:
                df[i] = pd.to_numeric(df[i])

        df["Test Date"] = pd.to_datetime(df["Test Date"])
        self.df = df
        del df

    # split dataframe by grouping based on county
    def create_dataframelist(self, split_column='County'):
        df = self.df.copy()
        df_list = [x for _, x in df.groupby(split_column)]
        df_dict = {}
        for i in df_list:
            # Remove spaces and unwanted symbols from table name
            county = i.iloc[0][split_column].replace(" ", "").replace(".", "")
            #             print(county)
            df_dict[county] = i.drop(columns=[split_column])

        self.df_dict = df_dict
        del df_dict

    # return df_dict (a dictionary of pandas dataframes)
    def get_framedict(self):
        return self.df_dict