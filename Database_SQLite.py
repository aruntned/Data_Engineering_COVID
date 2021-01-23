# /**
#  * Author: Arun Thomas Varughese
#  * Last Modified: Jan 22, 2021
#
#  This class is used to manage SQLite database.
#  It is used to load pandas dataframes to SQL and also check the rows that are needed to be updated.
#  It also has functions to load data from database to a pandas dataframe.
#
#  */



from multiprocessing.pool import ThreadPool
from time import time as timer
import datetime as dt
import concurrent.futures
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy.pool import QueuePool
from sqlalchemy.pool import NullPool
from sqlalchemy.pool import SingletonThreadPool
import pandas as pd




class Database_SQLite(object):

    def __init__(self, database_name, df_dict):

        self.df_dict = df_dict
        self.database_name = database_name

        self.engine = None
        self.conn = None
        self.server = None
        self.last_dates = None
        self.loaded_tables = {}
        self.tables_isempty = {}
        self.set_serverlink()
        self.start_engine()
        self.create_db_tables()
        self.set_echo()

    # set server name/link
    def set_serverlink(self):
        server = 'sqlite:///' + self.database_name
        self.server = server

    # start sqlalchemy engine and create connection
    def start_engine(self):
        engine = create_engine(self.server, echo=True)
        sqlite_connection = engine.raw_connection()
        self.engine = engine
        self.conn = sqlite_connection

    # create tables based on provided schema
    def create_db_tables(self):

        print("Creating databse tables if it does not exist")

        engine = self.engine
        df_dict = self.df_dict.copy()
        for i in df_dict.keys():

            if not engine.dialect.has_table(engine, i):
                meta = MetaData()

                i = Table(
                    i, meta,
                    #             Column('index', Integer),
                    Column('Test Date', String, primary_key=True),
                    Column('New Positives', Integer),
                    Column('Cumulative Number of Positives', Integer),
                    Column('Total Number of Tests Performed', Integer),
                    Column('Cumulative Number of Tests Performed', Integer),
                    Column('Load date', String))

                meta.create_all(engine)

    # change echo of server engine
    def set_echo(self, value=False):
        self.engine.echo = value

    # load_rows_to_df_pool: load database from sqlite and update loaded_tables variable
    # support multithreading/pooling
    def load_rows_to_df_pool(self, table_name, poolclass=QueuePool):

        engine1 = create_engine(self.server, echo=False, poolclass=poolclass)
        conn = engine1.raw_connection()
        select_string = 'select * from ' + table_name
        table = conn.execute(select_string)
        cols = [column[0] for column in table.description]
        df0 = pd.DataFrame.from_records(data=table.fetchall(), columns=cols)
        self.loaded_tables[table_name] = df0

    #         print('county:',table_name,' last date: ',last_date)

    # load_rows_to_df: load database from sqlite and update loaded_tables variable
    # used for single table (does not support multithreading/pooling)
    def load_rows_to_df(self, table_name):
        print("Load table to df", table_name)
        select_string = 'select * from ' + table_name
        table = self.conn.execute(select_string)
        cols = [column[0] for column in table.description]
        df0 = pd.DataFrame.from_records(data=table.fetchall(), columns=cols)
        #         last_date = table[-1][0]
        #         self.last_dates[table_name] = last_date
        self.loaded_tables[table_name] = df0
        print("Load completed for ", table_name)

    # Function for multithreading load_rows_to_df_pool function

    def load_tables_multi(self, load_func=None, n_threads=20):

        if load_func is None:
            load_func = self.load_rows_to_df_pool

        start = timer()
        counties = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
            # Start the load operations and mark each future with its URL
            future_to_sql = {executor.submit(load_func, table_name): table_name for table_name in self.df_dict.keys()}
            #         print(future_to_sql)
            for future in concurrent.futures.as_completed(future_to_sql):
                table_name = future_to_sql[future]

                try:
                    data = future.result()
                #                 eng.close()
                except Exception as exc:
                    print('%r generated an exception: %s' % (table_name, exc))
                else:
                    counties.append(table_name)

        #                 print('%r county loaded' % (table_name))

        print("Data loaded in %ss" % (timer() - start))
        print("Number of loaded tables : ", len(counties))

    # get last date from database for filtering update data
    def get_last_date_from_db(self, table_name):

        if self.engine.dialect.has_table(self.engine, table_name):

            select_string = 'SELECT "Test Date" FROM ' + table_name + ' ORDER BY "Test Date" DESC LIMIT 1'

            v1 = self.conn.execute(select_string).fetchall()
            if len(v1) == 0:
                return None
            else:

                last_date = v1[0][0]
                return last_date

    # set last keys or latest dates for each county
    def set_last_keys(self):
        print("Loading Last dates from database")

        start = timer()
        last_key = {}
        for i in self.df_dict.keys():
            ldate = self.get_last_date_from_db(i)
            if ldate is not None:
                last_key[i] = pd.to_datetime(ldate)

        print("keys loaded in %ss" % (timer() - start))
        self.last_dates = last_key

    # filter update data by removing already existing rows in database
    def filter_df_over_db(self):
        print("Filtering dataframe based on Test date")
        for table_name in self.df_dict.keys():
            df0 = self.df_dict[table_name].copy()
            ldate = self.last_dates[table_name]
            filter_mask = df0['Test Date'] > ldate
            df0 = df0[filter_mask]
            self.df_dict[table_name] = df0

    # A function used to multithread updating of database based on new data from url
    def add_rows_to_df_multi(self, table_name, mode='append', poolclass=QueuePool):

        df_dict = self.df_dict
        engine0 = create_engine(self.server, echo=False, poolclass=poolclass)
        #         print(df_dict[table_name])
        df_dict[table_name].to_sql(table_name, con=engine0, if_exists=mode, index=False)

        return table_name

    # enables multithreading of function:add_rows_to_df_multi for insertion into SQLite
    def insert_multi_sql(self, df_dict, load_func=None, mode='append', n_threads=10):

        start = timer()
        df_dict = self.df_dict
        counties = []
        if load_func is None:
            load_func = self.add_rows_to_df_multi
        df_dict = self.df_dict
        with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
            # Start the load operations and mark each future with its tablename
            future_to_sql = {executor.submit(load_func, table_name, mode): table_name for table_name in df_dict.keys()}
            #         print(future_to_sql)
            for future in concurrent.futures.as_completed(future_to_sql):
                table_name = future_to_sql[future]

                try:
                    data = future.result()

                except Exception as exc:
                    print('%r generated an exception: %s' % (table_name, exc))
                else:
                    counties.append(table_name)

        print("Database updated in %ss" % (timer() - start))
        print("Length of counties : ", len(counties))

    # Set df_dict
    def set_df_dict(self, df_dict):
        self.df_dict = df_dict

    # function used to update SQLite database
    def update_database(self, df_dict):
        if df_dict is None:
            df_dict = self.df_dict
        if df_dict is None:
            print("No new data....dataframes missing")
        else:
            print("Updating database")
            self.set_last_keys()
            print('length', len(self.last_dates.keys()))

            #  Filter dataframe if tables have rows
            if len(self.last_dates.keys()) != 0:
                self.filter_df_over_db()

            self.insert_multi_sql(df_dict)
            print("Database updated : ", self.database_name)

    # Load the entire database from SQLite
    def load_database_from_sqlite(self):
        self.load_tables_multi()

    # Get the entire database from SQLite
    # returns database as a dict of dataframes
    def get_database(self):
        self.load_database_from_sqlite()
        return self.loaded_tables

    # Get a single table from database
    def get_table(self, table_name):
        self.load_rows_to_df(table_name)
        return self.loaded_tables[table_name]