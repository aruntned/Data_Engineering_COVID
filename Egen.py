# /**
#  * Author: Arun Thomas Varughese
#  * Last Modified: Jan 22, 2021
#
# Main function to run the entire ETL process
#
#  */



from ETL import *
from Database_SQLite import Database_SQLite
import  sys

# init constants
URL_LIST = ["https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD"]
DATABASE = 'Egen_Data_Engineering.db'

# Function to enable ETL by initializing and calling the required ETL classes
# input: url_list(list of urls),dbase(string of server name/database name)
# return df0:the database from server as a dict of dataframes
def ETL(urls,dbase):


    needed_columns = ["Test Date", "New Positives", "Cumulative Number of Positives", "Total Number of Tests Performed",
                      "Cumulative Number of Tests Performed", "County"]


    data = Data_Extractor(urls).get_data()

    df_dict = Data_Transformer(data, needed_columns).get_framedict()
    print("length of county dict frame", len(df_dict))

    assert( len(df_dict) == 62),"Number of counties mismatch"

    db = Database_SQLite(database_name=dbase, df_dict=df_dict)
    db.update_database(df_dict)
    # df1 = db.get_table('NewYork')
    df0 = db.get_database()

    return df0



if __name__ == '__main__':
    # urls = sys.argv[1]
    # data_base_path = sys.argv[2]
    # dbase = data_base_path

    # if urls is None:
    #     urls = URL_LIST
    #
    # if dbase is None:
    #     dbase = DATABASE

    latest_db = ETL(URL_LIST,DATABASE)

    print("Printing database for NewYork County")
    print(latest_db["NewYork"])



