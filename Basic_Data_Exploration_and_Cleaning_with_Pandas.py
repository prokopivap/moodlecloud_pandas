import pandas as pd
import numpy as np
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
path = script_dir + '/AirBnB_NY/'
print('THE PATH = ' + path)

def print_dataframe_info(df, message=""):
    if message:
        print(message)
        print(" ")
    print(df)
    print("\n")
    print("-------------------------------------")


def basic_dataframe_info(df):
    df_inf = df.info()

    print_dataframe_info(df_inf, '')


def amt_of_na_values(df):
    df_na = df.isnull().sum()

    print_dataframe_info(df_na, 'Amount of NA values across all DataFrame')


def replace_na_for_name_and_hostname(df):
    df[['name', 'host_name']] = df[['name', 'host_name']].fillna(value='Unknown')
    df_replacing_names = df.isnull().sum()

    print_dataframe_info(df_replacing_names, 'Replacing NA for "name" and "hos_name" columns')


def replace_na_for_last_review(df):
    #change  datatype of column "last_review" from object to datetime (to having NaT value)
    df['last_review'] = pd.to_datetime(df['last_review'])
    df.dtypes
    df['last_review'] = df['last_review'].fillna(value=pd.NaT)
    df_lr = df['last_review'].head(30)

    print_dataframe_info(df_lr, 'Replacing NA for "last_review" column')



def categorise_price_ranges(df):
    df['price_category'] = np.where(df['price'] < 100, 'Low',
                              np.where(df['price'].between(100, 300), 'Medium', 'High')
                           )

    df_categoresed = df[['price', 'price_category']]

    print_dataframe_info(df_categoresed, 'Categorise "price" ranges')



def minimum_nights(df):
    df['length_of_stay_category'] = np.where(df['minimum_nights'] <= 3, 'short-term',
                                      np.where(df['minimum_nights'].between(4, 14), 'medium-term', 'long-term')
                                    )

    df_categorise_night_amt = df[['minimum_nights', 'length_of_stay_category']]

    print_dataframe_info(df_categorise_night_amt, 'Categorise "minimum_nights" ranges')


def remove_price_equal_zero(df):
    #df[df['price'] <= 0]
    df = df.drop(df[df['price'] <= 0].index)
    df_check = df[df['price'] <= 0]

    print_dataframe_info(df_check, 'Remove "0" from "price" columns')


def write_dataframe_to_csv(df):
    try:
        df.to_csv(path + 'cleaned_airbnb_data.csv', index=False)
        print('Success: The DataFrame was successfully written to csv file')
    except Exception as e:
        print('Fail: Failed to store file in csv')
        print(f'Error: {e}')





if __name__ == "__main__":
    df = pd.read_csv(path + 'AB_NYC_2019.csv')
    print (df.head())
    basic_dataframe_info(df)
    amt_of_na_values(df)
    replace_na_for_name_and_hostname(df)
    replace_na_for_last_review(df)
    categorise_price_ranges(df)
    minimum_nights(df)
    remove_price_equal_zero(df)

    write_dataframe_to_csv(df)



