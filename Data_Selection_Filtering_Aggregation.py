import pandas as pd
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
path = script_dir + '/AirBnB_NY/'
print('THE PATH = ' + path)

def print_grouped_data(df, message=""):
    if message:
        print(message)
        print(" ")
    print(df)
    print("\n")
    print("-------------------------------------")


def select_by_loc(df):
    df_loc = df.loc[:, ('name', 'price', 'price_category', 'length_of_stay_category')]

    print_grouped_data(df_loc, 'Select using "loc"')


def select_by_iloc(df):
    df_iloc = df.iloc[0:10, [1, 9, 16, 17]]

    print_grouped_data(df_iloc, 'Select using "iloc"')

#select_by_iloc(df)


def extracting_manhattan_brooklyn(df):
    filtering_list = ['Manhattan', 'Brooklyn']
    df_extracted = df[df['neighbourhood_group'].isin(filtering_list)]

    print_grouped_data(df_extracted, 'Extracting Manhattan and Brooklyn only')

#extracting_manhattan_brooklyn(df)


def price_greather_hundred_num_of_rev_gretaher_ten(df):
    df_filter = df[(df['price'] > 100) & (df['number_of_reviews'] > 10)]

    print_grouped_data(df_filter, 'Extract where "price" greather 100 and "number_of_reviews" greather 10')

#price_greather_hundred_num_of_rev_gretaher_ten(df)



def create_df_with_specific_data(df):
    df = df[['neighbourhood_group', 'price', 'minimum_nights', 'number_of_reviews', 'price_category', 'availability_365']]

    print_grouped_data(df, 'Select neighbourhood_group, price, minimum_nights, number_of_reviews, price_category and availability_365')
    return df

#create_df_with_specific_data(df)


def grouped_and_average(df):
    group_df = df1.groupby(['neighbourhood_group', 'price_category'])
    grouped_df = group_df[['price', 'minimum_nights']].mean()

    print_grouped_data(grouped_df, 'Average price and minimum_nights')

#grouped_and_average(df)


#group_df[['number_of_reviews', 'availability_365']].mean()
def avg_number_of_reviews_and_availability_365(df):
    df_avg = df1.groupby(['neighbourhood_group', 'price_category']).agg(
                                            {'number_of_reviews': ['mean'], 'availability_365': ['mean']})

    print_grouped_data(df_avg, 'Compute the average "number_of_reviews" and "availability_365" for each group')

#avg_number_of_reviews_and_availability_365(df)


def sorting_asc_desc(df):
    df_sorted = df1.sort_values(['price', 'number_of_reviews'], ascending=[False, True])

    print_grouped_data(df_sorted, 'Sort the data by price in descending order and by number_of_reviews in ascending order.')
    return df_sorted
#sorting_asc_desc(df)


#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# Create a ranking of neighborhoods     based on the total number of listings       and the average price.

def df_ranking(df):
    # Group by neighbourhood_group for calculatio the total number of listings and average price
    grouped = df1.groupby('neighbourhood_group').agg(
                                total_listings=('price', 'size'),     # Total number of listings
                                average_price =('price', 'mean')      # Average price
                            ).reset_index()
    #display(grouped)

    # Total listing Rank
    grouped['listings_rank'] = grouped['total_listings'].rank(ascending=False)

    # average price Rank
    grouped['price_rank'] = grouped['average_price'].rank(ascending=False)
    #display(grouped)

    print_grouped_data(grouped, 'Create a ranking of neighborhoods based on the total number of listings and the average price.')

#df_ranking(df)



##### I'M NOT SURE I UNDARSTAND WHAT EXACTLY DATA SHOULD BE UNLOADED TO CSV FILE
def write_dataframe_to_csv(df):
    try:
        sorting_df.to_csv(path + 'aggregated_airbnb_data.csv', index=False)
        print('Success: The DataFrame was successfully written to csv file')
    except Exception as e:
        print('Fail: Failed to store file in csv')
        print(f'Error: {e}')



if __name__ == "__main__":
    df = pd.read_csv(path + 'cleaned_airbnb_data.csv', parse_dates=['last_review'])
    select_by_loc(df)
    select_by_iloc(df)
    extracting_manhattan_brooklyn(df)
    price_greather_hundred_num_of_rev_gretaher_ten(df)
    create_df_with_specific_data(df)
    df1 = create_df_with_specific_data(df)
    grouped_and_average(df)
    avg_number_of_reviews_and_availability_365(df)
    sorting_asc_desc(df)
    sorting_df = sorting_asc_desc(df)
    df_ranking(df)

    write_dataframe_to_csv(df)

