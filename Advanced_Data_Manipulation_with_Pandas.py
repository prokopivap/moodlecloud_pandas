import pandas as pd
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
path = script_dir + '/AirBnB_NY/'
print('THE PATH = ' + path)



def print_analysis_results(df, message=""):
    if message:
        print(message)
        print(" ")
    print(df)
    print("\n")
    print("-------------------------------------")




# Use the pivot_table function to create a detailed summary that reveals the
# average price for different combinations of neighbourhood_group and
# room_type. This analysis will help identify high-demand areas and optimize
# pricing strategies across various types of accommodations (e.g., Entire
# home/apt vs. Private room).

def pivoting_df(df):
    pivot_table = pd.pivot_table(
      df,
      values='price',                 # The data to aggregate
      index='neighbourhood_group',    # Rows will be neighbourhood groups
      columns='room_type',            # Columns will be room types
      aggfunc='mean'                  # Aggregate using mean to get the average price
    )

    print_analysis_results(pivot_table, 'Use the pivot_table function to create a detailed summary')
#pivoting_df(df)




# Transform the dataset from a wide format to a long format using the melt
# function.
# This restructuring facilitates more flexible and detailed analysis of
# key metrics like price and minimum_nights, enabling the identification of
# trends, outliers, and correlations.

def melting_df(df):
    df_melted = pd.melt(
        df,
        id_vars=['neighbourhood_group'],          # Columns to keep as identifiers
        value_vars=['price', 'minimum_nights'],   # Columns to unpivot
        var_name='metric',
        value_name='value'
    )

    print_analysis_results(df_melted, 'Transform the dataset from a wide format to a long format using the melt func.')
#melting_df(df)




#Create a new column availability_status using the apply function, classifying each listing into one of three
#categories based on the availability_365 column:
# ▪ "Rarely Available": Listings with fewer than 50 days of availability in a year.
# ▪ "Occasionally Available": Listings with availability between 50 and 200 days.
# ▪ "Highly Available": Listings with more than 200 days of availability.

#df[['neighbourhood_group', 'availability_365']]
def create_availability_status(df):
    def availability_status(availability_365):
        if availability_365 < 50:
            return "Rarely Available"
        elif availability_365 >= 50 and availability_365 < 200:
            return "Occasionally Available"
        else:
            return "Highly Available"

    df['availability_status'] = df['availability_365'].apply(availability_status)

    df_availability = df.copy()

    print_analysis_results(df_availability, 'Adding new column "availability_status"')
#create_availability_status(df)




# Analyze trends and patterns using the new availability_status column, and
# investigate potential correlations between availability and other key
# variables like price, number_of_reviews, and neighbourhood_group to
# uncover insights that could inform marketing and operational strategies.

def corr_on_price(df):
    corr_on_price = df['availability_365'].corr(df['price'])

    print_analysis_results(corr_on_price, 'Correlations between availability and price')


def corr_on_num_of_rev(df):
    corr_on_num_of_rev = df['availability_365'].corr(df['number_of_reviews'])

    print_analysis_results(corr_on_num_of_rev, 'Correlations between availability and number_of_reviews')
# corr_on_price(df)
# corr_on_num_of_rev(df)




# Neighbourhood_group and availability_status

def availability_by_neighbourhood(df):
    availability_by_neighbourhood = df.groupby(['neighbourhood_group', 'availability_status']).size()

    print_analysis_results(availability_by_neighbourhood, 'Availability by Neighbourhood Group:')
#availability_by_neighbourhood(df)




# Calculate average price by availability_status
# average_price_by_availability = df.groupby('availability_status')['price'].mean()

# print("Avg price by availability_status:")
# print(average_price_by_availability)

# Calculate average number of reviews by availability_status
# average_reviews_by_availability = df.groupby('availability_status')['number_of_reviews'].mean()

# print("Avg number_of_reviews by availability_status:")
# print(average_reviews_by_availability)




# Perform basic descriptive statistics (e.g., mean, median, standard deviation) on
# numeric columns such as price, minimum_nights, and number_of_reviews to
# summarize the dataset's central tendencies and variability, which is crucial for
# understanding overall market dynamics.

def summary_statistic(df):
    summary_statistics = df[['price', 'minimum_nights', 'number_of_reviews']].describe()

    print_analysis_results(summary_statistics, 'Basic descriptive statistics')
#summary_statistic(df)





def set_last_review_as_index(df):
    df['last_review'] = pd.to_datetime(df['last_review']) # convert to datetime
    df.set_index('last_review', inplace=True)

    df_new_index = df.copy()

    print_analysis_results(df_new_index, 'Set "last_review" as index')
    #set_last_review_as_index(df)




# Resample the data to observe monthly trends in the number of reviews and
# average prices, providing insights into how demand and pricing fluctuate
# over time.

def resample_df(df):
    monthly_reviews = df['number_of_reviews'].resample('M').sum()

    print_analysis_results(monthly_reviews, 'Monthly trends number_of_reviews')
#resample_df(df)




def monthly_trends_average_price(df):
    monthly_average_price = df['price'].resample('M').mean()

    print_analysis_results(monthly_average_price, 'Monthly average trends - price')
#monthly_average_price(df)




# Group the data by month to calculate monthly averages and analyze
# seasonal patterns, enabling better forecasting and strategic planning around
# peak periods.

def calculate_monthly_averages(df):
    df['month'] = df.index.month           #make colum with month number
    monthly_price_avg = df.groupby('month')[['price', 'number_of_reviews', 'minimum_nights']].mean()

    print_analysis_results(monthly_price_avg, 'Monthly average price')
#calculate_monthly_averages(df)




def write_dataframe_to_csv(df):
    try:
        df.to_csv(path + 'time_series_airbnb_data.csv', index=False)
        print('Success: The DataFrame was successfully written to csv file')
    except Exception as e:
        print('Fail: Failed to store file in csv')
        print(f'Error: {e}')



if __name__ == "__main__":
    df = pd.read_csv(path + 'AB_NYC_2019.csv')
    pivoting_df(df)
    melting_df(df)
    create_availability_status(df)
    corr_on_price(df)
    corr_on_num_of_rev(df)
    availability_by_neighbourhood(df)
    summary_statistic(df)
    set_last_review_as_index(df)
    resample_df(df)
    monthly_trends_average_price(df)
    calculate_monthly_averages(df)
    write_dataframe_to_csv(df)


