import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Load data
dim_date = pd.read_csv('/Users/vandana/Desktop/Hospitality Atliq/dim_date.csv')
dim_hotels = pd.read_csv('/Users/vandana/Desktop/Hospitality Atliq/dim_hotels.csv')
dim_rooms = pd.read_csv('/Users/vandana/Desktop/Hospitality Atliq/dim_rooms.csv')
fact_aggregated_bookings = pd.read_csv('/Users/vandana/Desktop/Hospitality Atliq/fact_aggregated_bookings.csv')
fact_bookings = pd.read_csv('/Users/vandana/Desktop/Hospitality Atliq/fact_bookings.csv')

# Convert 'date' in dim_date to datetime in the format YYYY-MM-DD
dim_date['date'] = pd.to_datetime(dim_date['date'], format='%d-%b-%y', errors='coerce')

# Verify conversion
print("Sample converted date values in dim_date:")
print(dim_date['date'].head(10))

# Merge fact_bookings with dim_hotels
combined_data = pd.merge(fact_bookings, dim_hotels, on='property_id', how='left')
print("After merging fact_bookings with dim_hotels:")
print(combined_data.head())

# Merge with dim_rooms using room_category to room_id
combined_data = pd.merge(combined_data, dim_rooms, left_on='room_category', right_on='room_id', how='left')
print("After merging with dim_rooms:")
print(combined_data.head())

# Convert booking_date and check_in_date to datetime
combined_data['booking_date'] = pd.to_datetime(combined_data['booking_date'], format='%Y-%m-%d', errors='coerce')
combined_data['check_in_date'] = pd.to_datetime(combined_data['check_in_date'], errors='coerce')

# Convert check_in_date in fact_aggregated_bookings to datetime
fact_aggregated_bookings['check_in_date'] = pd.to_datetime(fact_aggregated_bookings['check_in_date'], format='%d-%b-%y', errors='coerce')

# Check the data types to ensure they match
print(combined_data.dtypes)
print(fact_aggregated_bookings.dtypes)

# Merge with dim_date using booking_date to date
combined_data = pd.merge(combined_data, dim_date, left_on='booking_date', right_on='date', how='left')
print("After merging with dim_date:")
print(combined_data.head())

# Convert date formats in fact_aggregated_bookings
fact_aggregated_bookings['check_in_date'] = pd.to_datetime(fact_aggregated_bookings['check_in_date'], format='%d-%b-%y')

# Merge combined_data with fact_aggregated_bookings
combined_data = pd.merge(combined_data, fact_aggregated_bookings, on=['property_id', 'check_in_date', 'room_category'], how='left')
print("After merging with fact_aggregated_bookings:")
print(combined_data.head())

# After merging with fact_aggregated_bookings
print("Columns after merging with fact_aggregated_bookings:")
print(combined_data.columns)

# Check for missing values in 'successful_bookings' and 'capacity'
print("Checking missing values in 'successful_bookings' and 'capacity':")
print(combined_data[['successful_bookings', 'capacity']].isnull().sum())

# Check the merge keys and DataFrames
print("Unique property_ids in fact_bookings:")
print(fact_bookings['property_id'].unique())
print("Unique property_ids in dim_hotels:")
print(dim_hotels['property_id'].unique())
print("Unique room_category in fact_bookings:")
print(fact_bookings['room_category'].unique())
print("Unique room_id in dim_rooms:")
print(dim_rooms['room_id'].unique())

# Check for missing values in the original data
print("Missing values in fact_aggregated_bookings:")
print(fact_aggregated_bookings[['successful_bookings', 'capacity']].isnull().sum())
print("Missing values in dim_rooms:")
print(dim_rooms[['room_id']].isnull().sum())

# Display combined data
print("First few rows of combined_data after merge:")
print(combined_data.head(10))

# Display date range of combined_data
print("Date range of combined_data:")
print(f"From: {combined_data['booking_date'].min()} To: {combined_data['booking_date'].max()}")

# Display date range of dim_date
print("Date range of dim_date:")
print(f"From: {dim_date['date'].min()} To: {dim_date['date'].max()}")

# Dates in combined_data that are before the start of dim_date
early_dates = combined_data[combined_data['booking_date'] < '2022-05-01']['booking_date'].unique()
print("Dates in combined_data before 2022-05-01:")
print(early_dates)

# Drop rows where booking_date is before 2022-05-01
combined_data_filtered = combined_data[combined_data['booking_date'] >= '2022-05-01']

# Check for duplicates
print("Number of duplicate rows in combined_data:")
print(combined_data.duplicated().sum())

# Create a copy to avoid SettingWithCopyWarning
combined_data_filtered = combined_data_filtered.copy()

# Handle missing values
mean_rating = combined_data['ratings_given'].mean()
combined_data['ratings_given'] = combined_data['ratings_given'].fillna(mean_rating)

print("Missing values in combined_data_filtered:")
print(combined_data.isnull().sum())

# Ensure 'successful_bookings' and 'capacity' columns do not have missing values
print("Checking missing values in 'successful_bookings' and 'capacity' after filtering:")
print(combined_data_filtered[['successful_bookings', 'capacity']].isnull().sum())

# Check for remaining missing values
print("Missing values in combined_data_filtered:")
print(combined_data_filtered.isnull().sum())

# Summary statistics of combined_data_filtered
print("Summary statistics of combined_data_filtered:")
print(combined_data_filtered.describe())

# Define the directory path
output_directory = '/Users/vandana/Desktop/Hospitality_Atliq'

# Create the directory if it does not exist
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Define the full path for the output file
output_path = os.path.join(output_directory, 'combined_data_filtered.csv')

# Save the combined DataFrame to a CSV file
combined_data_filtered.to_csv(output_path, index=False)
print(f"DataFrame has been saved to CSV at {output_path}.")

# Define function to filter data
def filter_data(df, city=None, property_name=None, status=None, platform=None, month=None, week=None):
    if city:
        df = df[df['city'] == city]
    if property_name:
        df = df[df['property_name'] == property_name]
    if status:
        df = df[df['booking_status'] == status]
    if platform:
        df = df[df['booking_platform'] == platform]
    if month:
        df = df[df['date'].dt.month == month]
    if week:
        df = df[df['date'].dt.isocalendar().week == week]
    return df

# Apply filters as an example
filtered_data = filter_data(combined_data_filtered, city='Mumbai', month=7)

# Define function to calculate key metrics
def calculate_metrics(df):
    revenue = df['revenue_realized'].sum()
    occupancy = df['successful_bookings'].sum() / df['capacity'].sum() * 100 if df['capacity'].sum() > 0 else 0
    average_rating = df['ratings_given'].mean()
    return revenue, occupancy, average_rating

# Calculate metrics for filtered data
revenue, occupancy, avg_rating = calculate_metrics(filtered_data)
print(f"Revenue: {revenue}")
print(f"Occupancy %: {occupancy}")
print(f"Average Rating: {avg_rating}")

# Visualizations for Exploratory Data Analysis (EDA)
# Example: Distribution of revenue_generated
plt.figure(figsize=(10, 6))
sns.histplot(combined_data_filtered['revenue_generated'], bins=30, kde=True)
plt.title('Distribution of Revenue Generated')
plt.xlabel('Revenue Generated')
plt.ylabel('Frequency')
plt.show()

# Example: Ratings Given Distribution
plt.figure(figsize=(10, 6))
sns.histplot(combined_data_filtered['ratings_given'], bins=10, kde=True)
plt.title('Distribution of Ratings Given')
plt.xlabel('Ratings Given')
plt.ylabel('Frequency')
plt.show()

# Revenue, Occupancy %, and Average Rating Cards
def display_metrics(revenue, occupancy, avg_rating):
    print(f"Total Revenue: {revenue}")
    print(f"Occupancy %: {occupancy}")
    print(f"Average Rating: {avg_rating}")

# Display metrics for entire data
revenue, occupancy, avg_rating = calculate_metrics(combined_data)
display_metrics(revenue, occupancy, avg_rating)

# Revenue by City
city_revenue = combined_data.groupby('city')['revenue_generated'].sum().reset_index()
plt.figure(figsize=(14, 7))
sns.barplot(data=city_revenue, x='city', y='revenue_generated')
plt.title('Total Revenue by City')
plt.xlabel('City')
plt.ylabel

 #compute occupancy by city
city_occupancy = combined_data.groupby('city').agg(
    occupancy_rate=('successful_bookings', lambda x: (x.sum() / combined_data.loc[x.index, 'capacity'].sum()) * 100 if combined_data.loc[x.index, 'capacity'].sum() > 0 else 0)
).reset_index()
plt.figure(figsize=(14, 7))
sns.barplot(data=city_occupancy, x='city', y='occupancy_rate')
plt.title('Occupancy Rate by City')
plt.xlabel('City')
plt.ylabel('Occupancy Rate (%)')
plt.xticks(rotation=45)
plt.show()

# Revenue by Property
property_revenue = combined_data.groupby('property_name')['revenue_generated'].sum().reset_index()
plt.figure(figsize=(14, 7))
sns.barplot(data=property_revenue, x='revenue_generated', y='property_name', orient='h')
plt.title('Total Revenue by Property')
plt.xlabel('Total Revenue Generated')
plt.ylabel('Property Name')
plt.show()


# Compute occupancy by property

property_occupancy = combined_data.groupby('property_name').agg(occupancy_rate=('successful_bookings', lambda x: (x.sum() / combined_data.loc[x.index, 'capacity'].sum()) * 100 if combined_data.loc[x.index, 'capacity'].sum() > 0 else 0)).reset_index()
sns.barplot(data=property_occupancy, x='occupancy_rate', y='property_name', orient='h')
plt.title('Occupancy Rate by Property')
plt.xlabel('Occupancy Rate (%)')
plt.ylabel('Property Name')
plt.show()
print("plots for different Visualizations created")