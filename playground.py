import datetime
import pandas as pd
from pymongo import MongoClient
import config

# MongoDB connection setup
client = MongoClient(config.SERVER_MONGO_URI)
db = client[config.DB_NAME]
collection = db[config.COLLECTION_METADATA]

# Define cutoff dates
now = datetime.datetime.now(datetime.timezone.utc)
one_week_ago = now - datetime.timedelta(days=7)

# Initialize KPI DataFrame
kpi_df = pd.DataFrame(index=['up_to_last_week', 'past_week', 'total'])

# Function to calculate KPIs
def calculate_kpis(start_date, end_date, row_label):
    cursor = collection.find({"start_time_utc": {"$gte": start_date, "$lt": end_date}})
    df_period = pd.DataFrame(list(cursor))

    # Patient count KPI
    kpi_df.loc[row_label, 'patient_count'] = len(df_period)

    # Night event count KPI
    night_start = datetime.datetime.combine(end_date.date() - datetime.timedelta(days=1), datetime.time(22, 0, 0), tzinfo=datetime.timezone.utc)
    night_end = datetime.datetime.combine(end_date.date(), datetime.time(6, 0, 0), tzinfo=datetime.timezone.utc)
    night_event_count = collection.count_documents({"start_time_utc": {"$lt": night_end}, "end_time_utc": {"$gt": night_start}})
    kpi_df.loc[row_label, 'night_event_count'] = night_event_count

    # Total hours KPI
    def calculate_hours(row):
        if pd.notnull(row['start_time_utc']) and pd.notnull(row['end_time_utc']):
            return (row['end_time_utc'] - row['start_time_utc']).total_seconds() / 3600
        return 0
    kpi_df.loc[row_label, 'total_hours'] = df_period.apply(calculate_hours, axis=1).sum()

    # Weighted averages KPI
    weighted_avg_df = df_period[['evaluation', 'duration']].dropna(subset=['evaluation', 'duration'])
    if not weighted_avg_df.empty:
        weighted_avg_df = pd.json_normalize(weighted_avg_df['evaluation']).join(weighted_avg_df['duration'])
        parameters = weighted_avg_df.columns.drop('duration')

        for param in parameters:
            weighted_avg = (weighted_avg_df[param] * weighted_avg_df['duration']).sum() / weighted_avg_df['duration'].sum()
            kpi_df.loc[row_label, f'weighted_avg_{param}'] = weighted_avg

    # "In bed" count KPI
    kpi_df.loc[row_label, 'in_bed_count'] = df_period['bed'].isnull().sum()

# Calculate KPIs for up to last week
calculate_kpis(datetime.datetime.min.replace(tzinfo=datetime.timezone.utc), one_week_ago, 'up_to_last_week')

# Calculate KPIs for past week
calculate_kpis(one_week_ago, now, 'past_week')

# Calculate KPIs for total period
calculate_kpis(datetime.datetime.min.replace(tzinfo=datetime.timezone.utc), now, 'total')

# Display KPI DataFrame
print("KPI DataFrame:")
print(kpi_df)
