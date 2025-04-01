import datetime
import pandas as pd
import os
import platform
from pymongo import MongoClient
import config
from fpdf import FPDF

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

    kpi_df.loc[row_label, 'patient_count'] = len(df_period)

    night_start = datetime.datetime.combine(end_date.date() - datetime.timedelta(days=1), datetime.time(22, 0, 0), tzinfo=datetime.timezone.utc)
    night_end = datetime.datetime.combine(end_date.date(), datetime.time(6, 0, 0), tzinfo=datetime.timezone.utc)
    kpi_df.loc[row_label, 'night_event_count'] = collection.count_documents({"start_time_utc": {"$lt": night_end}, "end_time_utc": {"$gt": night_start}})

    def calculate_hours(row):
        if pd.notnull(row['start_time_utc']) and pd.notnull(row['end_time_utc']):
            return (row['end_time_utc'] - row['start_time_utc']).total_seconds() / 3600
        return 0

    kpi_df.loc[row_label, 'total_hours'] = df_period.apply(calculate_hours, axis=1).sum()

    weighted_avg_df = df_period[['evaluation', 'duration']].dropna(subset=['evaluation', 'duration'])
    if not weighted_avg_df.empty:
        weighted_avg_df = pd.json_normalize(weighted_avg_df['evaluation']).join(weighted_avg_df['duration'])
        parameters = weighted_avg_df.columns.drop('duration')

        for param in parameters:
            weighted_avg = (weighted_avg_df[param] * weighted_avg_df['duration']).sum() / weighted_avg_df['duration'].sum()
            kpi_df.loc[row_label, f'weighted_avg_{param}'] = weighted_avg

    kpi_df.loc[row_label, 'in_bed_count'] = df_period['bed'].isnull().sum()

# Calculate KPIs
calculate_kpis(datetime.datetime.min.replace(tzinfo=datetime.timezone.utc), one_week_ago, 'up_to_last_week')
calculate_kpis(one_week_ago, now, 'past_week')

# Display KPI DataFrame
print("KPI DataFrame:")
print(kpi_df)

# PDF Creation
pdf = FPDF()
pdf.add_page()

# Add timestamp at top right
pdf.set_font("Arial", '', 10)
current_time_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
pdf.cell(0, 10, current_time_str, ln=True, align='R')

# Add Title
pdf.set_font("Arial", 'B', 16)
pdf.cell(0, 10, 'Weekly Connection Report', ln=True, align='C')
pdf.ln(10)

# Function to create table in PDF
def create_table(title, df_row_label):
    pdf.set_font("Arial", 'B', 14)
    pdf.cell(0, 10, title, ln=True)

    pdf.set_font("Arial", 'B', 12)
    pdf.cell(60, 10, 'Metric', 1)
    pdf.cell(60, 10, 'Value', 1, ln=True)

    pdf.set_font("Arial", '', 12)
    for col in kpi_df.columns:
        value = kpi_df.loc[df_row_label, col]
        pdf.cell(60, 10, col, 1)
        pdf.cell(60, 10, f"{value:.2f}" if isinstance(value, float) else str(value), 1, ln=True)

    pdf.ln(10)

# Tables
create_table('Current Data Volume - Up to Last Week:', 'up_to_last_week')
create_table('Data Volume Only From Last Week:', 'past_week')

# Define network save path
platform_prefix = "N:\\"
if platform.system() != 'Windows':
    platform_prefix = "/Neteera/Work"

save_path = os.path.join(platform_prefix, "homes", "haggai.zohar", "Git")
os.makedirs(save_path, exist_ok=True)
pdf_filename = os.path.join(save_path, "weekly_connection_report.pdf")

# Save PDF (overwrite if exists)
pdf.output(pdf_filename)
print(f"PDF successfully saved to: {pdf_filename}")
