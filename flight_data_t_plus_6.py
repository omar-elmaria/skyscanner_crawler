from functions import extract_flight_data_by_date, df_routes
from datetime import datetime, timedelta

crawling_date = datetime.now().date() + timedelta(days=7) + timedelta(days=180)
crawling_date_formatted = datetime.strftime(crawling_date, "%Y-%m-%d")

extract_flight_data_by_date(
    crawling_date=crawling_date_formatted,
    crawling_range=range(0, len(df_routes)),
    flight_data_json_file_name="flight_data_t_plus_6",
    failed_routes_json_file_name="failed_routes_t_plus_6",
    no_data_routes_json_file_name="no_data_routes_t_plus_6.json",
    routes_dataframe=df_routes
)
