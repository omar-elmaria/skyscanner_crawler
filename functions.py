# Imports
import requests
import json
import pandas as pd
import os
import logging
import time
import warnings
warnings.filterwarnings(action="ignore")
from dotenv import load_dotenv
# Load environment variables
load_dotenv()

# Helper functions
def find_dict_key_from_value(val, dictionary):
    return next((key for key, value in dictionary.items() if value == val), None)

failed_airports_mapping_dict = {
    # Failed airports
    "Duesseldorf": "Düsseldorf",
    "Basel/Mulhouse": "Basel",
    "Cologne/Bonn": "Cologne",
    "Karlsruhe/Baden-Baden": "Karlsruhe",
    "Klaipeda/Palanga": "Palanga",
    "Leipzig/Halle": "Leipzig",
    "Lourdes/Tarbes": "Tarbes",
    "Maastricht/Aachen (NL)": "Maastricht",
    "Muenster/Osnabrueck (DE) 00": "Münster",
    "Paderborn/Lippstadt": "Paderborn Lippstadt",
    "Preveza/Lefkada": "Preveza",

    # Cities that don't have a City ID in the API response
    "Palma de Mallorca": "Mallorca",
    "Kerkyra": "Corfu",
    "Irakleion": "Heraklion",
    "Eilat (IL)": "Eilat",
    "Tel Aviv-yafo": "Tel Aviv",
}

def modify_search_term(x):
    if x in list(failed_airports_mapping_dict.values()):
        return find_dict_key_from_value(val=x, dictionary=failed_airports_mapping_dict) # Return the value of the dictionary
    else:
        return x # Return the same value in the data frame or the key of the dictionary

# Global inputs
num_api_attemps = 3
api_request_wait = 20
df_routes = pd.read_excel("flight_routes.xlsx")
df_routes.columns = df_routes.columns.str.lower().str.replace(" ", "_")

# Pull the airport data from airport_date.xlsx
df_airport_data_excel = pd.read_excel("airport_data.xlsx")
df_airport_data_excel["search_term_original"] = df_airport_data_excel["search_term"].apply(modify_search_term)

# Get flight data function
def get_flight_data(origin_airport, destination_airport, departure_date):
	url = "https://skyscanner50.p.rapidapi.com/api/v1/searchFlights"

	querystring = {
		"origin": origin_airport,
		"destination": destination_airport,
		"date": departure_date,
		"adults": "1",
		"cabinClass":"economy",
		"filter": "best",
		"currency": "EUR",
	}

	headers = {
		"X-RapidAPI-Key": os.getenv("API_KEY"),
		"X-RapidAPI-Host": "skyscanner50.p.rapidapi.com"
	}

	response = requests.get(url, headers=headers, params=querystring)

	return response.json()

# Extract flight data by date function
# Create a for loop to call the API for each route pair
def extract_flight_data_by_date(crawling_date, crawling_range, flight_data_json_file_name, failed_routes_json_file_name, no_data_routes_json_file_name, routes_dataframe):
    list_flight_data = []
    list_no_data_routes = []
    list_failed_routes = []
    length_of_crawling_list = crawling_range[-1] + 1
    for idx in crawling_range:
        # Get the departure and arrival cities from df_routes
        departure_city = routes_dataframe.loc[idx, 'departure_city'] # This could be df_routes or df_failed_routes
        arrival_city = routes_dataframe.loc[idx, 'arrival_city'] # This could be df_routes or df_failed_routes

        # Print a status message
        logging.info(f"Extracting the flight route data of destination airport {departure_city} and arrival airport {arrival_city}. This is route number {idx + 1} out of {length_of_crawling_list}")

        # Get the city IDs that will be inserted as parameters in the get_flight_data function
        departure_city_id = df_airport_data_excel[df_airport_data_excel["search_term_original"] == departure_city]["IataCode"].reset_index(drop=True)[0]
        arrival_city_id = df_airport_data_excel[df_airport_data_excel["search_term_original"] == arrival_city]["IataCode"].reset_index(drop=True)[0]

        # Get the airport data and create a data frame out of it
        try:
            output_flight_data = get_flight_data(origin_airport=departure_city_id, destination_airport=arrival_city_id, departure_date=crawling_date)["data"]
        # This KeyError can happen when the API fails to return a response with the given parameters
        # This could be because there are no flights on that date or the city IDs are incorrect or simply because the requests are too fast
        # We will retry for three times and if there is still no response, then we will return an error
        except KeyError:
            logging.info(f"A KeyError has occurred while attempting to extract the data for destination airport {departure_city} and arrival airport {arrival_city}")
            for i in range(num_api_attemps):
                # Print a status message informing the user of the number of retry attempts
                logging.info(f"Retry #{i + 1}. Waiting for {api_request_wait} seconds")
                
                # Wait for a certain number of seconds
                time.sleep(api_request_wait)
                
                # Repeat the API request
                try:
                    logging.info(f"Sending an API request for destination airport {departure_city} and arrival airport {arrival_city}")
                    output_flight_data = get_flight_data(origin_airport=departure_city_id, destination_airport=arrival_city_id, departure_date=crawling_date)["data"]
                except KeyError:
                    # If the API request does not succeed, check if the number of attempts has been exhaused
                    if i <= (num_api_attemps-1):
                        logging.info(f"Total number of attempts is {i + 1}, which is still less than {num_api_attemps}. Continuing to the next iteration")
                        pass
                    else:
                        logging.info(f"A PERMANENT KeyError has occurred while attempting to extract the data for destination airport {departure_city} and arrival airport {arrival_city}. Appending the failed_routes list")
                        output_dict = {
                            "departure_city": departure_city,
                            "arrival_city": arrival_city,
                            "origin_city_id": departure_city_id,
                            "arrival_city_id": arrival_city_id,
                            "crawling_date": crawling_date
                        }
                        list_failed_routes.append(output_dict)
                        
                        # Write the result to a JSON file
                        with open(f"{failed_routes_json_file_name}.json", mode="w", encoding="utf-8") as f:
                            json.dump(obj=list_failed_routes, fp=f, indent=4, ensure_ascii=False)
                            f.close()
            
                # If the API request succeeds, exit the for loop and continue with the rest of the code
                if len(output_flight_data) > 0:
                    break
        
        # If output_flight_data contains data, append it to list_output_dict and populate flight_data_json_file_name.json. If not, add the route to the list of routes with no data
        if output_flight_data != []:
            # Loop over all the results in "data"
            list_output_dict = []
            for idx_2, res in enumerate(output_flight_data):
                logging.info(f"Extracting flight data from result number {idx_2 + 1} out of {len(output_flight_data)}. This is for destination airport {departure_city} and arrival airport {arrival_city}, which is route number {idx + 1} out of {length_of_crawling_list}")
                output_dict = {
                    "price_eur": res["price"]["amount"],
                    "origin_airport_name": res["legs"][0]["origin"]["name"],
                    "origin_airport_display_code": res["legs"][0]["origin"]["display_code"],
                    "arrival_airport_name": res["legs"][0]["destination"]["name"],
                    "arrival_airport_display_code": res["legs"][0]["destination"]["display_code"],
                    "flight_departure_time": res["legs"][0]["departure"],
                    "flight_arrival_time": res["legs"][0]["arrival"],
                    "competitor": res["legs"][0]["carriers"][0]["name"],
                    "flight_duration": res["totalDuration"],
                    "origin_city_search_term": departure_city,
                    "arrival_city_search_term": arrival_city,
                    "origin_city_id": departure_city_id,
                    "arrival_city_id": arrival_city_id,
                    "crawling_date": crawling_date
                }
                list_output_dict.append(output_dict)
            
            # Append the result to airport_list
            list_flight_data.append(list_output_dict)

            # Write the result to a JSON file
            with open(f"{flight_data_json_file_name}.json", mode="w", encoding="utf-8") as f:
                json.dump(obj=list_flight_data, fp=f, indent=4, ensure_ascii=False)
                f.close()
        else:
            logging.info(f"There is no data for destination airport {departure_city} and arrival airport {arrival_city}. Appending to the list of routes with no data...")
            output_dict = {
                "departure_city": departure_city,
                "arrival_city": arrival_city,
                "origin_city_id": departure_city_id,
                "arrival_city_id": arrival_city_id,
                "crawling_date": crawling_date
            }
            list_no_data_routes.append(output_dict)

            # Write the result to a JSON file
            with open(f"{no_data_routes_json_file_name}.json", mode="w", encoding="utf-8") as f:
                json.dump(obj=list_no_data_routes, fp=f, indent=4, ensure_ascii=False)
                f.close()
        
        # Print a new line to mark the start of a new route
        logging.info("\n")
        
        # Wait one second between each request and the next
        time.sleep(1)

    return
