import requests
import json

def create_temp_file():
    # 1. Define the API endpoint URL
    url = f'https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/?api_key=PUTAPIKEYHERE&frequency=hourly&data[0]=value&start=2026-02-22T00&end=2026-02-28T00&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=50' 

    # 2. Make a GET request to the URL
    response = requests.get(url)

    # 3. Check the status code and process the response
    if response.status_code == 200:
        # Parse the JSON data into a Python dictionary
        data = response.json()
        #print("Data received:", data)
        with open("./data/temp_api_data.json", 'w') as json_file:
            json.dump(data["response"]["data"], json_file)

    else:
        print(f"Request failed with status code: {response.status_code}")