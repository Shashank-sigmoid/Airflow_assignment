import requests
import pandas as pd
import csv


def task_1():
    """ Fetch weather details for the states and stores it in a csv file """

    # Initialize the API URL endpoint and headers
    def initialize():
        url = "https://community-open-weather-map.p.rapidapi.com/weather"
        headers = {
            'x-rapidapi-host': "community-open-weather-map.p.rapidapi.com",
            'x-rapidapi-key': "86216fa808mshd873e09ff9c6e10p1c1249jsn0239eee2cdb5"
        }
        return url, headers

    # Read Excel file to store the states in a dataframe
    def read_excel():
        df = pd.read_excel("/Users/shashankdey/airflow-assignment/Indian states.xlsx")
        return df

    # Send HTTP requests to the API endpoint to fetch the weather data
    def fetch_weather(df, url, headers):
        data_list = []
        for row in df.iterrows():
            # Generating query string from the dataframe
            querystring = {"q": row[1]['Indian states']}
            response = requests.request("GET", url, headers=headers, params=querystring)
            result = response.json()
            data = [result['name'], result['weather'][0]['description'], result['main']['temp'], result['main']['feels_like'],
                    result['main']['temp_min'], result['main']['temp_max'], result['main']['humidity'], result['clouds']['all']]
            print(data)
            data_list.append(data)
        return data_list

    # Write the fetched data into a CSV file
    def write_csv_file(data):
        filename = "weather_data.csv"
        fields = ['State', 'Description', 'Temperature', 'Temperature feels like', 'Min temperature', 'Max temperature',
                  'Humidity', 'Clouds']
        with open(filename, 'w') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(fields)
            csvwriter.writerows(data)

    try:
        url, headers = initialize()
        df = read_excel()
        data_fetched = fetch_weather(df, url, headers)
        write_csv_file(data_fetched)

    except:
        print("Error in the connection")


if __name__ == '__main__':
    task_1()
