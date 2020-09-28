# Libraries to import
import os
import requests
import pandas as pd
from datetime import datetime


# Perform GET request from data.gov.sg API (individually)
def do_request(reading, date, failed_dates, repeated=False):
    request = requests.get(f"https://api.data.gov.sg/v1/environment/{reading}?date={date}")
    if request.status_code == 200:
        with open(f"./{reading}/{date[0:4]}/{date}.json", "w", encoding="utf-8") as f:
            f.write(request.text)
        print(f"Completed writing {reading} for {date}")
        if repeated:
            failed_dates.remove(date)
    else:
        print(f"Unable to receive {reading} data for date: {date}")
        print(f"\tStatus code: {request.status_code}")
        if not repeated:
            failed_dates.append(date)


# Default start will be 2016-12-14, which is the start of the API coverage
# according to https://data.gov.sg/dataset/realtime-weather-readings
def run(start_date="2016-12-14", end_date=datetime.now().strftime("%Y-%m-%d"), threshold=10):
    # all dates from the start
    dates = pd.date_range(start_date, end_date, freq="d").strftime("%Y-%m-%d").tolist()

    # all unique years
    years = {date[0:4] for date in dates}

    readings = {"air-temperature", "rainfall", "wind-speed", "wind-direction", "relative-humidity"}

    # create directories and subdirectories
    for reading in readings:
        # create directories separate by readings
        if not os.path.isdir(f"./{reading}/"):
            os.mkdir(f"./{reading}/")
        # create subdirectories within readings, separate by years
        for year in years:
            if not os.path.isdir(f"./{reading}/{year}/"):
                os.mkdir(f"./{reading}/{year}/")

        # now we retrieve the data using the API from data.gov.sg
        failed_dates = []
        for date in dates:
            do_request(reading, date, failed_dates, False)

        epoch = 0
        while len(failed_dates) > 0:
            for failed_date in failed_dates:
                do_request(reading, failed_date, failed_dates, True)
            epoch += 1
            if epoch > threshold:
                print("Too many failed attempts. Some dates are still not retrieved.")
                print(f"Failed dates: {failed_dates}")


if __name__ == "__main__":
    start = input("Enter start date (in YYYY-MM-DD format): ")
    end = input("Enter end date (in YYYY-MM-DD format): ")
    run(start, end)
    print("Completed running script.")
