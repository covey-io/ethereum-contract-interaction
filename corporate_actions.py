import os
import base64
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

class CorporateActions:
    def __init__(self, **kwargs):
        load_dotenv()
        self.start_date = kwargs.get('start_date', (datetime.now() +timedelta(days=365)).strftime("%Y-%m-%d"))
        self.end_date = kwargs.get('end_date', datetime.now().strftime("%Y-%m-%d"))
        self.urls = self.generate_urls()
        self.mergers = self.get_mergers()

    # https://stackoverflow.com/questions/29721228/given-a-date-range-how-can-we-break-it-up-into-n-contiguous-sub-intervals
    def get_dates(self, intv):
        start = datetime.strptime(self.start_date, "%Y-%m-%d")
        end = datetime.strptime(self.end_date, "%Y-%m-%d")
        diff = (end - start) / intv
        days = diff.days + 1
        for i in range(days):
          yield (start + timedelta(days=intv) * i).strftime("%Y-%m-%d")
        yield end.strftime("%Y-%m-%d")

    def generate_urls(self):
        dates = list(self.get_dates(80))
        urls = []
        for d in range(1,len(dates)):
            urls.append('https://broker-api.sandbox.alpaca.markets/v1/corporate_actions/announcements?ca_types=merger&since={}&until={}'.format(
                  dates[d-1],dates[d]
            ))
        return urls

    def get_mergers(self):

        keys = os.environ['APCA_BROKER_API_KEY_ID'] + ':' + os.environ['APCA_BROKER_API_SECRET_KEY']
        keys_bytes = keys.encode('ascii')
        keys_64 = base64.b64encode(keys_bytes)
        payload={}
        headers = {'Authorization': 'Basic ' + keys_64.decode('ascii')}
        urls = self.generate_urls() if self.urls is None else self.urls

        json_dfs = []

        for u in urls:
            response = requests.request("GET", u, headers=headers, data=payload)
            df = pd.DataFrame.from_records(response.json())
            json_dfs.append(df)

        final_df = pd.concat(json_dfs)
        return final_df



if __name__== '__main__':

    ca = CorporateActions(start_date='2019-12-31', end_date='2022-07-14')

    ca.mergers.to_csv('data/mergers.csv')
