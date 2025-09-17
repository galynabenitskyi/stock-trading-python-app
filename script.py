
import requests
import os
from dotenv import load_dotenv
load_dotenv()
import csv

POLYGON_API_KEY=os.getenv('POLYGON_API_KEY')
LIMIT=1000

url=f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}"
response=requests.get(url)
tickers=[]

data=response.json()
for ticker in data.get('results', []):
    tickers.append(ticker)



while 'next_url' in data:
    print('requesting next page', data['next_url'])
    response=requests.get(data['next_url']+f'&apiKey={POLYGON_API_KEY}')   
    data=response.json()
    print(data)
    for ticker in data.get('results', []):
        tickers.append(ticker)

example_ticker={'ticker': 'HUM', 
'name': 'Humana Inc.', 
'market': 'stocks', 
'locale': 'us', 
'primary_exchange': 'XNYS', 
'type': 'CS', 
'active': True, 
'currency_name': 'usd', 
'cik': '0000049071', 
'composite_figi': 'BBG000BLKK03', 
'share_class_figi': 'BBG001S5S1X6', 
'last_updated_utc': '2025-09-16T06:05:51.697381223Z'}


# define CSV schema based on example_ticker
FIELDNAMES = list(example_ticker.keys())

with open("tickers.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
    writer.writeheader()

    # write everything you already collected
    for t in tickers:
        writer.writerow({k: t.get(k) for k in FIELDNAMES})
