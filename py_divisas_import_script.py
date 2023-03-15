# %%
from api_key import key
from sql_credentials import user_name, password
import pandas as pd
import requests
from sqlalchemy import create_engine
import time

# %%
# import forex from api

divisas = [
    'CLP',
	'USD',
	'MXN',
	'COP',
	'UYU',
	'PEN',
	'ARS',
]

df = pd.DataFrame()

for i, item in enumerate(divisas):
		
	url = 'https://www.alphavantage.co/query?function=FX_DAILY&from_symbol=EUR&to_symbol=' + item + '&apikey=' + key

	try:
		r = requests.get(url)
		data = r.json()

		dfi = pd.DataFrame.from_dict(data['Time Series FX (Daily)'], orient='index')

		dfi['information'] = data['Meta Data']['1. Information']
		dfi['from_symbol'] = data['Meta Data']['2. From Symbol']
		dfi['to_symbol'] = data['Meta Data']['3. To Symbol']
		dfi['output_size'] = data['Meta Data']['4. Output Size']
		dfi['last_refreshed'] = data['Meta Data']['5. Last Refreshed']
		dfi['time_zone'] = data['Meta Data']['6. Time Zone']

		dfi = dfi.reset_index()

		dfi = dfi.rename(columns={'index': 'date', '1. open': 'open', '2. high': 'high', '3. low': 'low', '4. close': 'close'})

		df = pd.concat([df, dfi], axis=0, ignore_index=True)

	except:

		print('Error: ' + item)
		print('URL: ' + url)
	
	# add a waiting period of one minute every five requests to avoid exceeding the API limit
	if (i+1) % 5 == 0:
		time.sleep(60)

print('Import Done, shape: ' + str(df.shape))

# %%
df.to_parquet('forex.parquet', engine='fastparquet')

# %%
# Create a sql server database connection
driver = 'ODBC Driver 17 for SQL Server'
server_name = '10.233.49.6'
database_name = 'HISPAM_OnHR'
connection_string = f'mssql+pyodbc://{user_name}:{password}@{server_name}/{database_name}?driver={driver}'

engine = create_engine(connection_string)

# test the connection
try:
	engine.connect()
	print("Connection Successful")
	engine.dispose()
except:
	print("Connection Unsuccessful")

# %%
# export to sql server
df.to_sql('T_Divisas', con=engine, if_exists='replace', index=False)
print('Export Done')