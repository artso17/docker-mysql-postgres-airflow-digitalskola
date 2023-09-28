# %%
# Load all required libraries
from sqlalchemy.engine import create_engine
import requests
import os
import pandas as pd 

# %%
# store url in variable url
url = 'http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian?level=kab%22'

# Get data and parse to json then create dataframe
response = requests.get(url)
data= response.json()['data']['content']
df = pd.DataFrame(data)

# %%
# Dataframe information
df.info()

# %%
# Create engine using connection string
mysql_engine = create_engine(os.environ.get('MYSQL_CONN_STRING'))


# %%
# Write dataframe to mysql and replace the table if exists
df.to_sql(name='covid_jabar', con=mysql_engine,index=False,if_exists='replace')
print('<======= Successfully loaded to MySQL =======>')


