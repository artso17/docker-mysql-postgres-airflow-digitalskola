# %%
# Import all dependencies
from sqlalchemy.engine import create_engine 
import pandas as pd 
import os 

# %%
# Create engines
mysql_engine = create_engine(os.environ.get('MYSQL_CONN_STRING','mysql+mysqlconnector://root:mysql@127.0.0.1:3307/mysql'))
ps_engine = create_engine(os.environ.get('PS_CONN_STRING','postgresql://postgres:postgres@127.0.0.1:5435/dwh'))


# %%
# Read Data from MySQL
df = pd.read_sql("""
                SELECT 
                    *
                FROM mysql.covid_jabar
                """,
                con=mysql_engine
                )


# %%
# Create Data Mart dim_province and load to Postgres
df_dim_province = df[['kode_prov','nama_prov']].copy()
df_dim_province.columns = ['province_id','province_name']
df_dim_province.drop_duplicates(inplace=True)
df_dim_province.to_sql(name='dim_province',con=ps_engine,index=False,if_exists='replace')

# %%
# Create Data Mart dim_district and load to Postgres
df_dim_district = df[['kode_kab','kode_prov','nama_kab']].copy()
df_dim_district.columns = ['district_id','province_id','district_name']
df_dim_district.drop_duplicates(inplace=True)
df_dim_district.to_sql(name='dim_district',con=ps_engine,index=False,if_exists='replace')

# %%
# Create Data Mart dim_case and load to Postgres
df_dim_case = df[['kode_kab','kode_prov','nama_kab']].copy()
df_dim_case.columns = ['district_id','province_id','district_name']
df_dim_case.drop_duplicates(inplace=True)
df_dim_case.to_sql(name='dim_district',con=ps_engine,index=False,if_exists='replace')

# %%
# Create and Transform Data Mart dim_case and load to Postgres
status = df.columns.copy()
status = pd.DataFrame(status[status.str.contains('^(?!ko|na).*_')],columns=['status'])
status[['status_name','status_detail']]=status.status.str.split('_',expand=True)
status.index +=1
status.to_sql('dim_case',con=ps_engine,index_label='id',if_exists='replace')
print('<===== Successfully Created Data Marts and Loaded to Postgres=====>')


