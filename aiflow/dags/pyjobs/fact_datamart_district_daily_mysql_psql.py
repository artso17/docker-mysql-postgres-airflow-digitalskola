# %%
# Import all dependencies
from sqlalchemy.engine import create_engine 
import pandas as pd 
import os 

# %%
# Create engines
mysql_engine = create_engine(os.environ.get('MYSQL_CONN_STRING'))
ps_engine = create_engine(os.environ.get('PS_CONN_STRING'))

# %%
# Read Data from MySQL
cases = pd.read_sql("""
                SELECT
                    kode_kab AS district_id,
                    tanggal AS date, 
                    closecontact_dikarantina,
                    closecontact_discarded,
                    closecontact_meninggal,
                    confirmation_meninggal,
                    confirmation_sembuh,
                    probable_diisolasi,
                    probable_discarded,
                    probable_meninggal,
                    suspect_diisolasi,
                    suspect_discarded,
                    suspect_meninggal
                FROM mysql.covid_jabar
             """,
             con = mysql_engine
            )

# %%
# Read Data Mart dim_district from Postgres
df_psql_dim_case = pd.read_sql("""
                                    SELECT 
                                            id AS case_id,
                                            status
                                    FROM dwh.public.dim_case
                                """,
                                con=ps_engine
                                )

# %%
# Transform
cases_melt = cases.melt(    id_vars=['district_id','date'],
                            value_vars= [ 'closecontact_dikarantina', 
                                          'closecontact_discarded',
                                          'closecontact_meninggal', 
                                          'confirmation_meninggal',
                                          'confirmation_sembuh',
                                          'probable_diisolasi', 
                                          'probable_discarded',
                                          'probable_meninggal', 
                                          'suspect_diisolasi', 
                                          'suspect_discarded',
                                          'suspect_meninggal'
                                          ],
                            var_name = 'status',
                            value_name = 'total'
                     )

# %%
# Joining Data
district_daily = pd.merge(  cases_melt,df_psql_dim_case,
                            on='status'
                        )
district_daily = district_daily[['district_id','case_id','date','total']]

# %%
# Validate data types
district_daily.district_id = district_daily.district_id.astype(int)
district_daily.case_id = district_daily.case_id.astype(int)
district_daily.date = district_daily.date.astype(str)
district_daily.total = district_daily.total.astype(int)

# %%
# Create table of Data Mart district_daily if not exist
ps_engine.execute(  """
                        CREATE TABLE IF NOT EXISTS dwh.public.district_daily (
                            id SERIAL,
                            district_id VARCHAR,
                            case_id VARCHAR,
                            date VARCHAR,
                            total BIGINT
                        )
                    """
                    )

# %%
# Iterate each row of data and load to Data Mart district_daily
for i in district_daily.to_records():
    ps_engine.execute(  f'''
                            DELETE FROM 
                                dwh.public.district_daily
                            WHERE 
                                district_id	= '{i[1]}'
                                AND case_id	= '{i[2]}'
                                AND date	= '{i[3]}'
                                AND total   = {i[4]};
                            
                            INSERT INTO 
                                dwh.public.district_daily  (district_id,
                                                            case_id,
                                                            date,
                                                            total
                                                            ) 
                            VALUES ({i[1]},
                                    {i[2]},
                                    '{i[3]}',
                                    {i[4]}
                                    )
                        '''
                    )


