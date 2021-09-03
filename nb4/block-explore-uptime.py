# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.11.4
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # Exploring Blocks, Signatures, Uptime
#
# See also:
#  - [Connecting to MongoDB — Anaconda Platform 5\.5\.0 documentation](https://enterprise-docs.anaconda.com/en/latest/data-science-workflows/data/mongodb.html)
#  - [collection – Collection level operations — PyMongo 3\.11\.4 documentation](https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.find)
#
# _**Note:** we use an ssh tunnel to access the big dipper database._

# +
def _connect(url='mongodb://localhost:27017/', name='stargate-agorictest-17'):
    import pymongo
    client = pymongo.MongoClient(url)
    return client.get_database(name)

_exp17 = _connect()
_exp17.list_collection_names()
# -

import pandas as pd
dict(pandas=pd.__version__)


# +
def explorer_blocks(db):
    df = pd.DataFrame.from_records(db.blocks.find())
    df = df.drop(columns=['_id']).set_index('height')
    df.time = pd.to_datetime(df.time)
    return df

blkq = explorer_blocks(_exp17)
blkq.describe()

# +
from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()


# +
# WARNING: client is ambient

def df_to_bq(df, file_path, table_id):
    type_map=dict(O='STRING', i='INT64', M='TIMESTAMP', f='FLOAT64', b='BOOLEAN')
    schema = [
        bigquery.SchemaField(name, type_map[df[name].dtype.kind])
        for name in df.columns
    ]
    job_config = bigquery.LoadJobConfig(
        schema=schema, skip_leading_rows=1, write_disposition='WRITE_TRUNCATE')
    df.to_csv(file_path, index=False)
    with open(file_path, 'rb') as source_file:
        load_job = client.load_table_from_file(
            source_file, table_id, job_config=job_config
        )

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    return destination_table


# -

destination_table = df_to_bq(
    blkq[['hash', 'time', 'proposerAddress', 'precommitsCount', 'validatorsCount']].reset_index(),
    'explorer-export/bk17.csv.gz',
    'slog45.bkexp')
print("Loaded {} rows.".format(destination_table.num_rows))

sigs = pd.DataFrame.from_records(
    dict(height=height, validator=validator)
    for height, b in blkq.iterrows()
    for validator in b.validators
)
sigs.tail()

destination_table = df_to_bq(
    sigs,
    'explorer-export/bksig17.csv.gz',
    'slog45.bksig')
print("Loaded {} rows.".format(destination_table.num_rows))

# !zcat explorer-export/bksig17.csv.gz |head -3

# +
# %%bigquery

select min(height) height, min(time) time
from slog45.bkexp
union all
select max(height) height, max(time) time
from slog45.bkexp


# +
def explorer_validators(db):
    df = pd.DataFrame.from_records(db.validators.find())
    df = df.set_index('address').drop(columns=['_id'])
    df['moniker'] = df.description.apply(lambda d: d.get('moniker'))
    for col in ['unbonding_time', 'jailed_until', 'lastSeen']:
        df[col] = pd.to_datetime(df[col])
    df['delegator_shares'] = df.delegator_shares.astype('float64')
    return df

valq = explorer_validators(_exp17)
print(valq.dtypes)
valq[['delegator_address', 'moniker']].tail(3)
# -

destination_table = df_to_bq(
    valq.reset_index(),
    'explorer-export/val17.csv.gz',
    'slog45.valexp')
print("Loaded {} rows.".format(destination_table.num_rows))

# ## Validators per block

# +
# %%bigquery bknval

select bk.height, bk.time, count(distinct sig.validator) val_qty
from slog45.bksig sig
join slog45.bkexp bk on bk.height = sig.height
-- where bk.time < '2021-08-27' -- Thu, Aug 26 7pm PT
group by bk.height, bk.time
order by bk.height
# -

bknval.set_index('height').tail()

bknval.set_index('time')[['val_qty']].plot(figsize=(15, 5));

bknval.set_index('height')[['val_qty']].plot(figsize=(15, 5));

bknval.loc[20000:22000].set_index('time')[['val_qty']].plot(figsize=(12, 6));

# ## Uptime

# +
# %%bigquery

drop table if exists slog45.uptime;

create table slog45.uptime as

with num as (
    select s.validator, count(distinct s.height) sigs
    from slog45.bksig s
    join slog45.bkexp bk on bk.height = s.height
    where bk.time < '2021-08-27' -- Thu, Aug 26 7pm PT
    group by s.validator
),
denom as (
    select count(bk.height) bk_qty, min(bk.time) time_lo, max(bk.time) time_hi
    from slog45.bkexp bk
    where bk.time < '2021-08-27'
)
select cast(round(sigs / bk_qty * 100, 3) as float64) uptime
     , num.sigs
     , denom.bk_qty
     , denom.time_lo
     , denom.time_hi
     , num.validator
     , gv.moniker
from num
join slog45.genval gv on gv.address = num.validator
cross join denom

# +
# %%bigquery uptime

select * from slog45.uptime
# -

uptime.describe()

df = uptime.sort_values('uptime', ascending=False)
df[df.uptime >= 0.95][['uptime', 'sigs', 'bk_qty', 'time_hi', 'moniker']].set_index('uptime')

uptime.set_index('moniker').sort_values('uptime')[['uptime']].plot(kind='barh', figsize=(8, 20));

# ## Sharing via Google Sheets

# +
import gspread
from gspread_pandas import Spread, Client
from pathlib import Path

from google.oauth2 import service_account

# from https://gspread-pandas.readthedocs.io/en/latest/gspread_pandas.html#module-gspread_pandas.client
SCOPES = ['openid',
          'https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/userinfo.email',
          'https://www.googleapis.com/auth/spreadsheets']

def open_workbook(id, client):
    return Spread(id, client=client)

def _the_workbook(id='1DjKQuBzHLw6slGuXumaYHBjtUEHhWcv1IP_g7-jp7_U',
                  key='../../keys/zinc-union-242321-4d7ae06ee750.json'):
    from pathlib import Path
    credentials = service_account.Credentials.from_service_account_file(key, scopes=SCOPES)
    return open_workbook(id, Client(creds=credentials))

doc45 = _the_workbook()
doc45.sheets
# -

uptime_share = uptime.set_index('uptime').sort_index(ascending=False).reset_index()
uptime_share

doc45.df_to_sheet(uptime_share, sheet='Uptime', index=False, start='A1', replace=True)

# ### btsniik - lots of voting power... how much uptime?

uptime[uptime.moniker.str.startswith('b').fillna(False)]

# +
import numpy as np

def uptime_viz(moniker, shutdown='2021-08-27'):
    validator = uptime[uptime.moniker == moniker].validator.iloc[0]
    v1 = blkq[['time']].copy() # .head()
    v1['ok'] = v1.index.isin(sigs[sigs.validator == validator].height)
    v1['up'] = np.where(v1.ok, 1, 0)
    df = v1[v1.time < shutdown].set_index('time')[['up']]
    return df.plot(style='.', alpha=0.2, figsize=(12, 1), title=f'{moniker} uptime')

uptime_viz('btsniik')

# +
# %%bigquery

select distinct Moniker from slog45.submittedtasks
where discordID = 'mirxl#0530'
limit 3
# -

uptime_viz('mirxl')
