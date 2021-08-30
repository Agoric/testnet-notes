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

_db = _connect()
# -

import pandas as pd
dict(pandas=pd.__version__)

blkq = pd.DataFrame.from_records(_db.blocks.find()).set_index('height')
blkq.tail()

blkq.time = pd.to_datetime(blkq.time)
blkq.tail(3)

sigs = pd.DataFrame.from_records(
    dict(height=height, validator=validator)
    for height, b in blkq.iterrows()
    for validator in b.validators
)

sigs.tail()

blkq[['hash', 'time', 'proposerAddress', 'precommitsCount', 'validatorsCount']].to_csv('explorer-export/bk17.csv.gz', index=False)

sigs.to_csv('explorer-export/bksig17.csv.gz', index=False)

# !zcat explorer-export/bksig17.csv.gz |head -3

# +
from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# +
file_path = 'explorer-export/bksig17.csv.gz'
table_id = "slog45.bksig"

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("height", "INT64"),
        bigquery.SchemaField("validator", "STRING"),
    ],
    skip_leading_rows=1,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)

with open(file_path, 'rb') as source_file:
    load_job = client.load_table_from_file(
        source_file, table_id, job_config=job_config
    )  # Make an API request.

load_job.result()  # Waits for the job to complete.

destination_table = client.get_table(table_id)  # Make an API request.
print("Loaded {} rows.".format(destination_table.num_rows))
# -

# %%bigquery
drop table slog45.bkexp


def df_to_bq(df, file_path, table_id):
    type_map=dict(O='STRING', i='INT64', M='TIMESTAMP', f='FLOAT64', b='BOOLEAN')
    schema = [
        bigquery.SchemaField(name, type_map[df[name].dtype.kind])
        for name in df.columns
    ]
    job_config = bigquery.LoadJobConfig(schema=schema, skip_leading_rows=1)
    df.to_csv(file_path, index=False)
    with open(file_path, 'rb') as source_file:
        load_job = client.load_table_from_file(
            source_file, table_id, job_config=job_config
        )

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    return destination_table


destination_table = df_to_bq(
    blkq[['hash', 'time', 'proposerAddress', 'precommitsCount', 'validatorsCount']].reset_index(),
    'explorer-export/bk17.csv.gz',
    'slog45.bkexp')
print("Loaded {} rows.".format(destination_table.num_rows))

# +
# %%bigquery

select min(height) height, min(time) time
from slog45.bkexp
union all
select max(height) height, max(time) time
from slog45.bkexp
# -

valq = pd.DataFrame.from_records(_db.validators.find()).set_index('address').drop(columns=['_id'])
valq['moniker'] = valq.description.apply(lambda d: d.get('moniker'))
for col in ['unbonding_time', 'jailed_until', 'lastSeen']:
    valq[col] = pd.to_datetime(valq[col])
valq['delegator_shares'] = valq.delegator_shares.astype('float64')
print(valq.dtypes)
valq.tail(3)

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
where bk.time < '2021-08-27' -- Thu, Aug 26 7pm PT
group by bk.height, bk.time
order by bk.height
# -

bknval.set_index('height').tail()

bknval.set_index('time')[['val_qty']].plot(figsize=(15, 5));

bknval.set_index('height')[['val_qty']].plot(figsize=(15, 5));

bknval.loc[20000:22000].set_index('time')[['val_qty']].plot(figsize=(12, 6));

# ## Uptime

# +
# %%bigquery uptime

select * from slog45.uptime
# -

df = uptime.set_index('moniker').sort_values('uptime', ascending=False)
df[df.uptime >= 95].reset_index().drop(columns=['validator', 'time_lo'])[['uptime', 'sigs', 'bk_qty', 'time_hi', 'moniker']].set_index('uptime')

uptime.set_index('moniker').sort_values('uptime')[['uptime']].plot(kind='barh', figsize=(8, 20));

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

uptime[uptime.moniker.str.startswith('b')]

sigs[sigs.validator == '93C1FD057D299A4128292031B8D6B43105155778'].height #.plot.scatter(x='height', y='height', alpha=0.2, figsize=(12, 12)) #[['height']].plot()

v1 = blkq[['time']].copy() # .head()
v1['ok'] = v1.index.isin(sigs[sigs.validator == '93C1FD057D299A4128292031B8D6B43105155778'].height)

v1.head()

import numpy as np
v1['up'] = np.where(v1.ok, 1, 0)

v1[v1.time < '2021-08-27'].set_index('time')[['up']].plot(style='.', alpha=0.2, figsize=(12, 4));

# ## Genesis, Uptime Tasks

tasksub = pd.read_csv('portal-export/submittedtasks.csv',
                      parse_dates=['Last Date Updated'])
tasksub.dtypes

# avoid
# BadRequest: 400 POST https://bigquery.googleapis.com/upload/bigquery/v2/projects/zinc-union-242321/jobs?uploadType=resumable:
# Invalid field name "Discord ID". Fields must contain only letters, numbers, and underscores, start with a letter or underscore, and be at most 300 characters long.
tasksubt = tasksub.rename(columns={
    'Discord ID': 'discordID',
    'Submission Link': 'submission',
    'Task Type': 'taskType',
    'Last Date Updated': 'updated',
})
tasksubt.dtypes

destination_table = df_to_bq(
    tasksubt,
    'explorer-export/tmp.csv.gz',
    'slog45.submittedtasks')
print("Loaded {} rows.".format(destination_table.num_rows))

# +
# %%bigquery

select count(*) count, max(updated) updated_hi
from slog45.submittedtasks
where updated >= '2021-08-15'

# +
# %%bigquery

select count(*) count
from slog45.submittedtasks t
where t.updated >= '2021-08-15'
and t.Task = 'Start your validator as part of the genesis ceremony '

# +
# %%bigquery task_start_genesis

select t.TaskBoardID, t.discordID, t.Status, t.Verified, gv.moniker, gv.address, gv.delegator_address, sig.height_lo, bk.time
from slog45.submittedtasks t
full outer join slog45.genval gv on gv.discordID = t.discordID
left join (
    select validator, min(height) height_lo
    from slog45.bksig sig
    group by validator
) sig on sig.validator = gv.address
left join slog45.bkexp bk on bk.height = sig.height_lo
where t.updated >= '2021-08-15'
and t.Task = 'Start your validator as part of the genesis ceremony '
# -

task_start_genesis.sort_values('height_lo').reset_index()[['height_lo']].plot();

task_start_genesis.sort_values('time').reset_index()[['time']].plot()

byt = task_start_genesis.groupby('height_lo')[['time']].min()
byt.head(5)

# +
df = task_start_genesis[['TaskBoardID', 'moniker', 'height_lo', 'time']].sort_values('height_lo').reset_index(drop=True)
df['unit'] = 1
agg = df.groupby('height_lo')

df = pd.concat([
    agg[['TaskBoardID']].count(),
    agg[['time']].min()
], axis=1)
df['cumsum'] = df.TaskBoardID.cumsum()
df.head(15)
# -

df[['cumsum']].iloc[:15].plot();

df.set_index('time')[['cumsum']].iloc[:15].plot();

task_start_genesis.loc[task_start_genesis.height_lo <= 10, 'Verified'] = 'Approved'
task_start_genesis.groupby('Verified')[['TaskBoardID']].count()

doc45.df_to_sheet(task_start_genesis, sheet='Start Genesis', index=False, start='A1', replace=True)
