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

# ## Genesis, Uptime Tasks

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

def _the_workbook(id='1DjKQuBzHLw6slGuXumaYHBjtUEHhWcv1IP_g7-jp7_U',
                  key='../../keys/zinc-union-242321-4d7ae06ee750.json'):
    from pathlib import Path
    credentials = service_account.Credentials.from_service_account_file(key, scopes=SCOPES)
    return Spread(id, client=Client(creds=credentials))

doc45 = _the_workbook()
doc45.sheets
# -

import pandas as pd
dict(pandas=pd.__version__)

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

# +
# WARNING: client is ambient

from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

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
    tasksubt,
    'explorer-export/tmp.csv.gz',
    'slog45.submittedtasks')
print("Loaded {} rows.".format(destination_table.num_rows))

# +
# %%bigquery

select count(*) task_qty, count(distinct discordID) discord_qty, max(updated) updated_hi
from slog45.submittedtasks
where updated >= '2021-08-15'

# +
# %%bigquery task_start_genesis

select t.TaskBoardID, t.discordID, t.Status, t.Verified, gv.moniker, gv.address, gv.delegator_address, sig.height_lo, bk.time
from slog45.submittedtasks t
left join slog45.genval gv on gv.discordID = t.discordID
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

task_start_genesis.sort_values('time').reset_index()[['time']].plot();

# +
df = task_start_genesis[['TaskBoardID', 'moniker', 'height_lo', 'time']].sort_values('height_lo').reset_index(drop=True)
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
task_start_genesis.loc[task_start_genesis.height_lo > 10, 'Verified'] = 'Not accepted'
task_start_genesis.groupby('Verified')[['TaskBoardID']].count()

doc45.df_to_sheet(task_start_genesis, sheet='Start Genesis', index=False, start='A1', replace=True)

# ### Uptime tasks

# +
# %%bigquery uptime

select * from slog45.uptime
# -

uptime.head()

# +
# %%bigquery

select discordID, count(*) from slog45.genval where Verified = 'Accepted' and discordID is not null group by discordID having count(*) > 1
-- select * from slog45.genval where Verified = 'Accepted'

# +
# %%bigquery

select address, count(*) from slog45.genval where Verified = 'Accepted' and discordID is not null group by address having count(*) > 1

# +
# %%bigquery task_uptime

with t as (
    select t.*
    from slog45.submittedtasks t
    where t.updated >= '2021-08-15' and t.Task = 'Maintain uptime during phase!'
)

-- why is this distinct needed?
select distinct uptime.uptime, t.TaskBoardID, t.Status, t.Verified, t.discordID
     , t.Moniker, uptime.moniker moniker_gtx, uptime.sigs, uptime.bk_qty, uptime.validator
     , gv.delegator_address
     , t.submission
from t
left join slog45.genval gv on gv.discordID = t.discordID and gv.Verified = 'Accepted'
left join slog45.uptime on uptime.validator = gv.address
# -

task_uptime.TaskBoardID.nunique(), len(task_uptime)

task_uptime[~task_uptime.TaskBoardID.isnull() & task_uptime.uptime.isnull()]

task_uptime.loc[task_uptime.uptime >= 50, 'Verified'] = 'Accepted'
task_uptime.loc[task_uptime.uptime < 50, 'Verified'] = 'Not accepted'
task_uptime.groupby('Verified')[['TaskBoardID']].count()

doc45.df_to_sheet(task_uptime.sort_values('uptime', ascending=False),
                  sheet='Uptime', index=False, start='A1', replace=True)

# +
# %%bigquery genval

select * from slog45.genval
# -

genval[genval.Moniker == 'Va1id8']

uptime[uptime.moniker == 'Va1id8']
