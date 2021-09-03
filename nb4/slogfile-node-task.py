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

# # Associating Slogfiles with Nodes, Tasks
#
#  - [ ] [Mismatch: Slogfile Tasks](#Mismatch:-Slogfile-Tasks)
#  - [ ] [Mismatch: Files](#Mismatch:-Files)
#  - [ ] [Mismatch: monikers](#Mismatch:-monikers)
#  - [ ] [Mismatch: gentx tasks](#Mismatch:-gentx-tasks)
#  - [ ] [Mismatch: Validators](#Mismatch:-Validators)
#

# ### Preface: PyData tools

import pandas as pd
dict(pandas=pd.__version__)

# +
# # !pip install gspread gspread-pandas
# -

# ### Script vs module

TOP = __name__ == '__main__'

# ## Task Portal Export
#
# Participants use the https://validateagoric.knack.com/ portal to submit their tasks for review.

# +
# # !ls -l portal-export/submittedtasks.csv
# -

tasksub = pd.read_csv('portal-export/submittedtasks.csv',
                      parse_dates=['Last Date Updated'])
phase45start = '2021-08-15'
tasksub45 = tasksub[tasksub['Last Date Updated'] >= phase45start].copy()
tasksub45.Event = tasksub45.Event.fillna('Metering ')
tasksub45.set_index('TaskBoardID')[['Event', 'Task', 'Last Date Updated']].sort_values('Last Date Updated').tail(3)

task_summary = tasksub45.groupby('Event')[['Last Date Updated']].agg(['count', 'min', 'max'])
task_summary.insert(0, 'Task', [None])
#pd.DataFrame.from_records([dict(
#    count=len(tasksub45),
#    min=tasksub45['Last Date Updated'].min(),
#    max=tasksub45['Last Date Updated'].max())])
task_summary

# +
task_breakdown = tasksub45.groupby(['Event', 'Task'])[['Last Date Updated']].agg(['count', 'min', 'max'])

task_breakdown = task_breakdown.sort_values(('Last Date Updated',   'min'))
task_breakdown

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

doc45.df_to_sheet(task_summary.reset_index(), index=False, sheet='Task Summary', start='A1', replace=True)
doc45.df_to_sheet(task_breakdown.reset_index(), index=False, sheet='Task Summary', start='A5')

# ## Slogfile tasks submitted

taskslog = tasksub[tasksub['Task'] == 'Capture and submit a slog file'].drop(columns=['Event', 'Task', 'Points', 'Task Type']).set_index('TaskBoardID')
print(len(taskslog))
taskslog.tail()


# ## Slogfiles Collected

# The https://submit.agoric.app tool authenticated participants using Discord OAuth
# and stored their submissions in the `slogfile-upload-5` bucket in Google Cloud Storage.

# up5 = !gsutil ls -l gs://slogfile-upload-5/

# +
def pathparts(row):
    filename = row.path.split('/')[-1]
    noext = filename.replace('.slog.gz', '')
    fresh, discordID = noext.split('Z-', 1)
    return dict(filename=filename, fresh=fresh, discordID=discordID)

sf5 = pd.DataFrame.from_records(
    [dict(size=int(size), modified=modified, path=path) for line in up5 if 'Z' in line
     for (size, modified, path) in [line.strip().split('  ', 2)]])
sf5 = pd.concat([sf5, sf5[['path']].apply(pathparts, axis=1, result_type='expand')], axis=1)
sf5 = sf5.set_index('filename').sort_index()
sf5 = sf5.assign(modified=pd.to_datetime(sf5.modified),
                 fresh=pd.to_datetime(sf5.fresh))
print(sf5.dtypes)
sf5.drop(columns=['path']).sort_values('size')
# -

sf5.dtypes

sf5.fresh.duplicated().any()

# ### Duplicate, empty uploads
#
# These were moved to `dup-test`.

sf5[sf5.duplicated(['discordID', 'size'], keep=False)]

sfold = sf5[sf5.duplicated(['discordID', 'size'], keep='last')]
sfold.path

# +
from slogdata import CLI

def _cli(bin):
    from subprocess import run, Popen
    return CLI(bin, run, Popen)

gsutil = _cli('gsutil')

# +
# gsutil.run('mv', *sfold.path, 'gs://slogfile-upload-5/dup-test/');
# -

# ## Connecting Slogfiles with Task Submissions

# +
sf5match = pd.merge(taskslog.reset_index(), sf5.reset_index(),
         left_on='Discord ID', right_on='discordID', how='left'
                   ).set_index('TaskBoardID').drop(columns=['Developer', 'Status', 'path', 'fresh', 'discordID', 'modified'])

sf5match.loc[~sf5match.filename.isnull(), 'Verified'] = 'Accepted'

sf5match = sf5match.sort_values(['Verified', 'Discord ID', 'filename'])

def move_col(df, col, pos):
    x = df[col]
    df = df.drop(columns=[col])
    df.insert(pos, col, x)
    return df

sf5match = move_col(sf5match, 'Verified', 1)
sf5match.head()
# -

sf5match.groupby('Verified')[['Discord ID']].aggregate('nunique')

doc45.df_to_sheet(sf5match.reset_index(), index=False, sheet='Slogfile Tasks and Files', start='A1', replace=True)

# ### Mismatch: Slogfile Tasks
#
# with no matching file

sf5match[sf5match.Verified != 'Accepted'].reset_index()

# !mkdir -p portal-review

sf5match.to_csv('portal-review/capture_submit_slog.csv')

# ### Mismatch: Files
#
# with no matching task

sf5_notask = sf5[~sf5.discordID.isin(taskslog['Discord ID'])][['discordID']] #.reset_index(drop=True)
sf5_notask

doc45.df_to_sheet(sf5_notask.reset_index(), index=False, sheet='Slogfiles unmatched', start='A1', replace=True)


# ## Validators

# ## Blocks, Votes from the BigDipper Explorer DB

# +
# # !conda install --yes -c anaconda pymongo
# pymongo-3.11.0

# # !pip install pymongo

# Successfully installed pymongo-3.12.0
# -

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

# The bigdipper refers to validators by address but we use moniker in the portal, so let's get the correspondence:

# +
validator = pd.DataFrame.from_records(_db.validators.find())

validator['moniker'] = validator.description.apply(lambda d: d['moniker'])
validator = validator.set_index('address', drop=True)
validator[['operator_address', 'delegator_address']].head()
# -

# ### Info from review of submitted gentx files
#
# See `gentx_clean.py` (_note: revision to include extracted moniker in the summary to appear_)

# !python gentx_clean.py portal-export/submittedtasks.csv gentx_files >portal-review/gentx17.csv

# !ls -l portal-review/gentx17.csv

gentx17 = pd.read_csv('portal-review/gentx17.csv', index_col=0, parse_dates=['Last Date Updated'])
# print(gentx17.dtypes)
gentx17.tail()

# ### Mismatch: monikers
#
# Monker mismatches between knack portal records and submitted gentxs

gentx17ok = gentx17[(gentx17.Status == 'Completed') & (~gentx17.jsonErr)]
gentx17ok[gentx17ok.Moniker != gentx17ok.moniker]

doc45.df_to_sheet(gentx17ok[gentx17ok.Moniker != gentx17ok.moniker], index=False,
                  sheet='Moniker mismatches', start='A1', replace=True)

# ## `gentx` Tasks

taskgen = tasksub[tasksub.Task == 'Create and submit gentx - Metering '][['TaskBoardID', 'Discord ID', 'Verified', 'Status', 'Last Date Updated', 'Submission Link']]
taskgen.tail()

# +
# pd.merge(taskgen, gentx17, on='Discord ID', how='outer')
# -

# ## Matching `gentx`s Validators
#
# matching `gentx` task submissions with validators from the Explorer

genval = pd.merge(gentx17, validator.reset_index(), on='delegator_address', how='outer', suffixes=['_knack', ''])
# print(x.dtypes)
genval[~genval.status.isnull() & ~genval.TaskBoardID.isnull()][[
    'TaskBoardID', 'Discord ID', 'jsonErr', 'moniker', 'address', 'delegator_address', 'status', 'tokens']]

# +
import numpy as np

genvalc = genval[['TaskBoardID', 'Discord ID', 'Status', 'jsonErr', 'Moniker', 'address', 'delegator_address', 'status', 'tokens']].copy()
genvalc.insert(3, 'Verified', np.where(genvalc.Status == 'Obsolete', 'Not accepted',
                                       np.where(genvalc.address.isnull(), 'in review', 'Accepted')))

# genvalc.groupby('Verified')[['TaskBoardID']].count()
genvalc = genvalc.set_index('TaskBoardID')
genvalc['Submission Link'] = tasksub[['TaskBoardID', 'Submission Link']].set_index('TaskBoardID')
genvalc
# -

genvalc.reset_index().groupby('Verified')[['TaskBoardID']].count()

doc45.df_to_sheet(genvalc.reset_index(), sheet='gentx Tasks and Validators', start='A1', replace=True)

# +
from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()


# -

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


# %%bigquery
drop table if exists slog45.genval

df_to_bq(genvalc.rename(columns={'Discord ID': 'discordID', 'status': 'status_exp', 'Submission Link': 'submission'}), 'portal-review/genval.csv.gz', 'slog45.genval')

# ### Mismatch: gentx tasks
#
# Submissions with no matching validator from the explorer:

genval[genval.status.isnull()][['TaskBoardID', 'Discord ID', 'Moniker', 'jsonErr', 'moniker', 'address', 'delegator_address', 'status', 'tokens']].reset_index(drop=True)

# ### Mismatch: Validators
#
# with no matching Task Submission

genval[genval.TaskBoardID.isnull()][['TaskBoardID', 'moniker', 'address', 'delegator_address', 'status', 'tokens']]

# ## A Canonical Slogfile

sf0 = sf5[sf5['size'] == sf5['size'].max()].rename(columns=dict(discordID='Discord ID'))
sf0.drop(columns=['path'])

pd.merge(sf0.reset_index(), genval[['Discord ID', 'moniker', 'delegator_address']]).drop(columns=['path'])

# ## Loadgen Task

task_loadgen = tasksub45[tasksub45.Task == "Launch and maintain Agoric's load generator node through the entire phase"]
task_loadgen.head(2)

doc45.df_to_sheet(task_loadgen, index=False, sheet='loadgen Task', start='A1', replace=True)
