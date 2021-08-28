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

# ### Preface: PyData tools

import pandas as pd
dict(pandas=pd.__version__)

# ## Task Portal Export
#
# Participants use the https://validateagoric.knack.com/ portal to submit their tasks for review.

# !ls -l portal-export/submittedtasks.csv

tasksub = pd.read_csv('portal-export/submittedtasks.csv',
                      parse_dates=['Last Date Updated'])
tasksub.tail(3)

phase45start = '2021-08-15'
tasksub[tasksub['Last Date Updated'] >= phase45start].groupby(['Event', 'Task'])[['TaskBoardID']].count()

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
sf5.drop(columns=['path']).sort_values('discordID')
# -

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
# -

sfold.path[0]

gsutil.run('rm', sfold.path[0])

gsutil.run('mv', *sfold.path[1:], 'gs://slogfile-upload-5/dup-test/');

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

x = sf5match.Verified
sf5match.drop(columns=['Verified']).insert(2, 'Verified', x).head()
# -

sf5match.groupby('Verified')[['Discord ID']].aggregate('nunique')

sf5match[sf5match.Verified != 'Accepted']

# !mkdir -p portal-review

sf5match.to_csv('portal-review/capture_submit_slog.csv')
