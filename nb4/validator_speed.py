# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# ## Preface: PyData

import pandas as pd
import numpy as np
dict(pandas=pd.__version__,
     numpy=np.__version__)

# ## MySQL

# +
from slogdata import mysql_socket, show_times

def _slog4db(database='slog4'):
    from sqlalchemy import create_engine
    return create_engine(mysql_socket(database, create_engine))

_db4 = _slog4db()
_db4.execute('show tables').fetchall()
# -

# ## Validator Speeds

pd.read_sql("""
select * from slog_block
limit 3
""", _db4)

pd.read_sql("""
select f.parent, count(distinct blockHeight), count(*)
from slog_block b
join file_info f on b.file_id = f.file_id
where b.blockTime >= 1625166000
group by b.file_id
order by 3
""", _db4).set_index('parent')

# Are we mixing chains? Any block times before `2021-07-01 19:00:00`?

df = pd.read_sql("""
select min(blockTime), max(blockTime) from slog_block
""", _db4)
show_times(df, ['min(blockTime)', 'max(blockTime)'])

pd.read_sql("""
select agg.*, blockHeight_hi - blockHeight_lo + 1 spread
from (
select count(*) records
     , count(distinct blockHeight) blocks
     , min(blockHeight) blockHeight_lo
     , max(blockHeight) blockHeight_hi
from slog_block b
where b.blockTime >= 1625166000
) agg
""", _db4)

df = pd.read_sql("""
select blockHeight, count(distinct file_id) file_id_qty
from slog_block b
where b.blockTime >= 1625166000
group by blockHeight
""", _db4, index_col='blockHeight')
df.head()

df.plot(title='Validator coverage by blockHeight');

_db4.execute("""
create index if not exists slog_block_ix
on slog_block (file_id, run_line_lo, blockHeight)
""")


# +
def block_mark_match(db):
    return pd.read_sql("""
    select lo.file_id, lo.run_line_lo, lo.blockHeight, lo.blockTime
         , lo.time time_lo
         , hi.time time_hi
         , lo.line line_lo
         , hi.line line_hi
    from slog_block lo
    join slog_block hi
      on hi.file_id = lo.file_id
     and hi.run_line_lo = lo.run_line_lo
     and hi.blockHeight = lo.blockHeight
    where lo.sign = -1
    and hi.sign = 1
    and lo.blockTime >= 1625166000
    and hi.blockTime >= 1625166000
    """, db)

_bmm = block_mark_match(_db4)
_bmm
# -

# ### Block processing time: duration, lag
#
#  - **dur** = time from `cosmic-swingset-end-block-start` to `cosmic-swingset-end-block-finish`
#  - **lag** = time from `blockTime` to `cosmic-swingset-end-block-start`
#
# But not all block end start/finish log entries follow immediately after voting:
#   - for the genesis block, block end start happens when the node is started up, which may be well before genesis time
#   - when a validator restarts, **does it emit block end start/finish event while it's replaying???**

df = _bmm[(_bmm.blockHeight > 64628) & (_bmm.blockHeight <= 75000)]
df = df.assign(dur=df.time_hi - df.time_lo,
               lag=df.time_lo - df.blockTime,
               line_count=df.line_hi - df.line_lo + 1)
df = df[(df.line_count > 2) & (df.lag >= 0) & (df.lag < 45)]
df[['dur', 'lag']].describe()

df

df.plot.scatter(x='blockHeight', y='dur', alpha=0.2);

df.plot.scatter(x='blockHeight', y='lag', alpha=0.2);

df[['dur']].hist(log=True);

_file_info = pd.read_sql_table('file_info', _db4, index_col='file_id')


# +
def name_files(df, file_info, drop=True):
    name = file_info['name'].loc[df.file_id]
    parent = file_info['parent'].loc[df.file_id]
    return pd.concat([
        df.set_index('file_id'),
        pd.DataFrame(dict(name=name, parent=parent)),
    ], axis=1).reset_index(drop=drop)


name_files(df, _file_info).groupby(['parent', 'run_line_lo'])[['dur', 'lag']].aggregate(['mean', 'median'])


# +
def order_of(items):
    lex = sorted(items)
    return [list(items).index(item) for item in lex]

med = name_files(df[['file_id', 'dur']], _file_info).groupby('parent')[['dur']].median().sort_values('dur')


ax = name_files(df[['file_id', 'dur']], _file_info).boxplot(
    positions=order_of(med.index),
    by=['parent'], rot=-75, figsize=(10, 5), whis=(5, 95));
ax.set_ylim(0, 6);
# ax[0][0].get_figure().suptitle('')
# ax.set_title('Block end start-to-finish');

# +

med = name_files(df[['file_id', 'lag']], _file_info).groupby('parent')[['lag']].median().sort_values('lag')

order_of(med.index.values)
#med.sort_values('lag') # .index.values # .reset_index() # .set_index('parent', drop=False)
# -

med.lag.sort_values()

# +
med = name_files(df[['file_id', 'lag']], _file_info).groupby('parent')[['lag']].median().sort_values('lag')

ax = name_files(df[['file_id', 'lag']], _file_info).boxplot(
    positions=order_of(med.index),
    by=['parent'], rot=-75, figsize=(10, 5), whis=(5, 95));
ax.set_ylim(4, 28)


# +
def block_end_rate(df, file_info):
    g = name_files(df, file_info).groupby('parent')
    dur = g[['dur']].sum()
    n = g[['blockHeight']].nunique()
    rate= pd.concat([dur, n], axis=1).assign(rate=dur.dur / n.blockHeight)
    return rate

block_end_rate(df, _file_info)[['rate']].sort_values('rate').plot.barh(
    title='End-block processing (sec/block)',
    figsize=(8, 6),
);
#x = name_files(df, _file_info).groupby('name')[['dur']].sum()
#x
# .plot.barh();

# -

block_end_rate(df, _file_info)[['rate']].sort_values('rate')

# Lag

name_files(df, _file_info).groupby('name')[['lag']].sum().plot.barh();
