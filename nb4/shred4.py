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

# ## MySql Access

TOP = __name__ == '__main__'

# +
import logging
from sys import stderr

logging.basicConfig(level=logging.INFO, stream=stderr,
                    format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger(__name__)
if TOP:
    log.info('notebook start')


# +
def _slog4db():
    import pymysql
    from sqlalchemy import create_engine
    from sqlalchemy.engine.url import URL
    url = URL.create(
        drivername='mysql+pymysql',
        host=None,
        username=None,
        password=None,
        database='slog4',
        query={
            'unix_socket': '/var/run/mysqld/mysqld.sock',
            'charset': 'utf8mb4',
        }
    )
    return create_engine(url)

_db4 = _slog4db()
_db4.execute('show tables').fetchall()
# -

# ## File Info

# Stop before loading the next file:

_db4.execute('''
alter table file_info_stop rename to file_info
''')

pd.read_sql('select * from file_info_stop order by line_count', _db4)

pd.read_sql('select sum(line_count) / 1000000 from file_info_stop order by st_size', _db4)

# ## Runs

from slogdata import show_times

gen16 = show_times(pd.DataFrame(dict(blockHeight=64628, blockTime=[1625166000], ts=1625166000)), ['blockTime'])
gen16

_run = pd.read_sql("""
select case when blockTime_lo < 1625166000 then 15 else 16 end chain
     , r.*
from slog_run r
where blockHeight_lo is not null
order by file_id, line_lo
""", _db4)
show_times(_run, ['blockTime_lo', 'blockTime_hi', 'time_lo', 'time_hi']
          ).set_index(['chain', 'parent', 'name', 'file_id', 'line_lo']).sort_index()

_run.pivot(columns='chain', values='line_count').hist();

# ## blockTime ranges of runs

df = show_times(_run, ['blockTime_lo', 'blockTime_hi']).sort_values('blockTime_hi')
df = df.set_index(['parent', 'line_lo'])[['blockTime_lo', 'blockTime_hi']]
ax = df.plot(kind='bar', rot=-75, figsize=(10, 5)); #, stacked=True
ax.set_ylim(df.blockTime_lo.min(), df.blockTime_hi.max());

df

# How to convert `"time"` field to python datetime?

import datetime
datetime.datetime.fromtimestamp(1625158475.7698095)


# ## Blocks, consensus block times / durations (agorictest-16)

# +
def blockmark(db, parent):
    df = pd.read_sql("""
        with f1 as (
         select file_id, st_size
         from file_info
         where parent = %(parent)s
        )
        select b.* from slog_block b
        cross join f1 on f1.file_id = b.file_id
        where b.blockTime >= 1625166000
        order by line
    """, db, params=dict(parent=parent))
    df = df.set_index(['file_id', 'run_line_lo', 'blockHeight'])
    return df

_blockmark = df = blockmark(_db4, 'Provalidator')
show_times(df)


# +
def blockdur(df):
    df = df[df.sign == 1].copy()  # only start markers
    df = df.reset_index([0, 1], drop=True)  # never mind file_id, run_line_lo
    df['dur'] = df[['blockTime']].diff().blockTime
    return df

_blkdur = df = blockdur(_blockmark)
df.head()


# -

def block_dur_plot(df, x='blockTime', y='dur', **plotkw):
    return df.plot.scatter(x=x, y=y, alpha=0.2, rot=75, figsize=(12, 4), **plotkw)
block_dur_plot(show_times(_blkdur), title='block-to-block durations in agorictest-16');

block_dur_plot(_blkdur.reset_index(), x='blockHeight');

agorictest_16_sched_end = datetime.datetime(2021, 7, 2, 19)
str(agorictest_16_sched_end)


# +
def cut_after(df, t):
    df = show_times(df)
    df = df[df.blockTime <= t]
    return df

block_dur_plot(cut_after(_blkdur, agorictest_16_sched_end), title='Block durations thru scheduled phase 4 end');
# -

# ## Notes Tue Aug 3
#
#  - long deliveries are anomalies "that' weird"
#  - blocks that take a long time made of normal-sized deliveries: policy can help

88000 - 88500
78000

block_dur_plot(_blkdur[(_blkdur.index >= 78800) & (_blkdur.index <= 78900)].reset_index(), x='blockHeight');

# So about 20 blocks that were slow (>10 sec)

#  & (_blkdur.dur > 10)
show_times(_blkdur[(_blkdur.index >= 78840) & (_blkdur.index <= 78852)].reset_index())

# 78842 longest
#
# 5 normal before
#
# no deliveries in 78842; where are they?

df = pd.read_sql("""
select blockHeight, count(*)
from j_delivery
where blockHeight between 78842 - 2 and 78842 + 10
group by blockHeight
""", _db4)
df

# BW is inclined to switch to shell and jq; what file to look in?

_file_info.loc[47274718195312]

# ## Select block for visualization

df = pd.read_sql('''
select b.blockHeight, bd.deliveries, b.sign, f.parent, f.name, b.line
from (
    select blockHeight, count(*) deliveries
    from j_delivery
    where blockHeight between 78842 - 2 and 78842 + 10
    group by blockHeight
) bd
join slog_block b
on b.blockHeight = bd.blockHeight
join file_info f on f.file_id = b.file_id
where b.file_id = 47274718195312
''', _db4)
df = df.assign(R=df.line.diff() + 1)
df[df.sign == 1].sort_values('deliveries')

# +
x = df[(df.deliveries == 1536) & (df.sign == 1)].iloc[0]
x['out'] = f'bk{x.blockHeight}.gv'
cmd = f'/home/customer/projects/gztool/gztool -v 0 -L {x.line} -R {x.R} slogfiles/{x.parent}/{x["name"]}'
# !$cmd | python causal_slog.py >$x.out

x
# -

# !ls *.gv

# block time vs swingset dur

df = pd.read_sql("""
select *
from slog_block
where blockHeight = 78840
""", _db4)

df = pd.read_sql("""
select *
from j_delivery
where blockHeight = 78840
and file_id = 47274718195312
""", _db4).drop(columns=['file_id', 'run_line_lo', 'line', 'time_lo'])
df

# color by method? vat name
df.plot.scatter(x='crankNum', y='compute', figsize=(12, 5))

# draw graph across multiple validators
show_times(df, ['time_hi']).plot.scatter(x='crankNum', y='time_hi', figsize=(12, 5))

show_times(df[df.crankNum >= 292496], ['time_hi'])

# vat names would be cool; color?
df.plot.scatter(x='crankNum', y='vatID', figsize=(12, 5))

df.groupby('vatID')[['crankNum']].count().plot.pie(y='crankNum')

# Zoe dominates time, but vattp, comms take 1/4th

df.groupby('vatID')[['dur']].sum().plot.pie(y='dur')

#
#  - What user activity was happening?
#    - (7 ext messages coming in, right? deliveryInbound, ack)

# causal:
# vat 1: result promise from send...
# vat 2: deliver result promise kpid
#
# vat 2: resolve same kpid
# vat 1: notify

df['cc'] = df.compute.cumsum()

# 25m cumulative computrons; **8m** ea for 10sec

show_times(df, ['time_hi']).plot.scatter(x='time_hi', y='cc', figsize=(12, 5),
                                        title='Cumulative Computrons over Time')

df[df.vatID == 14]

df.groupby('method')[['line']].count().sort_values('line')

# big pipelines AMM thing... 9x...
# getCurrentAmount, getUpdateSince

# ### Any `slog_entry` records yet?
#
# They're loaded in chunks.

show_times(pd.read_sql('select * from slog_entry limit 10', _db4)).drop(columns=['record'])

# ### Breakdown of entries by type

_db4.execute('drop index slog_entry_ty_ix on slog_entry')

# optimize query by type
_db4.execute('create index slog_entry_ty_ix on slog_entry (type)')

df = pd.read_sql('''
select type, count(*) -- , min(line), min(time), max(line), max(time)
from slog_entry
where type is not null
group by type
''', _db4)
# show_times(df, ['min(time)', 'max(time)'])
df.sort_values('count(*)', ascending=False)

df.sort_values('count(*)', ascending=False)

55224062 / 2876760

# #### Deliver Results

df = pd.read_sql("""
select * from slog_entry
-- where blockHeight is not null
where type = 'deliver-result'
limit 10
""", _db4)
show_times(df)

# Are we getting high resolution time?

df.time[1]

# Is the `time` column datatype right?

pd.read_sql("""
describe slog_entry
""", _db4)

# Can we do JSON functions on the server side?

df = pd.read_sql("""
select json_extract(record, '$.dr[2].compute') compute
     , json_extract(record, '$.kd') kd
     , json_extract(record, '$.dr') dr
     , json_keys(record) rk
from slog_entry
where type in ('deliver', 'deliver-result')
limit 100
""", _db4)
show_times(df)

# ### Deliveries / Cranks

agorictest_16_sched_end.timestamp()

# avoid
# OperationalError: (pymysql.err.OperationalError) (1206, 'The total number of locks exceeds the lock table size')
_db4.execute('SET GLOBAL innodb_buffer_pool_size=268435456;');


# +
def build_delivery(db,
                   table='t_delivery',
                   blockTime_hi=agorictest_16_sched_end):
    log.info('building %s', table)
    db.execute(f"""
    create table {table} as
    select file_id, run_line_lo
         , line
         , blockHeight
         , blockTime
         , time
         , crankNum
         , cast(substr(json_unquote(json_extract(record, '$.vatID')), 2) as int) vatID
         , coalesce(cast(json_extract(record, '$.deliveryNum') as int), -1) deliveryNum
         , json_unquote(json_extract(record, '$.kd[2].method')) method
         , crc32(json_extract(record, '$.kd')) kd32
    from slog_entry
    where type = 'deliver'
    and blockTime <= {blockTime_hi.timestamp()}
    """)
    log.info('indexing %s', table)
    db.execute(f"""
    create index {table}_ix on {table} (file_id, run_line_lo, vatID, deliveryNum)
    """)
    return pd.read_sql(f'select * from {table} limit 10', db)


def build_delivery_result(db,
                          table='t_delivery_result',
                          blockTime_hi=agorictest_16_sched_end):
    log.info('building %s', table)
    db.execute(f"""
    create table {table} as
    select file_id, run_line_lo
         , line
         , blockHeight
         , blockTime
         , time
         , crankNum
         , cast(substr(json_unquote(json_extract(record, '$.vatID')), 2) as int) vatID
         , coalesce(cast(json_extract(record, '$.deliveryNum') as int), -1) deliveryNum
           -- json INTEGER comes out as a python/pandas str, so cast to int
           -- then, to avoid floating point, coalese null to -1
         , coalesce(cast(json_extract(record, '$.dr[2].compute') as int), 0) compute
        from slog_entry
        where type = 'deliver-result'
        and blockTime <= {blockTime_hi.timestamp()}
    """)
    log.info('indexing %s', table)
    db.execute(f"""
    create index {table}_ix on {table} (file_id, run_line_lo, vatID, deliveryNum)
    """)
    return pd.read_sql(f'select * from {table} limit 10', db)


# -

log.info('drop...')
_db4.execute('drop table t_delivery')
df = build_delivery(_db4)
log.info('done')
df

pd.read_sql("""
describe t_delivery
""", _db4)

show_times(pd.read_sql("""
select file_id, run_line_lo, count(*)
from t_delivery
group by file_id, run_line_lo
order by 3 desc
""", _db4))

# +
log.info('drop...')
_db4.execute('drop table t_delivery_result')

df = build_delivery_result(_db4)
log.info('done')
df


# +
def join_delivery(db,
                  table='j_delivery',
                  t_lo='t_delivery', t_hi='t_delivery_result',):
    log.info('building %s', table)
    db.execute(f"""
    create table {table} as
    select lo.file_id, lo.run_line_lo
         , lo.vatID
         , lo.deliveryNum
         , lo.crankNum
         , lo.line
         , lo.blockHeight
         , lo.blockTime
         , lo.time time_lo
         , hi.time time_hi
         , hi.time - lo.time dur
         , lo.method
         , lo.kd32
         , hi.compute
    from {t_lo} lo
    join {t_hi} hi
      on hi.file_id = lo.file_id
     and hi.run_line_lo = lo.run_line_lo
     and hi.vatID = lo.vatID
     and hi.deliveryNum = lo.deliveryNum
    """)
    log.info('indexing %s', table)
    db.execute(f"""
    create index {table}_ix on {table} (vatID, deliveryNum)
    """)
    return pd.read_sql(f'select * from {table} limit 10', db)

log.info('start')
df = join_delivery(_db4)
log.info('done')
df
# -

# ## Vat names: what is the vatID for vattp?

pd.read_sql('''
select distinct vatID, name
from (
select
       cast(substr(json_unquote(json_extract(record, '$.vatID')), 2) as int) vatID
     , json_unquote(json_extract(record, '$.name')) name
     -- , json_unquote(json_extract(record, '$.description')) description
from slog_entry
where type = 'create-vat'
) t
where name is not null
order by vatID
''', _db4, index_col='vatID')

df = pd.read_sql('''
select vatID, deliveryNum, count(distinct compute)
from j_delivery
where vatID != 14 -- vattp has a known problem
group by vatID, deliveryNum
having count(distinct compute) > 1
''', _db4)
df

df = pd.read_sql('''
select vatID, deliveryNum, count(distinct kd32)
from j_delivery
where vatID != 14 -- vattp has a known problem
group by vatID, deliveryNum
having count(distinct kd32) > 1
''', _db4)
df

df.set_index(['vatID', 'deliveryNum']).sort_index()

# syscalls per crank

df = pd.read_sql("""
select file_id, run_line_lo, blockHeight, blockTime, crankNum, count(*) syscalls
from slog_entry
where type in ('syscall-result')
group by file_id, run_line_lo, blockHeight, blockTime, crankNum
""", _db4)
show_times(df)

show_times(df.set_index(['file_id', 'run_line_lo', 'crankNum']))

df = pd.read_sql('''
select count(distinct vatID, deliveryNum)
from j_delivery
-- where vatID != 14 -- vattp has a known problem
-- group by vatID, deliveryNum
''', _db4)
df
