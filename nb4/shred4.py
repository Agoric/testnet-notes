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
dict(pandas=pd.__version__)

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

pd.read_sql('select * from file_info order by line_count', _db4)

pd.read_sql('select sum(line_count) / 1000000 from file_info order by st_size', _db4)

# ## Runs

from slogdata import show_times

_run = pd.read_sql("""
select * from slog_run
where blockHeight_lo is not null
order by file_id, line_lo
""", _db4)
show_times(_run, ['blockTime_lo', 'blockTime_hi', 'time_lo', 'time_hi'])

_run[['line_count']].hist();

# ## blockTime ranges of runs

df = show_times(_run, ['blockTime_lo', 'blockTime_hi']).sort_values('blockTime_hi')
df = df.set_index(['parent', 'line_lo'])[['blockTime_lo', 'blockTime_hi']]
ax = df.plot(kind='bar', rot=-75, figsize=(10, 5)); #, stacked=True
ax.set_ylim(df.blockTime_lo.min(), df.blockTime_hi.max());

df

# How to convert `"time"` field to python datetime?

import datetime
datetime.datetime.fromtimestamp(1625158475.7698095)


# ## Blocks, consensus block times / durations

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

# ## Validator Speeds

pd.read_sql("""
select * from slog_block
limit 3
""", _db4)

pd.read_sql("""
select f.parent, count(distinct blockHeight), count(*)
from slog_block b
join file_info f on b.file_id = f.file_id
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
from slog_block
) agg
""", _db4)

df = pd.read_sql("""
select blockHeight, count(distinct file_id) file_id_qty
from slog_block
group by blockHeight
""", _db4, index_col='blockHeight')
df.head()

df.plot(title='Validator coverage by blockHeight');

_db4.execute("""
create index slog_block_ix
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

[med.index[i] for i in order_of(med.index)]

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

# ### Any `slog_entry` records yet?
#
# They're loaded in chunks.

show_times(pd.read_sql('select * from slog_entry limit 10', _db4)).drop(columns=['record'])

# ### Breakdown of entries by type (slow!)

# optimize query by type
_db4.execute('create index slog_entry_ty_ix on slog_entry (type)')

df = pd.read_sql('''
select type, count(*) -- , min(line), min(time), max(line), max(time)
from slog_entry
where type is not null
group by type
''', _db4)
# show_times(df, ['min(time)', 'max(time)'])
df

df.sort_values('count(*)', ascending=False)

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
_db4.execute('SET GLOBAL innodb_buffer_pool_size=268435456;')

_db4.execute('drop table t_delivery')


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

build_delivery(_db4)

pd.read_sql("""
describe t_delivery
""", _db4)

show_times(pd.read_sql("""
select file_id, run_line_lo, count(*)
from t_delivery
group by file_id, run_line_lo
order by 3 desc
""", _db4))

log.info('start')
build_delivery_result(_db4)
log.info('done')


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

pd.read_sql('''
select distinct *
from (
select json_unquote(json_extract(record, '$.vatID')) vatID
     , json_unquote(json_extract(record, '$.name')) name
     , json_unquote(json_extract(record, '$.description')) description
from slog_entry
where type = 'create-vat'
) t
''', _db4)

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
