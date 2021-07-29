# -*- coding: utf-8 -*-
# # How long does a Computron take?
#
#  - [build model of computron\-to\-wallclock relationship · Issue \#3459 · Agoric/agoric\-sdk](https://github.com/Agoric/agoric-sdk/issues/3459)

# ## Preface: Python Data Tools
#
# See also [shell.nix](shell.nix).

# +
import pandas as pd
import numpy as np
import sqlalchemy as sqla
import matplotlib.cm as cm
import dask
import dask.dataframe as dd
import dask.bag as db

dict(pandas=pd.__version__,
     numpy=np.__version__,
     sqlalchemy=sqla.__version__,
     dask=dask.__version__)
# -

# ### Notebook / Scripting Authority
#
# As a nod to OCap discipline, we avoid ambient authority unless we're in a `TOP`-level scripting or notebook context.

TOP = __name__ == '__main__'

# Logging is a bit of an exception to OCap discipline, as is stderr.

# +
import logging
from sys import stderr

logging.basicConfig(level=logging.INFO, stream=stderr,
                    format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger(__name__)
if TOP:
    log.info('notebook start')
# -

# ### Dask Parallel Scheduler UI

# +
from dask.distributed import Client, LocalCluster

if TOP:
    cluster = LocalCluster(n_workers=8)
    client = Client(cluster)

TOP and client
# -

# ## Result Store

# +
db4_uri = 'sqlite:///slog4.db'

if TOP:
    db4 = sqla.create_engine(db4_uri)
# -

# ## SLog files
#
# [rclone support for Google drive](https://rclone.org/drive/)
#
# > This contains 564GB of data from 117 participants, spread across 172 slogfiles ...
#
# ```
# [nix-shell:~/t4]$ rclone sync --progress 'Engineering:/2021-07-04 testnet phase4-stress data/validator slogfiles' ./slogfiles/
# Transferred:       78.633G / 78.633 GBytes, 100%, 101.302 MBytes/s, ETA 0s
# Checks:                 5 / 5, 100%
# Transferred:          182 / 182, 100%
# Elapsed time:     13m16.0s
# ```
#

# +
import importlib
import slogdata
importlib.reload(slogdata)
from slogdata import SlogAccess, CLI, show_times

if TOP:
    def _dir(path):
        import pathlib
        return pathlib.Path(path)
    def _cli(bin):
        from subprocess import run, Popen
        return CLI(bin, run, Popen, debug=True)
    _sa4 = SlogAccess(_dir('/home/customer/t4/slogfiles'),
                      _cli('/home/customer/projects/gztool/gztool'))

TOP and show_times(_sa4.get_records('pathrocknetwork/chain-15.pathrocknetwork.slog.gz', 7721, 2))
# -

_bySize = _sa4.files_by_size()
_bySize

_bySize[_bySize.parent == 'KingSuper']

TOP and _bySize[::5].set_index('name')[['st_size']].plot.barh(
    title='slogfile sizes (sample)',
    figsize=(10, 8));

# ### random access with `gztool`
#
# [gztool](https://github.com/circulosmeos/gztool) `a03c5b4fd5b3` Jul 13 2021.
#
#
# ```
# ~/projects/gztool/gztool -C -e */*.slog.gz
# ...
# ERROR: Compressed data error in 'atlantean/atlantean-agorictest16-chain.slog.gz'.
# ...
# Index file 'ZenQQQ/ZenQQQ-agorictest16-chain.slog.gzi' already exists and will be used.
# Processing 'ZenQQQ/ZenQQQ-agorictest16-chain.slog.gz' ...
# Processing index to 'ZenQQQ/ZenQQQ-agorictest16-chain.slog.gzi'...
#
# 172 files processed
# 1 files processed with errors!
# ```

# +
# count lines on all slogfiles in parallel
# TODO: if it's already in the DB, don't compute it again.

if TOP:
    _withLines = _bySize.assign(
        lines=db.from_sequence(_bySize.values).map(
            lambda v: _sa4.line_count(*v[1:3])).compute())

TOP and _withLines
# -

_withLines.to_sql('file_meta', db4, index=False, if_exists='replace')

# !sqlite3 slog4.db '.header on' '.mode column' 'select * from file_meta limit 3'

_withLines = pd.read_sql_table('file_meta', db4)


# +
def file_chart(slogdf, sample=5, **plotkw):
    df = slogdf[['name', 'st_size', 'lines']].copy()
    df['b64'] = df.st_size / 64
    df.drop('st_size', axis=1, inplace=True)
    df.set_index('name')[::sample].plot.barh(**plotkw)

TOP and file_chart(_withLines, title='slogfile sizes (sample)', figsize=(10, 8))
# -

# ## slogfile basics

pd.read_sql("""
select st_size, lines
from file_meta
order by st_size desc
""", db4).describe()


# ## Runs, Blocks, and Deliveries
#
# > split each slogfile into runs (each beginning with an import-kernel event)

# +
def partition_lines(lines, step=1000000):
    """Note: line numbers are **1-based**
    """
    lo = pd.DataFrame.from_records([
        dict(start=lo, qty=min(lines + 1 - lo, step), lines=lines)
        for lo in range(1, lines + 1, step)])
    return lo

partition_lines(_withLines.lines.iloc[-1])


# +
#client.restart()

# +
# # !sqlite3 slog4.db 'drop table run'

# +
def provide_table(engine, table, todo, chunksize=None, index=True):
    if sqla.inspect(engine).has_table(table):
        return pd.read_sql_table(table, engine, chunksize=chunksize)
    df = todo()
    df.to_sql(table, engine, index=index)
    return df

def runs_todo(withLines):
    runs = dd.from_delayed([
        dask.delayed(_sa4.provide_runs)(f.parent, f['name'], part.start, part.qty)
        for fid, f in withLines.iterrows()
        for _, part in partition_lines(f.lines).iterrows()
    ]).compute().sort_values(['file_id', 'line'])
    withNames = pd.merge(runs, withLines[['file_id', 'parent', 'name', 'st_size', 'lines']],
                         on='file_id')
    # Compute end times
    byFile = withNames.groupby('file_id')
    runs = pd.concat([
        withNames,
        byFile.apply(lambda g: pd.DataFrame(dict(time_end=g.time.shift(-1)))),
        byFile.apply(lambda g: pd.DataFrame(dict(line_end=g.line.shift(-1)))),
    ], axis=1)
    runs.line_end = np.where(runs.line_end.isnull(), runs.lines, runs.line_end)
    return runs.sort_values(['st_size', 'file_id', 'line']).reset_index(drop=True)

_runs = provide_table(db4, 'run', lambda: runs_todo(_withLines))
# -

# !sqlite3 slog4.db '.schema run'

show_times(_runs, ['time', 'time_end'])[['st_size', 'line', 'line_end', 'parent', 'file_id', 'time', 'time_end']]

# ### runs per slogfile

df = _runs.groupby('file_id')[['line']].count()
df.describe()

# +
df = pd.read_sql("""
select file_id, count(*) runs, name, st_size, lines
from run r
-- join file_id s on s."index" = r.slogfile
group by file_id
order by 2
""", db4)

df.set_index('name')[['runs']][::5].plot.barh(
    log=True,
    title='slogfile runs (sample)',
    figsize=(10, 8));
# -

# ## agorictest-16 genesis: `2021-07-01 19:00:00`

gen16 = show_times(pd.DataFrame(dict(blockHeight=64628, blockTime=[1625166000], ts=1625166000)), ['blockTime'])
gen16

# ## Block end start / finish events

# +
import importlib
import slogdata
from slogdata import SlogAccess
importlib.reload(slogdata)

_sa4 = SlogAccess(_dir('/home/customer/t4/slogfiles'),
                  _cli('/home/customer/projects/gztool/gztool'))

show_times(
    _sa4.provide_blocks('ChainodeTech', 'agorictest-16_chain.slog.gz', 1, 1000000)
)


# -

# ## Separate runs by chain

# +
def first_block(sa, run,
                head=5000,
                ts=gen16.ts[0]):
    log.info('1st block: %s/%s', run.parent, run['name'])
    qty = min(int(run.line_end) - run.line + 1, head)
    df = sa.get_blocks(f'{run.parent}/{run["name"]}', run.line, qty)[:2]
    if not len(df):
        return pd.DataFrame.from_records([dict(
            blockHeight=-1,
            blockTime=-1,
            run=run.name,
            chain=np.nan)], index=[run.name])
    df = df.assign(run=run.name,
                   chain=16 if df.blockTime[0] >= ts else 15)
    return df

show_times(first_block(_sa4, _runs.loc[0]))


# +
def run2chain(sa, runs):
    df = runs.apply(lambda run: first_block(sa, run).iloc[0][['blockHeight', 'blockTime', 'chain']],
                    axis=1)
    return df

_r2c = run2chain(_sa4, _runs)
_r2c
# -

_runchain = pd.concat([_runs.drop(columns=['index']), _r2c], axis=1)
_runchain.to_sql('runchain', db4)
_runchain.groupby('chain')[['line']].count()

# !sqlite3 slog4.db '.header on' '.mode column' 'select * from runchain limit 3'

_runchain = pd.read_sql('runchain', db4)
_runchain.groupby('chain')[['line']].count()

_runs['chain'] = _runchain.chain
_runs.groupby('chain')[['file_id', 'lines']].count()


# +
# # !sqlite3 slog4.db 'drop table blockval;'

# +
def blockval_todo(file_meta):
    return dd.from_delayed([
        dask.delayed(_sa4.provide_blocks)(f.parent, f['name'], part.start, part.qty)
        for fid, f in file_meta.iterrows()
        for _, part in partition_lines(f.lines).iterrows()
    ]).compute()

_blockval = provide_table(db4, 'blockval', lambda: blockval_todo(_withLines), index=True)
show_times(_blockval)
# -

# !sqlite3 slog4.db '.schema blockval'

pd.read_sql("""
select file_id, max(blockHeight)
from blockval
where blockTime >= 1625166000
group by file_id
order by 2 desc
""", db4)

# ### Consensus Block-to-Block Time

# +
# db4.execute("""drop table if exists block""")
# -

db4.execute("""
create table block as
  select distinct
         case when blockTime >= 1625166000 then 16 else 15 end chain
       , blockHeight, blockTime
  from blockval
  order by blockTime
""")
pd.read_sql("""
select * from block limit 10
""", db4)

# ### What is the range of blocks in `agorictest-16`?

pd.read_sql("""
select lo, n, lo + n - 1,  hi from (
select min(blockHeight) lo, max(blockHeight) hi, count(distinct blockHeight) n
from block
where chain = 16
)
""", db4)

# +
blk16 = pd.read_sql("""
select blockHeight, blockTime
from block
where chain = 16
""", db4, index_col='blockHeight')

show_times(blk16).describe(datetime_is_numeric=True)
# -

b16time = pd.read_sql("""
select * from block
where chain = 16
""", db4, index_col='blockHeight')
b16time['delta'] = b16time.shift(-1).blockTime - b16time.blockTime
b16time[['delta']].describe()

b16time[b16time.index < 90527].delta.max()

b16time[b16time.delta == 120]

b16time[['delta']].plot(
    title='agorictest-16 consensus blockTime delta',
    ylabel='sec',
    figsize=(9, 6));

show_times(b16time, ['blockTime']).set_index('blockTime')[['delta']].plot(
    title='agorictest-16 consensus blockTime delta',
    ylabel='sec',
    figsize=(9, 6));

# histogram of block-to-block time delta for agorictest-16. (_Note the log scale on the y axis._)

b16time[['delta']].hist(bins=20, log=True);

df = show_times(b16time, ['blockTime'])
df[df.blockTime <= '2021-07-02 19:00:00'][['delta']].hist(bins=20, log=True);

df[df.blockTime <= '2021-07-02 19:00:00'][['delta']].describe()

# ### How many validators logged each block in agorictest-16?

df = pd.read_sql("""
select blockHeight, count(distinct file_id) qty
from blockval
where sign = -1
and blockTime >= 1625166000
group by blockHeight
""", db4)
df.head()

df.set_index('blockHeight').plot(title='agorictest-16 validator coverage by block', figsize=(9, 6));

# !sqlite3 slog4.db '.schema run'

# +
# db4.execute('drop table if exists blockrun16')
db4.execute("""
create table blockrun16 as
with b as (
  select *
  from blockval
  where blockTime >= 1625166000
)
select file_id
     , (select r."index"
        from run r
        where r.file_id = b.file_id and r.line <= b.line and b.line < r.line_end) run
     , b.line, b.time
     , b.sign
     , blockHeight, blockTime
from b
""")

df = pd.read_sql("""
select * from blockrun16
""", db4)

df.tail()
# -

x = df.groupby('blockHeight')[['run']].count()
x.plot();

x['blockHeight'].sort_values('max').reset_index(drop=True).plot();

# ## Slow Blocks

df = show_times(b16time, ['blockTime'])
df[(df.blockTime <= '2021-07-02 19:00:00') &
   (df.delta >= 30)]

# Which runs include block 72712, which took 31 sec?

b33 = pd.read_sql("""
select lo.file_id, lo.run, lo.line, hi.line - lo.line + 1 range, lo.blockHeight
from blockrun16 lo
join blockrun16 hi on hi.run = lo.run and hi.blockHeight = lo.blockHeight
where lo.blockHeight in (72712)
and lo.sign = -1
and hi.sign = 1
""", db4)
b33

# ## Correlating block start with block end

_blockrun16 = df = pd.read_sql_table('blockrun16', db4)
df.tail()

lo = df[df.sign == -1]
hi = df.shift(-1)
hi = hi[hi.sign == 1]
dur = hi.time - lo.time
# show_times(df, ['time', 'time_end'])
lo['dur'] = dur
lo['s_hi'] = hi.file_id
lo['l_hi'] = hi.line
lo['t_hi'] = hi.time
dur = lo[lo.file_id == lo.s_hi]
show_times(dur, ['time', 'blockTime'])

show_times(
    dur.sort_values('dur').dropna().tail(),
    ['time', 'blockTime', 't_hi']
)

dur[dur.dur.abs() <= 120].plot.scatter(x='blockHeight', y='dur')

dur[['blockHeight', 'dur']].describe()


# ## Cranks in a Block

# +
def long_runs_including(runs, blockrun, blockHeight):
    runs_matching = blockrun[blockrun.blockHeight == blockHeight].run
    runs = runs.assign(length=runs.line_end - runs.line)
    runs = runs[runs.index.isin(runs_matching)]
    return runs.sort_values('length', ascending=False)

_long16 = long_runs_including(_runs, _blockrun16, 64628)
_long16.head()
# -

show_times(dur[dur.run == _long16.index[0]], ['time', 'blockTime', 't_hi'])

_blockrun16[(_blockrun16.run == _long16.index[0]) & (_blockrun16.blockHeight == 64628)].iloc[:2]


# +
def blockrun_records(blockHeight, run, slogAccess, blockrun,
                     target=None, include=None):
    ref = f'{run.parent}/{run["name"]}'
    br = blockrun[(blockrun.run == run.name) & (blockrun.blockHeight == blockHeight)]
    block_start = br.iloc[0]  # assert sign == -1?
    block_end = br.iloc[1]
    length = block_end.line - block_start.line + 1
    df = slogAccess.get_records(f'{run.parent}/{run["name"]}', int(block_start.line), int(length),
                                target=target, include=include)
    return df.assign(file_id=run.file_id)

def get_vats(slogAccess, ref, start, qty):
        df = slogAccess.get_records(ref, start, qty,
                              target='create-vat',
                              include=['create-vat'])
        return df

def vats_in_blockrun(blockHeight, run, slogAccess, blockrun):
    br = blockrun[(blockrun.run == run.name) & (blockrun.blockHeight == blockHeight)]
    block_start = br.iloc[0]  # assert sign == -1?
    block_end = br.iloc[1]
    length = block_end.line - block_start.line + 1
    ref = f'{run.parent}/{run["name"]}'
    df = get_vats(slogAccess, ref, int(block_start.line), int(length))
    return df.assign(blockHeight=blockHeight, parent=run.parent)

# _sa4.get_records('Nodeasy.com/Nodeasy.com-agorictest15-chain.slog.gz', 1662497, 1671912 - 1662497)
vats_in_blockrun(_blockrun16.iloc[0].blockHeight, _runs.loc[_long16.index[0]],
                 _sa4, _blockrun16)
# -

vats_in_blockrun(64629, _runs.loc[_long16.index[0]],
                 _sa4, _blockrun16)


no_deliveries = pd.DataFrame.from_records([
    {'time': 1625198620.6265895,
     'type': 'deliver-result',
     'crankNum': 1291,
     'vatID': 'v11',
     'deliveryNum': 124,
     'kd': object(),
     'line': 1673077,
     'dr': object(),
     'syscalls': 2,
     'method': 'inbound',
     'compute': 119496.0,  # missing compute is possible... from replay.
     'dur': 0.1912224292755127,
    }]).iloc[:0]
no_deliveries.dtypes

# +
import json
import itertools

# {"time":1625059432.2093444,"type":"cosmic-swingset-end-block-start","blockHeight":58394,"blockTime":1625059394}
# {"time":1625059432.2096362,"type":"cosmic-swingset-end-block-finish","blockHeight":58394,"blockTime":1625059394}


def block_cranks(records):
    deliveries = []
    syscalls = 0
    deliver = None
    for record in records:
        ty = record['type']
        if ty == 'deliver':
            deliver = record
            syscalls = 0
        elif ty == 'syscall-result':
            syscalls += 1
        elif ty == 'deliver-result':
            if not deliver:
                log.warn('no deliver? %s', record)
                continue
            dur = record['time'] - deliver['time']
            method = deliver['kd'][2]['method'] if deliver['kd'][0] == 'message' else None
            compute = record['dr'][2]['compute'] if type(record['dr'][2]) is type({}) else np.nan
            detail = dict(record,
                          syscalls=syscalls,
                          kd=deliver['kd'],
                          method=method,
                          compute=compute,
                          dur=dur)
            deliveries.append(detail)
    if deliveries:
        return pd.DataFrame.from_records(deliveries)
    else:
        return no_deliveries


def get_deliveries(slogAccess, ref, start, qty):
    if qty <= 2:  # just block start, block end
        return no_deliveries
    df = slogAccess.get_records(
        ref, int(start), int(qty),
        target=None, include=['deliver', 'deliver-result', 'syscall-result'])
    if len(df) > 0 and 'syscallNum' in df.columns:
        for c in ['syscallNum', 'ksr', 'vsr', 'vd']:
            df = df.drop(columns=list(set(df.columns) & set(['syscallNum', 'ksr', 'vsr', 'vd'])))
        return block_cranks(df.to_dict('records'))
    else:
        return no_deliveries

_g16 = _blockrun16[(_blockrun16.run == _long16.index[0]) & (_blockrun16.blockHeight == 64628)].iloc[:2]
_run1 = _runs.loc[_long16.index[0]]

get_deliveries(_sa4, f'{_run1.parent}/{_run1["name"]}', _g16.iloc[0].line, _g16.iloc[1].line - _g16.iloc[0].line + 1)
# -

df = dur[dur.run == _long16.index[0]].assign(length=dur.l_hi - dur.line + 1)
# df[df.length > 2].head(10)
df[df.dur > 5].head(10)


# +
# https://avi.im/blag/2021/fast-sqlite-inserts/
def run_sql(script, engine):
    for stmt in script.strip().split(';\n'):
        engine.execute(stmt)

run_sql('''
PRAGMA journal_mode = OFF;
PRAGMA synchronous = 0;
PRAGMA cache_size = 1000000;
PRAGMA locking_mode = NORMAL;
PRAGMA temp_store = MEMORY;
''', db4)
# -

len(dur)

dur.to_sql('blockrun16dur', db4, if_exists='replace', chunksize=25000, index=False)

# +
_br2 = _blockrun16[(_blockrun16.run == _long16.index[0]) & (_blockrun16.blockHeight == 64632)].iloc[:2]

get_deliveries(_sa4, f'{_run1.parent}/{_run1["name"]}',
               _br2.iloc[0].line, _br2.iloc[1].line - _br2.iloc[0].line + 1)

# +
# chain_id, vatID, deliveryNum -> blockHeight, kd, compute
import inspect

def provide_deliveries(slogAccess, blockHeight, run, blockrun):
    br = blockrun[(blockrun.run == run.name) & (blockrun.blockHeight == blockHeight)]
    if len(br) < 2:
        return no_deliveries.assign(file_id=-1, chain=-1, blockHeight=blockHeight, run=run.name)
    block_start = br.iloc[0]  # assert sign == -1?
    block_end = br.iloc[1]
    length = int(block_end.line - block_start.line + 1)
    df = slogAccess.provide_data(run.parent, run['name'], int(block_start.line), length,
                                 f'deliveries-{blockHeight}', no_deliveries,
                                 lambda ref, start, qty: get_deliveries(slogAccess, ref, start, qty),
                                 'gzip')
    df = df.assign(chain=run.chain, blockHeight=blockHeight, run=run.name)
    if df.dtypes['chain'] not in ['int64', 'float64'] or 'vatID' not in df.columns or 'vd' in df.columns:
        raise NotImplementedError(f'cols: {df.columns} dtypes: {df.dtypes} block {blockHeight, int(block_start.line)}, run\n{run}')
    return df

df = provide_deliveries(_sa4, 66371, _run1, _blockrun16)

show_times(df)
# -

# Computron rate for just this one block?

df.compute.sum() / df.dur.sum()

# test empty
provide_deliveries(_sa4, 64629, _run1, _blockrun16)

_runs.loc[455:456]

# ## Cranks in one long run starting at agorictest-16 genesis

gen16

df = pd.read_sql("""
with lo as (
    select *
         , time - blockTime delta
    from blockrun16
    where blockHeight = 64628
    and blockTime = 1625166000
    and sign = -1
    and run is not null
), hi as (
    select run, max(blockHeight) hi, max(blockTime) t_hi
    from blockrun16
    where run is not null
    and sign = -1
    group by run
), agg as (
    select lo.*, hi.hi, hi.t_hi
    from lo join hi on lo.run = hi.run
    where abs(delta) < 7
    order by hi.t_hi desc
)
select agg.*, run.parent, run.name
from agg
join run on agg.run = run."index"
limit 5
""", db4)
show_times(df, ['time', 'blockTime', 't_hi'])

show_times(_runs).loc[445]

# +
import json


def run1_deliveries(con, sa, lo, hi, run, br,
                    json_cols=['kd', 'dr'],
                    table='run1'):
    if sqla.inspect(con).has_table(table):
        lo = pd.read_sql(f'select max(blockHeight) + 1 lo from {table}', con).iloc[0].lo
        if_exists = 'append'
    else:
        if_exists = 'replace'
    for blockHeight in range(lo, hi):
        df = provide_deliveries(sa, blockHeight, run, br)
        if not len(df):
            # log.info('block %d: no deliveries', blockHeight)
            continue
        for col in json_cols:
            df[col] = df[col].apply(json.dumps)
        log.info('block %d of %d: %s += %d rows', blockHeight, hi, table, len(df))
        df.to_sql(table, con, if_exists=if_exists, index=False)
        if_exists = 'append'


run1_deliveries(db4, _sa4, 64628, 75000, _runs.loc[445], _blockrun16)
# run1_deliveries(db4, _sa4, 75000, 90530, _runs.loc[445], _blockrun16, table='run1b')
# -

_run1 = df = pd.read_sql('select * from run1 union all select * from run1b', db4)
show_times(_run1.tail(3))

_run1.blockHeight.describe()

_run1[_run1.blockHeight >= 88296 - 2].sort_values('blockHeight').head(30).drop(columns=['kd', 'dr', 'file_id'])

df = _run1[_run1.blockHeight == 88295].sort_values('dur', ascending=False).drop(columns=['kd', 'dr', 'file_id'])
df.head(10)

df[df.dur >= 1]

# TODO: compare `getPayout` here (in 88295) vs something earlier... same computrons? same duration?
#
# e.g. if harden weakset grew, the duration could grow while keeping computrons constant

_run1[_run1.method == 'getPayout'][['compute', 'dur']].describe()

_run1[_run1.method == 'getPayout'].compute.hist()

_run1[(_run1.method == 'getPayout') & (_run1.compute == 31654)].plot.scatter(x='blockHeight', y='dur')

lg = _run1[_run1.blockHeight > 76000]
lg = lg[lg.dur < 1]
lg[(lg.method == 'getPayout') & (lg.compute == 31654)].plot.scatter(x='blockHeight', y='dur')

# Things got slower over time.
#
# Hypothesis: GC didn't happen -> weak set got big -> weakset access time got big

# So computron model should not be based on this range, but rather on pre-loadgen time.

# When looking at comptron / wallclock, we should look at:
#
#  - all getCurrentAmount calls
#  - within a narrow range of blockHeight
#  - that all use the same # of computrons
#
# (as above)
#

b16time[b16time.delta == 224]

_run1[['compute', 'dur']].describe()


# +
def drate(df):
    rate = df.compute / (df.syscalls + 1) / df.dur
    # rate = df.compute / df.dur
    return df.assign(rate=rate)

df = drate(_run1).groupby('method')[['rate']].aggregate(['count', 'mean', 'std', 'max'])
df = df.sort_values(('rate', 'mean'), ascending=False)
df
# -

common = _run1.groupby('method')[['line']].count()
common = common[common.line > 20]
common

drate(_run1[_run1.method.isin(common.index)])[['method', 'rate']].boxplot(by='method', rot=90, figsize=(20, 12))

common.sort_values('line', ascending=False).head()

_run1.blockHeight.describe()

_run1.sort_values('dur', ascending=False)


# This is an always-busy sim, but **TODO** we'd like to look at the arrival pattern that we have.

# +
def sim(df, c_eg, dur_eg, target):
    df = df[df.chain == 16]
    df['running'] = df.compute.cumsum()  # try exp
    threshold = target * (c_eg / dur_eg)
    log.info('threshold: %s', threshold)
    df['sim_blk'] = (df.running / threshold).round()
    # df['adj'] = df.sim_blk - df.blockHeight
    return df.reset_index(drop=True)

df = _run1.drop(columns=['type', 'kd', 'dr', 'file_id', 'line', 'run'])
# df = df[df.method != 'executeContract']
# df = df[df.method == 'getCurrentAmount']  # getPayout

# df.blockHeight = df.blockHeight - df.blockHeight.iloc[0]
df = sim(df, 48390.0, 0.074363, 5)
df = df[df.sim_blk.notnull()]
df.sim_blk = df.sim_blk.astype('int64')
show_times(df)
# -

pd.read_sql('''
select count(distinct run)
from blockrun16
''', db4)

len(_runs)


# +
def nth_block(sa, blockHeight, run, blockrun,
              ts=gen16.ts[0]):
    log.info('%d th block: %s/%s', blockHeight, run.parent, run['name'])
    br = blockrun[(blockrun.blockHeight == blockHeight) & (blockrun.run == run.name)]
    df = provide_deliveries(sa, blockHeight, run, br)
    if not len(df):
        return df
    df = df.assign(run=run.name, chain=run.chain)
    return df


m1b1 = pd.concat(
    df
    for _, run in _runs.iterrows()
    for df in [nth_block(_sa4, 80001, run, _blockrun16)]
    if len(df)
)
m1b1
# -

m1b1[(m1b1.method == 'getCurrentAmount') & (m1b1.deliveryNum == 44721)][['compute', 'dur', 'run']]

df = m1b1[(m1b1.method == 'getCurrentAmount') & (m1b1.deliveryNum == 44721)][['compute', 'dur', 'run']]
df.describe()

# ## Validator speed: 2-4x spread for `getCurrentAmount`

df[['dur']].hist()

# +
# df.groupby('method')[['compute']].describe().loc['executeContract']
# -

df.compute.hist(log=True);

df.dur.hist(log=True);

df[df.dur < .1].dur.hist()

# #### Total delivery duration per block

x = pd.concat([
    df.groupby('blockHeight')[['dur']].sum(),
    df.groupby('sim_blk')[['dur']].sum().rename(columns=dict(dur='dur_sim')),
], axis=1)
x.hist(); # log=True);

x.describe()

x.dur.quantile(.9)

xx = df.groupby('sim_blk')[['dur']].sum().rename(columns=dict(dur='dur_sim'))

xx[xx.dur_sim > 25]

df[df.blockHeight == 88295].sort_values('dur', ascending=False)

df[df.sim_blk == 32607].sort_values('dur', ascending=False)

_run1[_run1.compute == 381240].dur.describe()

_run1[_run1.compute == 381240].plot.scatter(x='blockHeight', y='dur')

# This wasn't a big deal during most of the chain (.25sec 75th percentile).
#
# We could model this within 2x or 3x by ignoring the spike.

# **TODO**: what happened during that spike? is it consensus-observable? kernel-observable?

df = _run1[_run1.compute == 381240]
df[(df.blockHeight >= 88100) & (df.blockHeight < 88400)].plot.scatter(x='blockHeight', y='dur')

df[df.sim_blk == 32607].compute.sum()

df[df.sim_blk == 32607].dur.sum()

df[df.sim_blk == 32607].syscalls.sum()

df.groupby('blockHeight')[['syscalls']].sum().describe()

# #### Total compute per block

x = pd.concat([
    df.groupby('blockHeight')[['compute']].sum(),
    df.groupby('sim_blk')[['compute']].sum().rename(columns=dict(compute='cmp_sim')),
], axis=1)
x.hist(log=True);

x.describe()

cluster.scale(8)

client.restart()

f'{12:04}'


# +
def pick_chain(ht,
               gen=1625166000, hi=16, lo=15):
    return np.where(ht > gen, hi, lo)


def run_deliveries(slogs, sa, run):
    chain_id = f'agorictest-{run.chain}'
    blocks = pd.concat(
        pd.read_csv(blockFile)
        for blockFile in (slogs / run.parent).glob('*-blocks.csv')
    )
    blocks = blocks[(blocks.line >= run.line) &
                    (blocks.line < run.line_end)]
    blocks = blocks.assign(run=run.name)
    heights = blocks.blockHeight.unique()
    log.info('run %s %-3d blocks %.16s %s', run.name, len(heights),
             pd.to_datetime(run.time, unit='s'), run['name'])
    tot = 0
    for blockHeight in heights:
        detail = provide_deliveries(sa, blockHeight, run, blocks)
        if not len(detail):
            continue
        tot += len(detail)
        yield detail
    if not tot:
        yield no_deliveries.assign(file_id=-1, chain=-1, blockHeight=-1, run=run.name)


def by_vat(dest, run, detail):
    chain_id = f'agorictest-{run.chain}'
    run_detail = f'{run.name:04}-{run.parent}-{run.file_id}-{run.line}'
    for vatID, g in detail.groupby('vatID'):
        try:
            (dest / chain_id / vatID).mkdir(parents=True)
        except:
            pass
        vat_dir = dest / chain_id / vatID
        f = vat_dir / f'delivery-detail-{run_detail}.csv.gz'
        log.info('saving to %s:\n%s', f, g.set_index(['vatID', 'deliveryNum'])[['compute', 'dur']].tail(3))
        g.to_csv(f, index=False)
        f = vat_dir / f'delivery-summary-{run_detail}.csv.gz'
        g[['vatID', 'deliveryNum', 'kd', 'syscalls', 'compute']].to_csv(f, index=False)
    return detail.assign(run=run.name).groupby(['run', 'vatID'])[['deliveryNum']].count()

#by_vat(_dir('slogfiles/'), _dir('vat-details/'), _sa4, _runs)

for df in run_deliveries(_dir('slogfiles/'), _sa4, _runs.loc[58]):
    print(df)
    print(by_vat(_dir('vat-details/'), _runs.loc[58], df))
    break


# +
def run_deliveries_todo(sa, slogs, dest, runs):
    def do_run(run):
        df = pd.concat(
            detail
            for detail in run_deliveries(slogs, sa, run)
        )
        return by_vat(dest, run, df)
    todo = (
        dask.delayed(do_run)(run)
        for _, run in runs.iterrows()
    )
    return todo

per_run = dd.from_delayed(run_deliveries_todo(_sa4, _dir('slogfiles/'), _dir('vat-details/'), _runs))
per_run.compute()
# -

pd.to_datetime(1625213913.1672082, unit='s')

# +
import inspect
from slogdata import show_times

db4.execute('drop table if exists crankrun') #@@

def deliveries_todo(sa, blockrun, runs):
    todo = (
        dask.delayed(provide_deliveries)(sa, blockHeight, run,
                                         blockrun[(blockrun.run == run.name) &
                                                  (blockrun.blockHeight == blockHeight)])
        for run_ix, run in runs.iterrows()
        for heights in [blockrun[blockrun.run == run_ix].blockHeight.unique()]
        for _ in [log.info('run %s %-3d blocks %.16s %s', run_ix, len(heights),
                           pd.to_datetime(run.time, unit='s'), run['name'])]
        for blockHeight in heights
    )
    log.info('todo: %s', type(todo))
    df = dd.from_delayed(todo,
                         meta=no_deliveries.assign(file_id=1, chain=1, blockHeight=1, run=1))
    return df.compute()

# _dr16 = provide_table(
#    db4, 'crankrun',
#    #  65517
#    lambda: deliveries_todo(_sa4, _blockrun16[_blockrun16.blockHeight <= 65000], _runs.loc[200:275]))

_dr16 = deliveries_todo(_sa4, _blockrun16,  # [_blockrun16.blockHeight <= 65000]
                        _runs[_runs.chain == 16])

_dr16
# -

# ## deliveries from batch

_delrun = pd.read_sql('select * from delrun', db4)
_delrun.groupby('chain')[['line']].count()


# ## Are compute meter values consistent?

# +
def compute_meter_consistent(df):
    compute_count = df.groupby(['vatID', 'deliveryNum'])[['compute']].nunique()
    dups = compute_count[compute_count['compute'] > 1]
    return pd.merge(dups.reset_index(),
                    df[['run', 'vatID', 'deliveryNum', 'compute']],
                    how='left', suffixes=['_dup', ''],
                    left_on=['vatID', 'deliveryNum'],
                    right_on=['vatID', 'deliveryNum'])

# x = compute_meter_consistent(_alld16).compute()
x = compute_meter_consistent(_delrun[_delrun.chain == 16]).sort_values(['vatID', 'deliveryNum'])  # .compute()
x
# -

compute_meter_consistent(_delrun[_delrun.chain == 15]).sort_values(['vatID', 'deliveryNum'])  # .compute()

# ## Computrons per block

blockdel = _delrun[_delrun.method != 'executeContract']
key = ['chain', 'blockHeight', 'vatID', 'deliveryNum', 'compute']
blockdel = blockdel.sort_values(key).drop_duplicates()
df = blockdel.groupby(['chain', 'blockHeight'])[['deliveryNum']].count().sort_index()
df.plot()

_bkcomp = df = blockdel.groupby(['chain', 'blockHeight'])[['compute']].sum()
df

df.plot()


# +
def type2sign(df):
    df['sign'] = np.where(df.type == 'cosmic-swingset-end-block-start', -1, 1)
    return df

def byChain(df, gen=gen16.ts[0], hi=16, lo=15):
    return df.assign(chain=np.where(df.blockTime >= gen, hi, lo))
    return df

def slog_blocks(slogfiles,
                pattern='**/*-blocks.csv'):
    df = pd.concat(type2sign(pd.read_csv(p)[['type', 'blockHeight', 'blockTime']])
                   for p in slogfiles.glob(pattern))
    df = byChain(df)
    key = ['chain', 'blockHeight', 'blockTime']
    df = df[key].sort_values(key).drop_duplicates()
    return df.reset_index(drop=True)

_blk = slog_blocks(_dir('slogfiles/'))    
_blk.tail()
# -

_byChain = _blk.groupby('chain')
df = pd.merge(
    _byChain[['blockHeight']].nunique(),
    _byChain[['blockHeight']].aggregate(['min', 'max'])['blockHeight'],
    left_index=True, right_index=True,
)
df['span'] = df['max'] - df['min'] + 1
df


# +
def blockdur(df):
    df = df.set_index(['chain', 'blockHeight'])
    df['dur'] = df.shift(-1).blockTime - df.blockTime
    return df

_bkdur = blockdur(_blk)
_bkdur
# -

# compute by block with duration
_bkcmpdur = _bkcomp.join(_bkdur, lsuffix='_d', rsuffix='_b')
_bkcmpdur['rate'] = (_bkcmpdur.compute / _bkcmpdur.dur).astype(float)
_bkcmpdur

_bkcmpdur[_bkcmpdur.dur > _bkcmpdur.dur.quantile(0.99)]

df = _bkcmpdur.loc[16]
df[df.dur < 8][['rate']].hist(log=True)

_bkcmpdur[_bkcmpdur.dur < 8][['rate']].describe()

# ## simulation

_delrun.groupby('run')[['line']].count()

_delrun[['crankNum', 'run']].groupby(['crankNum'])[['run']].aggregate(['count']).plot()


# +
def sim(df, percentile):
    df = df[df.chain == 16]
    df = df[df.method != 'executeContract']
    key = ['blockHeight', 'crankNum', 'vatID', 'deliveryNum', 'compute']
    df = df.groupby(key)[['dur']].aggregate(['count', 'mean', 'median', 'sum'])
    return df
    df = df[['blockHeight', 'crankNum', 'vatID', 'deliveryNum', 'compute']].sort_values(
        ['blockHeight', 'crankNum', 'vatID', 'deliveryNum']).drop_duplicates()
    threshold = df.compute.quantile(percentile)
    df['running'] = df.compute.cumsum()
    df['sim_block'] = (df.running / threshold).round()
    return df.reset_index(drop=True)

df = sim(_run1, .99)
df
# -

df[['blockHeight']].plot()

df.set_index('blockHeight')[['sim_block']].plot()

# ## Compute rate by vat

plt.cm.rainbow[1]

pd.Categorical(_delrun.method.dropna(), ordered=True)

# +
import matplotlib as plt

def cmap_of(df, color,
            cmap=plt.cm.get_cmap('hot')):
    df = df.loc[:, [color]].fillna('???')
    byColor = df.groupby(color).count() #.set_index(color)
    byColor['unit'] = range(len(byColor))
    byColor.unit = byColor.unit / len(byColor)
    byColor['color'] = byColor.unit.apply(cmap)
    return byColor.loc[df[color]].color

cmap_of(_delrun, 'method')


# +
def vat_rate(df, vatID):
    df = df[['vatID', 'deliveryNum', 'compute', 'dur']].dropna()
    df['rate'] = df.compute / df.dur
    df = df[df.vatID == vatID]
    # df = df.groupby('deliveryNum')[['compute', 'dur', 'rate']].mean()
    #df.sort_values('dur', ascending=False)
    #df
    df = df.set_index('deliveryNum').sort_index()
    return df

def show_rate(df, vatID, figsize=(8, 9)):
    df = vat_rate(df, vatID)
    ax = df.plot(subplots=True, figsize=figsize)
    
def fit_line(df, x, y, color=None, figsize=(9, 6)):
    df = df[~df[x].isnull() & ~df[y].isnull()]
    cs = np.polyfit(df[x], df[y], 1)
    f = np.poly1d(cs)
    if color:
        color = cmap_of(df, color)
    ax1 = df[[x, y]].plot.scatter(x=x, y=y, color=color, figsize=figsize)
    df['fit'] = f(df[x])
    df.plot(x=x, y='fit', color='Red', legend=False, ax=ax1);


# show_rate(start1, 'v10');
# vat_rate(start1, 'v10').plot.scatter(x='compute', y='dur')
# fastSlog = start1[start1.slogfile == 'PDPnodeTestnet-agorictest16-chain.slog.gz']
# fit_line(vat_rate(fastSlog, 'v10'), 'compute', 'dur')
# len(fastSlog[fastSlog.vatID == 'v10'])
# fastSlog[fastSlog.vatID == 'v10'].drop(['kd', 'dr'], axis=1) #.sort_values('compute', ascending=False)
#fastSlog[fastSlog.vatID == 'v10'].set_index('deliveryNum').sort_index()[['compute', 'dur']].plot(subplots=True)

fit_line(_delrun[_delrun.chain == 16], 'compute', 'dur', color='method')
# -

_r = _delrun[['compute', 'dur', 'method']].assign(rate=_delrun.compute / _delrun.dur)
_r.groupby('method')[['rate']].describe().sort_values(('rate', 'mean'))

df.sort_values(('compute', 'mean'))

df = fastSlog[fastSlog.vatID == 'v10']
df['rate'] = df.compute / df.dur
df[['deliveryNum', 'dur', 'compute', 'rate']].set_index('deliveryNum').plot(subplots=True)

df.rate.describe()

# ### exclude dynamic vat creation

fastSlog.groupby('method')[['compute']].mean().plot.barh(log=True, figsize=(12, 10))

noContract = df =fastSlog[fastSlog.method != 'executeContract'].copy()
df['rate'] = df.compute / df.dur
df[['dur', 'compute', 'rate']].plot(subplots=True)

fit_line(noContract, 'compute', 'dur')

fit_line(fastSlog, 'compute', 'dur')

# ## Add syscalls to the model

df = noContract
cs = np.polyfit(df[['compute', 'syscalls']], df['dur'], 1)

df = _dr16.assign(chain_id=16)
df = df[['chain_id', 'vatID', 'deliveryNum', 'blockHeight', 'kd', 'compute']].drop_duplicates()
df = df.set_index(['chain_id', 'vatID', 'deliveryNum']).sort_index()
df[df.index.duplicated()]
df

df.loc[16].loc['v1'].loc[0]

_dr16.query('(deliveryNum == 0) & (vatID == "v1")').groupby('compute')[['line']].count()

pd.merge(_dr16,
         df[df.index.duplicated()].reset_index()[['vatID', 'deliveryNum']],
         left_on=['vatID', 'deliveryNum'], right_on=['vatID', 'deliveryNum']
        )[['vatID', 'deliveryNum', 'blockHeight', 'kd', 'compute']]
# _dr16.assign(chain_id=16).set_index(['chain_id', 'vatID', 'deliveryNum'])


dall = pd.concat(
    pd.read_csv(f)
    for f in _dir('slogfiles/').glob('**/*-deliveries-*.csv.gz')
)
dall


# +
def load_deliveries(files, con, table):
    if_exists = 'replace'
    for file in files:
        df = pd.read_csv(file)
        df.to_sql(table, con, if_exists=if_exists)
        if_exists = 'append'
        log.info('loaded %d records from %s', len(df), file)

load_deliveries(
    _dir('slogfiles/').glob('**/*-deliveries-*.csv.gz'),
    db4,
    'delrun3')
# -

# ### Did we ever do more than 1000 cranks in a block?
#
# if not, current policy never fired

df = _dr16[['blockHeight', 'crankNum']].drop_duplicates()
df.groupby('blockHeight')[['crankNum']].count().sort_values('crankNum', ascending=False)

# ## @@ Older approaches

# ## Delivery statistics
#
# > For each delivery in the corpus, we want to get statistics on the range of wallclock times taken by these validators.

# +
import gzip
import itertools


def iter_cranks(path):
    """split each slogfile into runs (each beginning with an import-kernel event),
    process each run by finding sequential matching deliver+deliver-result pairs,
    turn each pair into a (crankNum, computrons, wallclock) triple
    """
    log.info('iter_cranks: %s', path)
    with gzip.open(path) as f:
        kernel = None
        deliver = None
        block = None
        syscalls = None
        for (ix, line) in enumerate(f):
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                log.warning('%s:%d: bad JSON: %s', path.name, ix, repr(line))
                continue
            ty = data['type']
            # print(ix, data['type'], kernel, deliver)
            if ty == 'import-kernel-finish':
                kernel = data
                deliver = None
                syscalls = None
                yield dict(kernel,
                           slogfile=path.name, line=ix)
            elif ty == 'create-vat':
                yield dict(slogfile=path.name,
                           line=ix,
                           time=data['time'],
                           type=ty,
                           vatID=data['vatID'],
                           description=data['description'],
                           managerType=data['managerType'],
                           time_kernel=kernel['time'])
# {"time":1625059432.2093444,"type":"cosmic-swingset-end-block-start","blockHeight":58394,"blockTime":1625059394}
# {"time":1625059432.2096362,"type":"cosmic-swingset-end-block-finish","blockHeight":58394,"blockTime":1625059394}
            elif ty == 'cosmic-swingset-end-block-start':
                block = data
            elif ty == 'cosmic-swingset-end-block-finish':
                time = data['time']
                time_start = block['time']
                dur = time - time_start
                if kernel:
                    time_kernel = kernel['time']
                else:
                    log.warning('%s:%d: missing kernel context', path.name, ix)
                    time_kernel = np.nan
                yield dict(slogfile=path.name,
                           line=ix,
                           time=time,
                           type=ty,
                           time_start=time_start,
                           dur=dur,
                           blockHeight=data['blockHeight'],
                           blockTime=data['blockTime'],
                           time_kernel=time_kernel)
                block = None
            elif deliver is None:
                if ty == 'deliver':
                    deliver = data
                    syscalls = 0
            elif data['type'] == 'deliver-result':
                time = data['time']
                time_start = deliver['time']
                dur = time - time_start
                method = deliver['kd'][2]['method'] if deliver['kd'][0] == 'message' else None
                compute = data['dr'][2]['compute'] if type(data['dr'][2]) is type({}) else None
                if block:
                    blockHeight = block['blockHeight']
                    blockTime=block['blockTime']
                else:
                    # odd... how do we get here without block info???
                    log.warning('%s:%d: missing block context', path.name, ix)
                    blockHeight = blockTime = np.nan
                if kernel:
                    time_kernel = kernel['time']
                else:
                    log.warning('%s:%d: missing kernel context', path.name, ix)
                    time_kernel = np.nan
                yield dict(slogfile=path.name,
                           line=ix,
                           time=time,
                           type=ty,
                           crankNum=data['crankNum'],
                           deliveryNum=data['deliveryNum'],
                           vatID=data['vatID'],
                           kd=deliver['kd'],
                           method=method,
                           syscalls=syscalls,
                           dr=data['dr'],
                           compute=compute,
                           time_start=time_start,
                           dur=dur,
                           blockHeight=blockHeight,
                           blockTime=blockTime,
                           time_kernel=time_kernel)
                deliver = None
            elif ty == 'syscall-result':
                syscalls += 1
            elif ty in ['clist', 'syscall']:
                continue
            else:
                log.warning("%s:%d: expected deliver-result; got: %s", path.name, ix, ty)
                deliver = None


def sample(files=50, cranks=2000, slogdir=slogdir):
    return pd.DataFrame.from_records(
        r
        for slogfile in itertools.islice(slogdir.glob('**/*.slog.gz'), files)
        for r in  itertools.islice(iter_cranks(slogfile), cranks))

# files_top = sample(200, 100)
c500 = sample()
# -

show_times(
files_top[files_top.crankNum == 1][[
    'slogfile', 'line', 'time', 'vatID', 'deliveryNum', 'syscalls', 'compute', 'time_kernel', 'blockHeight']
].sort_values('blockHeight').set_index(['slogfile', 'line']),
    ['time'])


# +
def show_times(df, cols):
    out = df.copy()
    for col in cols:
        out[col] = pd.to_datetime(out[col], unit='s')
    return out

def slogfile_summary(df):
    g = df.groupby(['slogfile', 'type'])
    out = g[['line']].count()
    out['time_min'] = g[['time']].min().time
    out['time_max'] = g[['time']].max().time
    out['blockHeight_min'] = g[['blockHeight']].min().blockHeight
    # out['blockHeight_max'] = g[['blockHeight']].max().blockHeight
    out['crankNum_min'] = g[['crankNum']].min().crankNum
    return show_times(out, ['time_min', 'time_max'])

slogfile_summary(files_top) # [files_top.type == 'deliver-result']).sort_values('crankNum_min', ascending=False).head(15)


# +
def stuff(df, slogfile):
    return df[(df.slogfile==slogfile) &
         (df.type == 'deliver-result')][['crankNum', 'vatID', 'deliveryNum', 'kd', 'line', 'blockHeight' ]]


coolex = stuff(c500, 'coolex-agorictest16-chain.slog.gz').set_index('crankNum')
mym = stuff(c500, 'mymoniker-agorictest16-chain.slog.gz').set_index('crankNum')
xwalk = pd.merge(coolex, mym, left_index=True, right_index=True)
xwalk[xwalk.kd_x != xwalk.kd_y]
# -

xwalk[xwalk.deliveryNum_y == 2801].kd_y.iloc[0]

# warner says: suppose we have 2 deliverInboundAcks
#
# when swingset tells mb device, device consults state _in RAM_ for dup ack num...
# not durable... differs between run-from-start and restart

# ## global crankNum -> vatID, deliveryNum

cranks = c500[c500['type'] == 'deliver-result']
cranks = cranks[['chain_id', 'crankNum', 'vatID', 'deliveryNum']].set_index(['chain_id', 'crankNum']).drop_duplicates().sort_index()
cranks # .sort_values('deliveryNum')

c500 = c500[~c500.line.isnull()]
show_times(c500[c500.blockHeight == 64628], ['time', 'time_start', 'blockTime'])

cranks.pivot(columns='vatID', values='deliveryNum')

cranks.plot(subplots=True)

c500[['kd']].dropna()

c500[['compute']].dropna()

# +
## reduced data set

# chain-wide deliveries
# chain_id, crankNum -> blockHeight, vatID, deliveryNum, kd, compute

# chain_id, vatID, deliveryNum -> blockHeight, kd, compute
# except vatTP?

# per-validator data
# chain_id, crankNum, run (slogfile, kernel-start) -> dur


# +
# global crankNum -> vatID, deliveryNum

c500[['crankNum', 'vatID', 'deliveryNum']].set_index()

# ignore un-full blocks?
# histogram of block durations; interval between...
# {"time":1625059432.2093444,"type":"cosmic-swingset-end-block-start","blockHeight":58394,"blockTime":1625059394}
# {"time":1625059432.2096362,"type":"cosmic-swingset-end-block-finish","blockHeight":58394,"blockTime":1625059394}

# "blockTime":1625059381 <- consensus block time is median of block times (?)   


# vatID, deliveryNum -> args / syscalls
# watch out for GC esp.

# c.run(runPolicy)
# simple model: kernel says how many computrons
# refinement: computrons, syscalls

# fitness: block distribution... 10s blocks...
#   blocks that aren't too big (latency, validator variance risk)
#   cpu that isn't idle (throughput)
# an ideal: median block time 10s
# 80 20 %ile


# importing a contract is an outlier


# +
# median validator - existing distribution of deliveries / compute -> blocks
#  supplement: study wallclock stuff
# -

show_times(c500[c500['type'] == 'deliver-result'].set_index(['crankNum', 'vatID', 'deliveryNum', 'slogfile'])
           .drop(['type', 'kd', 'dr', 'time_dr', 'description', 'managerType'], axis=1).sort_index(),
           ['time', 'time_kernel', 'blockTime'])

# ### Missing `compute` meter info?

start1 = c500
start1[(start1['type'] == 'deliver-result') & start1.compute.isnull()]

compute_ref = start1[(start1.slogfile == 'coolex-agorictest16-chain.slog.gz') &
                     (start1['type'] == 'deliver-result')].set_index('crankNum')[['compute']]
compute_ref

compute_delta = start1[['slogfile', 'crankNum', 'compute']]
compute_delta = pd.merge(compute_delta, compute_ref,
                         left_on='crankNum', right_index=True, suffixes=['', '_ref'])
compute_delta['delta'] = (compute_delta.compute - compute_delta.compute_ref).abs()
compute_delta.sort_values('delta', ascending=False)

# +
df = start1
categories = df.vatID.apply(lambda v: int(v[1:]))
colors = cm.rainbow(np.linspace(0, 1, categories.max() + 1))

df.plot.scatter(x='compute', y='dur', c=colors[categories],
                title='Deliveries (colored by vatID)',
                figsize=(12, 9), ylabel="dur (sec)");
# -

start1[~start1.compute.isnull()].groupby('vatID')[['crankNum']].count().sort_values('crankNum', ascending=False)


# +
def vat_rate(df, vatID):
    df = df[['vatID', 'deliveryNum', 'compute', 'dur']].dropna()
    df['rate'] = df.compute / df.dur
    df = df[df.vatID == vatID]
    # df = df.groupby('deliveryNum')[['compute', 'dur', 'rate']].mean()
    #df.sort_values('dur', ascending=False)
    #df
    df = df.set_index('deliveryNum').sort_index()
    return df

def show_rate(df, vatID, figsize=(8, 9)):
    df = vat_rate(df, vatID)
    ax = df.plot(subplots=True, figsize=figsize)
    
def fit_line(df, x, y, figsize=(9, 6)):
    cs = np.polyfit(df[x], df[y], 1)
    f = np.poly1d(cs)
    ax1 = df[[x, y]].plot.scatter(x=x, y=y, figsize=figsize)
    df['fit'] = f(df[x])
    df.plot(x=x, y='fit', color='Red', legend=False, ax=ax1);


# show_rate(start1, 'v10');
# vat_rate(start1, 'v10').plot.scatter(x='compute', y='dur')
fastSlog = start1[start1.slogfile == 'PDPnodeTestnet-agorictest16-chain.slog.gz']
fit_line(vat_rate(fastSlog, 'v10'), 'compute', 'dur')
# len(fastSlog[fastSlog.vatID == 'v10'])
# fastSlog[fastSlog.vatID == 'v10'].drop(['kd', 'dr'], axis=1) #.sort_values('compute', ascending=False)
#fastSlog[fastSlog.vatID == 'v10'].set_index('deliveryNum').sort_index()[['compute', 'dur']].plot(subplots=True)
# -

vat_rate(start1, 'v16');

df = start1.pivot(columns='vatID', values=['compute', 'dur'],
                  index=['vatID', 'deliveryNum', 'crankNum', 'slogfile', 'line'])
df.reset_index().set_index('deliveryNum').drop(['crankNum', 'line'], axis=1) #.plot(figsize=(12, 8));

df.reset_index().set_index('deliveryNum')[['v23']].sort_index().dropna() #.plot()

df.describe()

df[['v14']].dropna()

df.crankNum.hist();

df.deliveryNum.hist();

df.groupby('method')[['compute', 'rate']].describe()

df.groupby('method')[['rate', 'compute', 'dur']].mean().sort_values('rate').head(90).plot(
    subplots=True, rot=90, figsize=(8, 6), title='Method Compute Cost, Rate: bottom 90');

df.groupby('method')[['rate', 'compute', 'dur']].mean().sort_values('rate').tail(8).plot(
    subplots=True, rot=90, figsize=(8, 6), title='Method Compute Cost, Rate: top 8');

durByMethod.dur.sum()

# +
durByMethod = df.groupby('method')[['dur']].sum().sort_values('dur', ascending=False)

durByMethod.plot.pie(y='dur', figsize=(12, 9), autopct='%1.1f%%')
# -

df.groupby('vatID')[['rate']].describe().head(20)

df.groupby('slogfile')[['rate']].describe().head(20)

df.plot.scatter(x='deliveryNum', y='rate')

speed = df.groupby('slogfile')[['rate']].describe()[['rate'][0]][['count', 'mean', 'std']]
speed = speed.sort_values('mean', ascending=False)
speed['relative'] = speed['mean'] / speed['mean'][0]
speed


# +
def boxplot_sorted(df, by, column, **config):
  df2 = pd.DataFrame({col:vals[column] for col, vals in df.groupby(by)})
  meds = df2.median().sort_values()
  return df2[meds.index].boxplot(**config)

ax = boxplot_sorted(df, by=["slogfile"], column="rate", rot=90, figsize=(12, 9))
ax.set_title('Validator Speed: Sample of 20 from Phase 4');
ax.set_ylabel('computrons / sec')
# -

ax = df.sort_values('crankNum').plot.scatter(x='crankNum', y='compute');
ax.set_yscale('log')

df[(df.dur < df.dur.mean() + df.dur.std()) &
   (df.compute < df.compute.mean() + df.compute.std())][['compute', 'dur']].hist();

# +
df = crank_info(c500)
df = df[df.crankNum.isin(compute_ref.index)]

rate = np.polyfit(df.compute, df.dur, 1)
f = np.poly1d(rate)
df['rate'] = f(df.compute)
# df[['compute', 'dur', 'rate']].head()
print(f)
# -

ax1 = df[['compute', 'dur']].plot.scatter(x='compute', y='dur', figsize=(9, 6))
df.plot(x='compute', y='rate', color='Red', legend=False, ax=ax1);
ax1.set_title(f"{len(df)} cranks from w3m: Duration vs. Compute Meter");
ax1.set_xlabel("compute units")
ax1.set_ylabel("duration (sec)")

r = df.compute / df.dur

r.max() / r.min()

df.sort_values('rate', ascending=False).drop(['time', 'type', 'detail', 'detail_dr'], axis=1)

# ## Colophon: jupytext
#
# This is a jupyter notebook paired with a python script using [jupytext](https://jupytext.readthedocs.io/en/latest/).
#
# We use the [python38Packages.jupytext](https://search.nixos.org/packages?channel=21.05&from=0&size=50&buckets=%7B%22package_attr_set%22%3A%5B%22python38Packages%22%5D%2C%22package_license_set%22%3A%5B%5D%2C%22package_maintainers_set%22%3A%5B%5D%2C%22package_platforms%22%3A%5B%5D%7D&sort=relevance&query=jupytext) nix package; in particular,  `/nix/store/a9911qj06dy0ah7fshl39x3w4cjs7bxk-python3.8-jupytext-1.11.2`.
#
