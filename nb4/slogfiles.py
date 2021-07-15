# -*- coding: utf-8 -*-
# # How long does a Computron take?
#
#  - [build model of computron\-to\-wallclock relationship · Issue \#3459 · Agoric/agoric\-sdk](https://github.com/Agoric/agoric-sdk/issues/3459)

# ## TODO: jupytext
#
# https://github.com/mwouts/jupytext
#
# as in https://github.com/kumc-bmi/naaccr-tumor-data/blob/master/pyspark_explore/requirements.txt
#

# ## Preface: Python Data Tools
#
# See also [shell.nix](shell.nix).

import logging
from sys import stderr
logging.basicConfig(level=logging.INFO, stream=stderr)
log = logging.getLogger(__name__)
log.info('logging looks like this')

# +
import sqlalchemy as sqla

import pandas as pd
import numpy as np
import matplotlib.cm as cm

import dask
import dask.dataframe as dd
import dask.bag as db

dict(pandas=pd.__version__,
     numpy=np.__version__,
     sqlalchemy=sqla.__version__,
     dask=dask.__version__)
# -

# ### Dask Parallel Scheduler UI

from dask.distributed import Client, LocalCluster
cluster = LocalCluster(n_workers=8)
client = Client(cluster)
client

# ## Result Store

db4 = sqla.create_engine('sqlite:///slog4.db')


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
def _slogfiles(path='/home/customer/t4/slogfiles'):
    import pathlib
    return pathlib.Path(path)

slogdir = _slogfiles()

slogdf = pd.DataFrame.from_records([
    dict(
        path=slogfile,
        parent=slogfile.parent.name,
        name=slogfile.name,
        st_size=slogfile.stat().st_size
    )
    for slogfile in slogdir.glob('**/*.slog.gz')
]).sort_values('st_size').reset_index(drop=True)

slogdf.drop(['path'], axis=1)
# -

slogdf[::5].set_index('name')[['st_size']].plot.barh(
    title='slogfile sizes (sample)',
    figsize=(10, 8));

# ### random access with `gztool`
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
from contextlib import contextmanager
from subprocess import PIPE

class CLI:
    def __init__(self, bin, run, popen):
        self.bin = bin
        self.__run = run
        self.__popen = popen

    def run(self, *args):
        cmd = [self.bin] + [str(a) for a in args]
        return self.__run(cmd, capture_output=True)

    @contextmanager
    def pipe(self, *args):
        cmd = [self.bin] + [str(a) for a in args]
        with self.__popen(cmd, stdout=PIPE) as proc:
            yield proc.stdout

def _cli(bin):
    from subprocess import run, Popen
    return CLI(bin, run, Popen)

gztool = _cli('/home/customer/projects/gztool/gztool')


# +
def line_count(path):
    """count lines using binary search
    """
    # log.info('line count: %s', path)
    lo = 0
    hi = 1024
    while True:
        # log.info('finding upper bound: %d to %d', lo, hi)
        line = gztool.run(str(path), '-L', hi, '-R', 1).stdout
        if not line:
            break
        lo = hi
        hi *= 2
    log.info('%s/%s <= %d', path.parent.name, path.name, hi)
    while hi > lo + 1:
        mid = (hi + lo) // 2
        line = gztool.run(str(path), '-L', mid, '-R', 1).stdout
        # log.info('narrowing: %d (%d to %d) %s', mid, lo, hi, not not line)
        if line:
            lo = mid
        else:
            hi = mid
    log.info('%s/%s = %d', path.parent.name, path.name, lo)
    return lo


# count lines on all slogfiles in parallel
slogdf['lines'] = db.from_sequence(slogdf.path).map(line_count).to_dataframe().compute().reset_index(drop=True)
# -

df = slogdf[['name', 'st_size', 'lines']].copy()
df['b64'] = df.st_size / 64
df.drop('st_size', axis=1, inplace=True)
df.set_index('name')[::5].plot.barh(title='slogfile sizes (sample)',
    figsize=(10, 8));

slogdf.drop(['path'], axis=1).to_sql('slogfile', db4, if_exists='replace')

# ## Runs, Blocks, and Deliveries
#
# > split each slogfile into runs (each beginning with an import-kernel event)

# +
import json
from json import JSONDecodeError


def partition_lines(counts, step=100000):
    lo = pd.DataFrame.from_records([
        dict(ix=ix, start=lo, qty=min(lines - lo, step), lines=lines)
        for (ix, lines) in zip(counts.index, counts.values)
        for lo in range(0, lines, step)])
    return lo


def extract_lines(p, include=['import-kernel-finish',
                              'cosmic-swingset-end-block-start',
                              'cosmic-swingset-end-block-finish'],
                  exclude=[], slogdf=slogdf):
    s, lo, r, tot = p
    records = []
    error = {'time': -1, 'type': 'error'}
    with gztool.pipe(slogdf.path[s], '-L', lo, '-R', r) as lines:
        for lnum, txt in enumerate(lines):
            loads = json.loads
            try:
                record = loads(txt)
            except (JSONDecodeError, UnicodeDecodeError):
                record = error
            ty = record['type']
            if ty in exclude:
                continue
            if include and ty not in include:
                continue
            records.append(dict(record, slogfile=s, line=lnum))
    if not records:
        records = [dict(time=-1, type='not-found', slogfile=s, line=lo,
                        blockHeight=np.nan, blockTime=np.nan)]
    df = pd.DataFrame.from_records(records)
    return df[sorted(df.columns)]


meta = extract_lines((10, 0, 5000, 2000))
meta.head() #.groupby('type')[['line']].count()


# -

def show_times(df, cols):
    out = df.copy()
    for col in cols:
        out[col] = pd.to_datetime(out[col], unit='s')
    return out


# +
block4 = dd.from_delayed(
    [dask.delayed(extract_lines)(p)
     for p in partition_lines(slogdf.lines).values],
    meta=meta, verify_meta=False)

show_times(block4.head(), ['time', 'blockTime'])
# -

block4 = block4[block4.type != 'not-found']
block4.to_sql('block', 'sqlite:///slog4.db', index=False, if_exists='replace',
              parallel=True, method='multi')

# ## Analysis

# +
df = pd.read_sql("""
select slogfile, count(*) runs, s.name, s.st_size, s.lines
from block b
join slogfile s on s."index" = b.slogfile
where type = 'import-kernel-finish'
group by slogfile
order by 2
""", db4)

df.set_index('name')[['runs']][::5].plot.barh(
    log=True,
    title='slogfile runs (sample)',
    figsize=(10, 8));

# +
runs = pd.read_sql("""
select slogfile, line, s.lines eof, time
from block b
join slogfile s on b.slogfile = s."index"
where type = 'import-kernel-finish'
order by slogfile, line
""", db4)

# Compute end times
runs = pd.concat([
    runs.drop('time', axis=1),
    runs.groupby('slogfile').apply(lambda g: pd.DataFrame(dict(start=g.time, end=g.time.shift(-1)))),
    runs.groupby('slogfile').apply(lambda g: pd.DataFrame(dict(line_end=g.line.shift(-1))))
], axis=1)

runs.line_end = np.where(runs.line_end.isnull(), runs.eof, runs.line_end)
runs = runs.drop('eof', axis=1)
runs.to_sql('run', db4, if_exists='replace', index=True, index_label='run')
runs
# -

# What is the range of blocks in `agorictest-16`?

gen16 = show_times(pd.DataFrame(dict(blockHeight=64628, blockTime=[1625166000], ts=1625166000)), ['blockTime'])
gen16

pd.read_sql("""
select lo, n, lo + n - 1,  hi from (
select min(blockHeight) lo, max(blockHeight) hi, count(distinct blockHeight) n
from block
where type = 'cosmic-swingset-end-block-start'
and blockTime >= 1625166000
)
""", db4)

# How many validators logged each block?

df = pd.read_sql("""
select blockHeight, count(distinct slogfile) qty
from block
where type = 'cosmic-swingset-end-block-start'
and blockTime >= 1625166000
group by blockHeight
""", db4)
df.head()

df.set_index('blockHeight').plot(title='agorictest-16 validator coverage by block', figsize=(9, 6));

# #### Block Processing Time

df = pd.read_sql("""
with b as (
  select *
  from block
  where blockTime >= 1625166000
)
select slogfile
     , (select r.run
        from run r
        where r.slogfile = b.slogfile and r.line <= b.line and b.line < r.line_end) run
     , b.line, b.time
     , case when type = 'cosmic-swingset-end-block-start' then -1 else 1 end sign
     , blockHeight, blockTime
from b
-- limit 10
""", db4)
df.tail()

x = df.groupby('blockHeight')[['run']].count()
x.plot()

x = df.groupby('run')[['blockHeight']].aggregate(['min', 'max'])
x.plot(title='agorictest-16 slog run ranges', figsize=(9, 5), ylabel='blockHeight');

x = df.groupby('run')[['blockTime']].aggregate(['min', 'max'])
show_times(x['blockTime'], ['min', 'max']).plot(
    title='agorictest-16 slog run intervals', figsize=(9, 5),
);

lo = df[df.sign == -1]
hi = df.shift(-1)
hi = hi[hi.sign == 1]
dur = hi.time - lo.time
# show_times(df, ['time', 'time_end'])
lo['dur'] = dur
lo['s_hi'] = hi.slogfile
lo['l_hi'] = hi.line
lo['t_hi'] = hi.time
dur = lo[lo.slogfile == lo.s_hi]
show_times(dur, ['time', 'blockTime'])

show_times(
    dur.sort_values('dur').dropna().tail(),
    ['time', 'blockTime', 't_hi']
)

dur[dur.dur.abs() <= 120].plot.scatter(x='blockHeight', y='dur')

hi = pd.read_sql("""
select distinct blockHeight, time from block
where type = 'cosmic-swingset-end-block-end' and blockTime >= 1625166000
""", db4)

lo.set_index('blockHeight').join(hi.set_index('blockHeight'))

# ## @@ Older approaches

# +
import json
import itertools

# {"time":1625059432.2093444,"type":"cosmic-swingset-end-block-start","blockHeight":58394,"blockTime":1625059394}
# {"time":1625059432.2096362,"type":"cosmic-swingset-end-block-finish","blockHeight":58394,"blockTime":1625059394}


def iter_blocks(run, types=['deliver', 'deliver-result', 'syscall', 'syscall-result']):
    block = None
    records = []
    for kernel_start, record in run:
        ty = record['type']
        if ty == 'cosmic-swingset-end-block-start':
            block_start = record
        elif ty == 'cosmic-swingset-end-block-finish':
            block_finish = record
            yield kernel_start, block_start, block_finish, records
            block_start = block_finish = None
            records = []
        elif ty not in types:
            continue
        else:
            records.append(record)


def iter_run(slogfile_ix, start_line=0):
    kernel_start = None
    with gztool.pipe(slogdf.path[slogfile_ix], '-L', start_line) as lines:
        for lnum, txt in enumerate(lines):
            loads = json.loads
            try:
                record = loads(txt)
            except JSONDecodeError:
                record = {'time': -1, 'type': 'error'}
            record = dict(record, slogfile=slogfile_ix, line=lnum)
            ty = record['type']
            if kernel_start:
                # end of run?
                if ty == 'import-kernel-finish':
                    # we'd like to return the line number info
                    # can we return stuff from iterators?
                    return record
                else:
                    yield kernel_start, record
            else:
                if ty == 'import-kernel-finish':
                    kernel_start = record


def block_cranks(kernel_start, block_start, block_end, records):
    deliveries = []
    syscalls = 0
    deliver = None
    slogfile = kernel_start['slogfile']
    blockHeight = block_start['blockHeight']
    for record in records:
        ty = record['type']
        if ty == 'deliver':
            deliver = record
            syscalls = 0
        elif ty == 'syscall-result':
            syscalls += 1
        elif ty == 'deliver-result':
            dur = record['time'] - deliver['time']
            method = deliver['kd'][2]['method'] if deliver['kd'][0] == 'message' else None
            compute = record['dr'][2]['compute'] if type(record['dr'][2]) is type({}) else None            
            detail = dict(record,
                          syscalls=syscalls,
                          kd=deliver['kd'],
                          method=method,
                          compute=compute,
                          blockHeight=blockHeight,
                          slogfile=slogfile,
                          dur=dur)
            deliveries.append(detail)
    return pd.DataFrame.from_records(deliveries)


# for block in iter_blocks(iter_run(10)):
#     work = client.submit(lambda x: x, block)

# x = next(iter_blocks(iter_run(10), types=[]))

# block_cranks(*x).set_index(['slogfile', 'line'], drop=True).drop(['type', 'dr', 'kd'], axis=1)
1
# .DataFrame(lambda _k, bs, be, _r: bs, iter_blocks(iter_run(10), types=[]))
# -

client.map(block_cranks, iter_blocks(iter_run(10)))

slogdf[10:11]

# ### Runs
#
# > split each slogfile into runs (each beginning with an import-kernel event)

slog_sample = slogdf[70:120:5]
slog_sample.drop('path', axis=1)

# +
import json
from json import JSONDecodeError


def log_items(slog_row):
    ix, detail = slog_row
    line = [0]  # sigh... python3 _still_ doesn't have normal static scoping?
    loads = json.loads
    def load_numbered(txt):
        try:
            record = loads(txt)
        except JSONDecodeError:
            record = {'time': -1, 'type': 'error'}
        line[0] = line[0] + 1
        return dict(record, line=line[0])
    data = db.read_text(detail.path).map(load_numbered)
    return data.map(lambda item: dict(item, slogfile=ix))


events4 = db.from_sequence(list(slog_sample.iterrows())).map(log_items).flatten().to_dataframe()

# +
runs = events4[events4.type == 'import-kernel-finish'].compute().reset_index()

# Compute end times / lines
runs = pd.concat([
    runs.drop('time', axis=1),
    runs.groupby('slogfile').apply(lambda g: pd.DataFrame(dict(start=g.time, end=g.time.shift(-1)))),
    runs.groupby('slogfile').apply(lambda g: pd.DataFrame(dict(end_line=g.line.shift(-1))))
], axis=1)

def show_times(df, cols):
    out = df.copy()
    for col in cols:
        out[col] = pd.to_datetime(out[col], unit='s')
    return out


show_times(runs, ['start', 'end'])

# +
import gzip
import json
import itertools


def iter_types(path, types, meta=''):
    log.info('%s/%s%s: extracting %s', path.parent.name, path.name, meta, types)
    with gzip.open(path) as f:
        for (ix, line) in enumerate(f):
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                log.warning('%s:%d: bad JSON: %s', path.name, ix, repr(line))
                return
            ty = data['type']
            if ty in types:
                yield ix, data


def slog_runs(slogdf=slogdf):
    for ix, slog in slogdf.iterrows():
        for line, kf in iter_types(slog.path, ['import-kernel-finish'],
                                   f' [{round(slog.st_size / 1024 / 1024, 2)}Gb]'):
            yield dict(slogfile=ix, parent=slog.parent, name=slog['name'], line=line, **kf)

runs = pd.DataFrame.from_records(slog_runs(slogdf))

# Compute end times
runs = pd.concat([
    runs.drop('time', axis=1),
    runs.groupby('slogfile').apply(lambda g: pd.DataFrame(dict(start=g.time, end=g.time.shift(-1))))
], axis=1)

runs.to_sql('run', db4, if_exists='replace')

def show_times(df, cols):
    out = df.copy()
    for col in cols:
        out[col] = pd.to_datetime(out[col], unit='s')
    return out

runs.head()
# -

show_times(runs, ['start', 'end'])

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

# ## Vats

vats = c500[c500.type == 'create-vat'][['vatID', 'description']]
vats.vatID = vats.vatID.apply(lambda v: int(v[1:]))
vats = vats.drop_duplicates()
vats = vats.set_index('vatID', drop=True).sort_index()
vats

# ## Block Time Distribution

# +
blocks = c500[c500['type'] == 'cosmic-swingset-end-block-finish']

ax = blocks[['dur']].plot(kind='hist',
    bins=30,
    figsize=(9, 5), log=True,
    title=f'Block Time Distribution ({len(blocks)} blocks across 4 slogfiles)')
ax.set_xlabel('endblock duration (sec)');
# -

# ### chain_id: agorictest-15 vs. agorictest-16
#
# agorictest-16 genesis was 2021-07-01 19:00:00

gen16 = show_times(pd.DataFrame(dict(blockHeight=64628, blockTime=[1625166000], ts=1625166000)), ['blockTime'])
gen16


# It's visually obvious which chain a block is on:

# +
def block_plot(df, **kw):
    return show_times(df, ['blockTime']).plot.scatter(x='blockTime', y='blockHeight', rot=45, **kw)    

block_plot(blocks);


# -

# Let's take the average of these lines:

# +
def ez_line(df, x, y, ix):
    x_min = df[x].min()
    x_max = df[x].max()
    y_min = df[y].min()
    y_max = df[y].max()
    [x1, x0] = np.polyfit([x_min, x_max], [y_min, y_max], 1)
    return pd.DataFrame(dict(x1=[x1], x0=[x0]), index=[ix])


chain_lines = pd.concat([
    ez_line(blocks[blocks.blockTime < gen16.ts[0]], 'blockTime', 'blockHeight', 15),
    ez_line(blocks[blocks.blockTime >= gen16.ts[0]], 'blockTime', 'blockHeight', 16)
])
chain_lines.mean()


# +
def block_plot(df, **kw):
    return df.plot.scatter(x='blockTime', y='blockHeight', rot=45, **kw)    

ax = block_plot(blocks, title='chain 16 vs 15');

def add_line(x, ax, cs, figsize=(9, 6)):
    f = np.poly1d(cs)
    line = pd.DataFrame({'x': [x.min(), x.max()]})
    line['y'] = f(line['x'])
    return line.plot(x='x', y='y', color='Red', legend=False, ax=ax)

add_line(blocks.blockTime, ax, chain_lines.mean()).set_xlabel('blockTime');
# -

# Now we can assign `chain_id` based on whether we're above or below the dividing line:

split = np.poly1d(chain_lines.mean())(c500.blockTime)
c500['chain_id'] = np.where(c500.blockHeight.isnull(), np.nan, np.where(c500.blockHeight < split, 16, 15))
c500[c500.type == 'deliver-result'][['blockTime', 'blockHeight', 'chain_id', 'vatID', 'deliveryNum']]


# Let's check that within a chain, `blockHeight` uniquely determines `blockTime`:

# +
def group_count(df, by, col):
    stats = df.groupby(by)[[col]].agg('nunique')
    stats = stats.sort_values(col, ascending=False)
    return stats

block_dup = group_count(c500, ['chain_id', 'blockHeight'], 'blockTime')
block_dup[block_dup.blockTime > 1]
# -

# And likewise, that `crankNum` determines `blockHeight`:

block_dup = group_count(c500[(c500.type == 'deliver-result')], ['chain_id', 'crankNum'], 'blockHeight')
block_dup[block_dup.blockHeight > 1]

# +
x = show_times(
    c500[c500.crankNum == 47111].sort_values('blockHeight').drop(['description', 'managerType', 'time_start'], axis=1),
    ['time', 'time_kernel'])

x.to_csv('crank_47111_dup.csv')
x
# -

# @@ so focus on `deliveryNum` (esp not vatTP)

# ### Runs
#
# > split each slogfile into runs (each beginning with an import-kernel event)

# +
runs = c500[['slogfile', 'time_kernel', 'chain_id']].dropna().drop_duplicates().reset_index(drop=True)

show_times(runs, ['time_kernel'])
# -

# ## global crankNum -> vatID, deliveryNum

cranks = c500[c500['type'] == 'deliver-result']
cranks = cranks[['chain_id', 'crankNum', 'vatID', 'deliveryNum']].set_index(['chain_id', 'crankNum']).drop_duplicates().sort_index()
cranks # .sort_values('deliveryNum')

# ### Did we ever do more than 1000 cranks in a block?
#
# if not, current policy never fired

cranks.reset_index().groupby('blockHeight')[['crankNum']].count().sort_values('crankNum', ascending=False)

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
