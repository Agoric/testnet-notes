# -*- coding: utf-8 -*-
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

# # Block Scheduling Policy Simulation
#
# context: [build model of computron\-to\-wallclock relationship · Issue \#3459 · Agoric/agoric\-sdk](https://github.com/Agoric/agoric-sdk/issues/3459)

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
from slogdata import mysql_socket, show_times

def _slog4db(database='slog4'):
    from sqlalchemy import create_engine
    return create_engine(mysql_socket(database, create_engine))

_db4 = _slog4db()
_db4.execute('show tables').fetchall()
# -

# ## Global compute results for agorictest-16
#
# Based on one validator, **Provalidator**.

show_times(pd.read_sql('''
select *
from slog_run
where parent = 'Provalidator'
limit 10
''', _db4), ['time_lo', 'time_hi', 'blockTime_lo', 'blockTime_hi']).iloc[0]


# +
# _db4.execute('''drop table if exists delivery_compute_16''');

# +
def build_consensus_compute(theValidator, db,
                            table='delivery_compute_16'):
    """We include duration from 1 validator as well.
    """
    log.info('creating %s', table)
    db.execute(f'''
    create table if not exists {table} as
    with slog1 as (
        select file_id
        from file_info
        where parent = %(theValidator)s
    )
    select blockHeight, blockTime, crankNum, vatID, deliveryNum, compute, dur
    from j_delivery r
    cross join slog1
    where r.file_id = slog1.file_id
    order by crankNum
    ''', dict(theValidator=theValidator))
    agg = pd.read_sql(f'select count(*) from {table}', db)
    log.info('done:\n%s', agg)
    return pd.read_sql(f'select * from {table} limit 5', db)

build_consensus_compute('Provalidator', _db4)
# -

_dc16 = pd.read_sql('select * from delivery_compute_16 order by crankNum', _db4, index_col='crankNum')
_dc16.tail()


# `crankNum` goes from 31 to 34; 32 and 33 are missing. Perhaps `create-vat` cranks?

# +
def simulate_run_policy(df, threshold=8e6):
    # does 10e6 help with bank update latency?
    # only a little: max ~50 rather than ~70
    meter = 0
    block_in = None
    block_out = None
    for crankNum, d in df.iterrows():
        if block_in is None:
            block_in = block_out = d.blockHeight
            t_in = t_out = d.blockTime
        yield block_out  # do the work
        if d.blockHeight > block_in:
            block_in = d.blockHeight
            if block_in > block_out:
                block_out = block_in
                meter = 0
        meter += d.compute
        if meter > threshold:
            meter = 0
            block_out += 1

df = _dc16[_dc16.blockHeight > _dc16.blockHeight.min()]
_sim16 = df.assign(bkSim=list(simulate_run_policy(df)))
_sim16


# +
def sim_aux_stats(df):
    df = _sim16
    original = df.groupby('blockHeight')
    df = original.apply(lambda g: g.assign(
        computeInBlock=g.compute.cumsum(),
        durationInBlock=g.dur.cumsum()))
    
    df = df.reset_index(level=0, drop=True)

    simulated = df.groupby('bkSim')
    df = simulated.apply(lambda g: g.assign(
        computeInSimBlk=g.compute.cumsum(),
        durationInSimBlk=g.dur.cumsum()))
    df = df.reset_index(level=0, drop=True)
    return df

df = sim_aux_stats(_sim16)
# -

df[['vatID', 'deliveryNum',
    'blockHeight',
    'compute', 'computeInBlock', 'computeInSimBlk']].describe()

df[['computeInBlock', 'computeInSimBlk']].plot(figsize=(12, 4));

df[['vatID', 'deliveryNum',
    'blockHeight',
    'dur', 'durationInBlock', 'durationInSimBlk']].describe()

df[['durationInBlock', 'durationInSimBlk']].plot(figsize=(12, 6));

# ## Latency

df['delay'] = df.bkSim - df.blockHeight
df[['delay']].describe()

df[['durationInBlock', 'durationInSimBlk', 'delay']].plot(figsize=(12, 4))

df.sort_values('delay', ascending=False).head(50)

# ## Zoom in on the X axis

103200 and 105500

# +
_zoom = df.loc[103200:105500]
_zoom = _zoom.reset_index().set_index(['blockHeight', 'crankNum'])

_zoom[['computeInBlock', 'computeInSimBlk']].plot(figsize=(12, 4), rot=-75);
# -

x = _zoom.reset_index()
g = x.groupby('bkSim')
x = pd.concat([
    g[['blockHeight']].min(),
    g[['compute']].sum(),
    g[['dur']].sum()
])
x = x.assign(delay=x.index - x.blockHeight)
x

x[['compute', 'dur', 'delay']].plot(subplots=True, figsize=(15, 9))

_zoom[['durationInBlock', 'durationInSimBlk']].plot(figsize=(12, 4), rot=-75);

(df.bkSim - df.blockHeight).describe()

(df.bkSim - df.blockHeight).hist(figsize=(10, 5), bins=72, log=True)

# ## Elapsed time* on the X axis
#
# *estimated as cumulative crank duration

_zoom = df.loc[103273:104400].copy()
# _zoom = df
_zoom['t'] = _zoom.dur.cumsum()
_zoom.set_index('t')[['durationInBlock', 'durationInSimBlk']].plot(figsize=(12, 4), rot=-75);

# ### Detailed Data

_zoom.groupby('bkSim').apply(lambda g: g.head(10))[50:100][[
    'dur', 't', 'durationInSimBlk', 'durationInBlock',
    'compute', 'computeInSimBlk', 'computeInBlock',
    'blockHeight', 'delay'
]]

df.loc[103200:105500][['delay']].plot()

x = pd.read_sql('''
select *
from j_delivery
where crankNum between 103200 and 105500
and file_id = 3288529541296525
''', _db4)
x

show_times(x[x.index.isin([x.index.min(), x.index.max()])])[['crankNum', 'blockHeight', 'blockTime']]

x.blockHeight.max() - x.blockHeight.min()

x.blockHeight.describe()

x[x.compute > 1000000].groupby('method')[['compute', 'dur']].aggregate(['count', 'median', 'mean'])

x = pd.read_sql('''
select *
from t_delivery
where method = 'fromBridge'
and blockHeight between 68817 and 69707
and file_id = 3288529541296525
''', _db4)
x

_db4.execute('''
create index if not exists slog_entry_bk_ix on slog_entry(blockHeight)
''');

_db4.execute('drop index if exists slog_entry_ty_ix on slog_entry');


# +
def bank_trace(db,
               limit=250,
               file_id=3288529541296525,
               bk_lo=68817,
               bk_hi=69707):
    df = pd.read_sql(
        '''
        with d as (
        select file_id, run_line_lo
         , line
         , blockHeight
         , blockTime
         , time
         , crankNum
         , cast(substr(json_unquote(json_extract(record, '$.vatID')), 2) as int) vatID
         , coalesce(cast(json_extract(record, '$.deliveryNum') as int), -1) deliveryNum
         , json_extract(record, '$.kd') kd
        from slog_entry e
        where blockHeight between %(bk_lo)s and %(bk_hi)s
        and file_id = %(file_id)s
        and type = 'deliver'
        limit %(limit)s
        ),
        detail as (
        select d.*
          , json_unquote(json_extract(d.kd, '$[0]')) tag
          , json_unquote(json_extract(d.kd, '$[1]')) target
          , json_unquote(json_extract(d.kd, '$[2].method')) method
          , json_length(json_unquote(json_extract(d.kd, '$[2].args.body')), '$[1].updated') updated
        from d
        )
        select blockHeight, blockTime, crankNum, vatID, deliveryNum
             , tag
             , case when tag = 'message' then target else null end target
             , method, updated
             , time
        -- validator-specific: file_id, run_line_lo, line, time
        from detail
        -- where method = 'fromBridge'
        order by blockHeight, crankNum
        ''', db, params=dict(limit=limit, file_id=file_id, bk_lo=bk_lo, bk_hi=bk_hi))
    return df


# x1 = bank_trace(_db4, bk_hi=68817 + 100)
# x2 = bank_trace(_db4, bk_lo=69707 - 100)
# x = pd.concat([x1, x2])
x = bank_trace(_db4, limit=100)
show_times(x)
# -

x.updated.describe()

show_times(x[~x.updated.isnull()]).plot.scatter(x='time', y='updated',
                                                figsize=(10, 4), alpha=0.45,
                                                title='Accounts Updated per delivery');

# +
import json

def notifer_traffic(df):
    kd = df.record.apply(lambda txt: json.loads(txt)['kd'])
    dt = kd.apply(lambda k: k[0])
    method = kd.apply(lambda k: k[2].get('method') if k[0] == 'message' else None)
    body = kd.apply(lambda k: k[2].get('args', {}).get('body') if k[0] == 'message' else None)
    body = body.apply(lambda v: json.loads(v) if v else None)
    updated = body.apply(lambda b: len(b[1].get('updated')) if b else None)
    # time = pd.as_datetime(df.time.dt.time)
    df = df.assign(dt=dt, method=method, body=body, updated=updated)
    dur = df.time.diff()
    return df.assign(dur=dur)

notifer_traffic(show_times(x2)).drop(columns=['file_id', 'run_line_lo', 'line', 'record', 'body'])
# -

show_times(x2)

x2.record[0]

len(x2.record[0])

# +
import json

r = json.loads(x2.record[0])
body = r['kd'][2]['args']['body']
x = json.loads(body)
# print(json.dumps(r, indent=2))
len(x[1]['updated'])
# -

x2.record[1]

x2.record[2]
