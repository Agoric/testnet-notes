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
    meter = 0
    block_in = None
    block_out = None
    for crankNum, d in df.iterrows():
        if block_in is None:
            block_in = block_out = d.blockHeight
            t_in = t_out = d.blockTime
        if d.blockHeight > block_in:
            block_in = d.blockHeight
            if block_in > block_out:
                block_out = block_in
                meter = 0
        yield block_out  # do the work
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

# ## Zoom in on the X axis

# +
_zoom = df.loc[110000:112000]
_zoom = _zoom.reset_index().set_index(['blockHeight', 'crankNum'])

_zoom[['computeInBlock', 'computeInSimBlk']].plot(figsize=(12, 4), rot=-75);
# -

# ## Elapsed time* on the X axis
#
# *estimated as cumulative crank duration

_zoom = df.loc[110000:112000].copy()
# _zoom = df
_zoom['t'] = _zoom.dur.cumsum()
_zoom.set_index('t')[['computeInBlock', 'computeInSimBlk']].plot(figsize=(12, 4), rot=-75);

# ### Detailed Data

_zoom.groupby('bkSim').apply(lambda g: g.head(10))[:50][[
    'dur', 't', 'durationInSimBlk', 'durationInBlock',
    'compute', 'computeInSimBlk', 'computeInBlock',
    'blockHeight'
]]
