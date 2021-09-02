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

# ## All slogfile entries have time, type

# Starting with `gs://slogfile-upload-5/2021-08-27T12:00:56.385Z-mirxl#0530.slog.gz`,
# the largest slogfile that doesn't exceed the 4GB bigquery load limit.
#
# | TaskBoardID | Discord ID | Verified | Moniker | Last Date Updated | filename | size |
# | --- | --- | --- | --- | --- | --- | --- |
# | 4584 | mirxl#0530 | Accepted | mirxl | 2021-08-27 10:18:00 | 2021-08-27T12:00:56.385Z-mirxl#0530.slog.gz | 3925214742 |

# +
# %%bigquery

select count(*) slog_lines from slog45.entry3
# -

# ## `entry_t` - all slogfile entries have time, type

# +
# %%bigquery

create or replace view slog45.entry_t as
select timestamp_micros(cast(time * 1000000 as int64)) ts
     , time, type
     , json_query(line, '$') record
from (
select cast(json_value(line, "$.time") as float64) time
     , json_value(line, "$.type") type
     , line
 from slog45.entry3
)
;

select * from slog45.entry_t limit 5;
# -

# ## Breakdown by type

# +
# %%bigquery

select type, count(*) qty
from slog45.entry_t
group by type
order by qty desc
# -

# ## SwingSet Kernel Runs

# +
# %%bigquery

drop table if exists slog45.kernel_run;

create table slog45.kernel_run as
with ks as (
    select rank() over (order by ts) as run_num
         , type, time, ts from slog45.entry_t where type = 'import-kernel-start'
),
last as (
    select max(ts) ts_hi from slog45.entry_t
)
select lo.run_num
     , lo.ts ts_lo, coalesce(hi.ts, last.ts_hi) ts_hi
from ks lo
left join ks hi on hi.run_num = lo.run_num + 1
cross join last;

select * from slog45.kernel_run order by run_num desc limit 10;

# +
# %%bigquery kernel_run

select * from (
select kr.*, unix_seconds(ts_hi) - unix_seconds(ts_lo) dur
from slog45.kernel_run kr
)
where dur > 2
order by run_num
# -

kernel_run.set_index('run_num', drop=True)

kernel_run.sort_values('dur', ascending=False).head(6)

# +
import matplotlib.pyplot as plt
import matplotlib.dates as dt

df = kernel_run.iloc[:12][['ts_lo', 'ts_hi']]

fig, ax = plt.subplots(figsize=(12, 4))
genesis17 = pd.to_datetime('2021-08-19 18:00:00')
ax.axvline(genesis17, color='r', linestyle='--', lw=2)
ax.xaxis.grid()
ax = ax.xaxis_date()
ax = plt.hlines(df.index, dt.date2num(df.ts_lo), dt.date2num(df.ts_hi))
plt.gca().invert_yaxis();
plt.title('Kernel Runs');
# -

# ## Normalization: Deliveries in Blocks in Kernel Runs

# Get details from kernel run, block, and delivery events:

# +
# %%bigquery r2bs

select ts, type
     , cast(json_value(record, '$.blockHeight') as int64) blockHeight
     , cast(json_value(record, '$.blockTime') as int64) blockTime
     , timestamp_micros(cast(json_value(record, '$.blockTime') as int64) * 1000000) blockTS
     , cast(json_value(record, '$.crankNum') as int64) crankNum
     , cast(json_value(record, '$.deliveryNum') as int64) deliveryNum
     , cast(substr(json_value(record, '$.vatID'), 2) as int64) vatID
     -- , json_value(record, '$.kd[1]') target
     , json_value(record, '$.kd[2].method') method
     -- , length(json_value(record, '$.kd[2].args.body')) body_length
     , cast(json_value(record, '$.dr[2].compute') as int64) compute
from slog45.entry_t
where type in (
    'import-kernel-start',
    'import-kernel-finish',
    'cosmic-swingset-end-block-start',
    'cosmic-swingset-begin-block',
    'cosmic-swingset-end-block-finish',
    'cosmic-swingset-bootstrap-block-start',
    'cosmic-swingset-bootstrap-block-finish',
    'start-replay',
    'finish-replay',
    'deliver',
    'deliver-result')

order by ts
-- limit 100000
;
# -

r2b = r2bs.assign(type=r2bs.type.astype('category'),
                  ts=r2bs.ts.dt.tz_convert(None))
r2b.groupby('type')[['ts']].count()

# Now pick out the starting time for each kernel run and block and "fill down".
#
# Use sign columns to indicate whether we're before or after other events.

# +
import numpy as np

r2b['run_ts_lo'] = np.where(r2b.type.isin(['import-kernel-start',
                                           'import-kernel-finish']), r2b.ts, np.datetime64('NaT'))
r2b['run_sign'] = np.where(r2b.type == 'import-kernel-start', -1,
                           np.where(r2b.type == 'import-kernel-finish', 1, np.nan))
r2b['blk_ts_lo'] = np.where(r2b.type.isin(['cosmic-swingset-begin-block',
                                           'cosmic-swingset-end-block-start',
                                           'cosmic-swingset-end-block-finish',
                                           'cosmic-swingset-bootstrap-block-start',
                                           'cosmic-swingset-bootstrap-block-finish']), r2b.ts, np.datetime64('NaT'))
r2b['blk_sign'] = np.where(r2b.type.isin(['cosmic-swingset-begin-block']), -1,
                           np.where(r2b.type.isin(['cosmic-swingset-end-block-start',
                                                   'cosmic-swingset-bootstrap-block-start']), 0,
                                    np.where(r2b.type.isin(['cosmic-swingset-end-block-finish',
                                                            'cosmic-swingset-bootstrap-block-finish']), 1, np.nan)))
for col in ['run_ts_lo', 'blk_ts_lo', 'blockHeight', 'blockTime', 'blockTS', 'run_sign', 'blk_sign']:
    r2b[col] = r2b[col].fillna(method='ffill')

r2bi = r2b.set_index(['run_ts_lo', 'blk_ts_lo', 'ts'])

r2bi.head()
# -

# ### Type Breakdown by Run

rty = r2b.groupby(['run_ts_lo', 'type'])[['ts']].count().reset_index().pivot(columns='type', index='run_ts_lo', values='ts')
rty[rty.deliver > 0]


# ## Vat compute by block
#
# These are consensus measurements, so we can drop duplicates:

# +
def compute_by_vat(df, x='blockTS'):
    df = df[[x, 'crankNum', 'vatID', 'deliveryNum', 'compute']].drop_duplicates()

    return df.groupby([x, 'vatID'])[['compute']].sum()

df = compute_by_vat(r2b)
df.head()


# +
def vat_compute_viz(df, scale=1000, x='blockTS'):
    df = df.assign(s=scale * df.compute / df.compute.max())
    return df.plot.scatter(x=x, y='vatID', s='s', figsize=(12, 6), alpha=0.3,
                title='Computrons per Block by Vat')

ax = vat_compute_viz(df.reset_index())
ax.axvline(genesis17, color='r', linestyle='--', lw=2)
# -

# ## Blocks in Kernel Runs
#
# Due to replay, blocks may be recorded in more than one kernel run.
#
# _Note partition by hour._

# +
# %%bigquery

drop table if exists slog45.block_dur;

# +
# %%bigquery

drop table if exists slog45.block_dur;

create table slog45.block_dur

PARTITION BY
  TIMESTAMP_TRUNC(ts_lo, hour)

as

with block_begin as (
    select cast(json_value(record, '$.blockHeight') as int64) blockHeight
         , cast(json_value(record, '$.blockTime') as int64) blockTime
         , timestamp_micros(cast(json_value(record, '$.blockTime') as int64) * 1000000) blockTS
         , e.ts, e.time, e.type
         , rank() over (order by e.time) bk_num
    from slog45.entry_t e
    where type = 'cosmic-swingset-begin-block'
)

select kr.run_num, lo.bk_num, lo.blockHeight, lo.blockTS, hi.blockTS blockTS_hi, hi.blockTime - lo.blockTime dur
     , lo.time time_lo, hi.time time_hi
     , lo.ts ts_lo, hi.ts ts_hi
from block_begin lo
join block_begin hi
  on hi.bk_num = lo.bk_num + 1
join slog45.kernel_run kr on kr.time_lo < lo.time and (kr.time_hi is null or kr.time_hi > lo.time)
;

select * from slog45.block_dur order by bk_num desc limit 15;

# +
# %%bigquery

-- check uniqueness of run, blockHeight

select run_num, blockHeight, count(*)
from slog45.block_dur
group by run_num, blockHeight
having count(*) > 1
# -

# ### Consensus Block Duration

# +
# %%bigquery bkdur

select distinct blockHeight, blockTS, dur
from slog45.block_dur
# -

bkdur.head()

bkdur[bkdur.dur >= 0].plot.scatter(x='blockTS', y='dur', alpha=0.3, figsize=(16, 8));

# ## Deliveries within Blocks and Kernel Runs

# ### Replay

# +
# %%bigquery

drop table if exists slog45.replay_range;

create table slog45.replay_range
as
with lo as (
    select ts, rank() over (order by time) as seq
         , cast(json_value(record, '$.deliveries') as int64) deliveries
    from slog45.entry_t
    where type = 'start-replay'
),
hi as (
    select ts, rank() over (order by time) as seq from slog45.entry_t
    where type = 'finish-replay'
)
select lo.seq, lo.ts ts_lo, hi.ts ts_hi, lo.deliveries
from lo
join hi on hi.seq = lo.seq and hi.ts > lo.ts
;

select * from slog45.replay_range rr order by seq limit 15;

# +
# %%bigquery

select count(*)
    from slog45.replay_range

# +
# %%bigquery

select seq, ts_lo, ts_hi, deliveries, cast(dur as string) dur_s from (
    select rr.*, ts_hi - ts_lo dur
    from slog45.replay_range rr where unix_seconds(ts_hi) - unix_seconds(ts_lo) > 10 order by seq limit 15
)


# +
# %%bigquery

with r1 as (select * from slog45.replay_range where seq = 178)
select e.*, r1.seq from slog45.entry_t e
join r1 on e.ts between r1.ts_lo and r1.ts_hi

# + active=""
# %%bigquery
#
# drop table if exists slog45.deliver_dur;
#
# create table slog45.deliver_dur
#
# PARTITION BY
#   TIMESTAMP_TRUNC(ts, hour)
#
# as
#
# with d as (
# select time, ts, type
#      , cast(json_value(record, '$.crankNum') as int64) crankNum
#      , cast(json_value(record, '$.deliveryNum') as int64) deliveryNum
#      , cast(substr(json_value(record, '$.vatID'), 2) as int64) vatID
#      , json_value(record, '$.kd[0]') tag
#      , json_query(record, '$.kd') kd
#      , json_query(record, '$.dr') dr
#      , rank() over (order by time) seq
#      , case when exists (
#          select 1 from slog45.replay_range rr
#          where e.ts between rr.ts_lo and rr.ts_hi
#        ) then 1 else 0 end as in_replay
# from slog45.entry_t e
# where type in ('deliver', 'deliver-result')
# ),
# lo as (
# select d.seq, d.time, d.ts, d.in_replay, d.crankNum, d.vatID, d.deliverynum, tag
#      , case when tag = 'message' then json_value(d.kd, '$[1]') end target
#      , case when tag = 'message' then json_value(d.kd, '$[2].method') end method
#      , case when tag = 'message' then length(json_value(d.kd, '$[2].args.body')) end body_length
#      -- , kd
# from d
# where d.type = 'deliver'
# ),
# hi as (
# select d.seq, d.time, d.ts, d.type, d.crankNum, d.vatID
#      , cast(json_value(dr, '$[2].compute') as int64) compute
#      , dr
# from d
# where d.type = 'deliver-result'
# )
# select -- bk.run_num, bk.blockHeight, bk.blockTS,
#        lo.*
#      , hi.compute, hi.time - lo.time dur
#      -- , bk.bk_num 
# from lo
# join hi on hi.crankNum = lo.crankNum and hi.seq = lo.seq + 1
# -- join slog45.block_dur bk on lo.time between bk.time_lo and bk.time_hi
# ;
#
# select * from slog45.deliver_dur order by seq limit 5;
# -



# %%bigquery 
select * from slog45.deliver_dur where in_replay = 1 order by seq limit 5;

# +
# %%bigquery

select * from slog45.deliver_dur order by seq limit 20;
# -

# ### Connect deliveries to blocks
#
# _Note use of partition on hour to avoid exceeding resource limits._

# # ## %%bigquery
#
# drop table if exists slog45.deliver_blk;
#
# create table slog45.deliver_blk as
# select run_num, bk_num, blockHeight, blockTS, crankNum, vatID, deliveryNum, in_replay, dd.ts
# from slog45.deliver_dur dd
# join slog45.block_dur bk on
#  timestamp_trunc(dd.ts, hour) = timestamp_trunc(bk.ts_lo, hour)
#  and dd.ts between bk.ts_lo and bk.ts_hi;
#
# select * from slog45.deliver_blk order by crankNum limit 5;

# +
# %%bigquery

-- check uniqueness of run, crankNum

select run_num, crankNum, count(*)
from slog45.deliver_blk
group by run_num, crankNum
having count(*) > 1

# +
# %%bigquery

drop table if exists slog45.deliver_x;

create table slog45.deliver_x as
with d2b as (
    select distinct blockHeight, blockTS, crankNum
    from slog45.deliver_blk
)
select distinct d2b.blockHeight, d2b.blockTS, dd.crankNum, vatID, deliveryNum, tag, target, method, body_length, compute
from slog45.deliver_dur dd
join d2b on d2b.crankNum = dd.crankNum;

select * from slog45.deliver_x limit 3;

# +
# %%bigquery

select crankNum, count(*)
from slog45.deliver_x
group by crankNum
having count(*) > 1
;
# -

# ## Vat compute by block (@@wrong - same crank occurs in multiple blocks)

# +
# %%bigquery

select * from slog45.deliver_x limit 3;

# +
# %%bigquery vat_compute

select blockHeight, blockTS, vatID, sum(compute) compute_tot
from slog45.deliver_x
group by blockHeight, blockTS, vatID;
# -

vat_compute.sort_values('compute_tot', ascending=False).head(10)

len(vat_compute)

df = vat_compute.assign(s=1000 * vat_compute.compute_tot / vat_compute.compute_tot.max())
df.plot.scatter(x='blockHeight', y='vatID', s='s', figsize=(12, 6), alpha=0.3,
                title='Computrons per Block by Vat');

df = vat_compute.assign(s=1000 * vat_compute.compute_tot / vat_compute.compute_tot.max())
df.plot.scatter(x='blockTS', y='vatID', s='s', figsize=(12, 6), alpha=0.3,
                title='Computrons per Block by Vat');

vat_compute[vat_compute.blockHeight.isin([8386, 47194])].sort_values('compute_tot', ascending=False).head(10)

# +
# %%bigquery

select *
from slog45.deliver_x
where blockHeight in (8386, 47194) -- , 
order by crankNum
limit 20

# +
# %%bigquery crank49

select *
from slog45.entry_t e
where e.type = 'deliver'
and json_value(e.record, '$.crankNum') = '49'
limit 2;
# -

crank49

# +
import json

r = json.loads(crank49.record[0])
b = json.loads(r['kd'][2]['args']['body'])
print('\n'.join(b[0]['source'].split('\n')[:9]))
