.echo on
create index delrun3_line on delrun3 (file_id, line);
create index blockrun16_line on blockrun16 (file_id, line);

drop table if exists delrun3b;
create table delrun3b as
select dr.*, b.run, b.blockHeight, b.blockTime, b.line line_blk
  from delrun3 dr
  join blockrun16dur b
    on dr.file_id = b.file_id
   and dr.line between b.line and b.l_hi
;

select count(*) from delrun3;
select count(*) from delrun3b;

drop view if exists compute_mismatch;
create view compute_mismatch as
with dups as (
  select dr.vatID, dr.deliveryNum, count(distinct dr.compute)
  from delrun3b dr
  group by dr.vatID, dr.deliveryNum
  having count(distinct dr.compute) > 1
),
breakdown as (
select dr.vatID, dr.deliveryNum, dr.compute, count(*) freq
from delrun3b dr
join dups
   on dr.vatID = dups.vatID
  and dr.deliveryNum = dups.deliveryNum
  and dr.compute = compute
  group by dr.vatID, dr.deliveryNum, dr.compute
),
outlier as (
  select b.vatID, b.deliveryNum, min(freq) lo, max(freq) hi, sum(freq) tot
  from breakdown b
  group by b.vatID, b.deliveryNum
),
norm as (
  select b.vatID, b.deliveryNum, b.compute, o.hi, o.tot
  from breakdown b
  join outlier o
    on o.vatID = b.vatID
   and o.deliveryNum = b.deliveryNum
   and o.hi = b.freq
),
rare as (
  select b.vatID, b.deliveryNum, b.compute, o.lo
  from breakdown b
  join outlier o
    on o.vatID = b.vatID
   and o.deliveryNum = b.deliveryNum
   and o.lo = b.freq
)
select fm.parent, fm.name, dr.line, dr.vatID, dr.deliveryNum, n.hi, n.compute, o.lo, o.compute, n.tot
     , dr.time
from delrun3 dr
join rare o
  on o.vatID = dr.vatID
 and o.deliveryNum = dr.deliveryNum
 and o.compute = dr.compute
join norm n
  on n.vatID = dr.vatID
 and n.deliveryNum = dr.deliveryNum
join file_meta fm
  on fm.file_id = dr.file_id
order by fm.st_size, dr.line
;

select 1 from compute_mismatch limit 1;

-- has_dup as (
-- select dr.file_id, dr.line, dr.vatID, dr.deliveryNum, dr.compute
-- from delrun3 dr
-- join dups
--    on dr.vatID = dups.vatID
--   and dr.deliveryNum = dups.deliveryNum
--   and dr.compute = compute
-- )
-- ;

-- select file_id, line, vatID, deliveryNum, compute
