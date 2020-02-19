/*
Merge aggregated air parcel trajectories with AOD data
*/


create schema if not exists results;
drop table results.aod_kentucky;
create table if not exists results.aod_kentucky as (
with aod_2006 as (
    select site_corrected,
       date,
       time,
       timestamp,
       year,
       mean as mean_aod
    from data_extraction.zctas_2000_extraction_clean
    where year = 2006
), agg_day_aod as (
    select site_corrected,
           date,
           avg(mean_aod) as mean_aod_daily
    from aod_2006
    group by site_corrected, date
), traj_aod_merge as(
    select
    a.site_corrected,
    t.date_run,
    a.date,
    t.t as prediction_period_24h,
    date_run + t as pred_after_emission_date,
    t.distance_nearest,
    a.mean_aod_daily
    from hysplit.trajectories_zcta_overlap as t
    right join agg_day_aod as a
    on a.site_corrected = t.zcta_id
    and a.date = t.date_run
)
select *
from traj_aod_merge
);
