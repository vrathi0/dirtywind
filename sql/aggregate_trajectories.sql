/*
Aggregate daily/hour trajectories to 24-hr block trajectories

To manage the total ammount of predicted positions, we aggregate the 
daily/hour (each 6 hours) trajectories to their mean position values
in a 24-hr timespan. This will reduce the space predicted and help us 
to aggregate the different daily trajectories to only one trajectory 
per emission hour/plant. 

*/

CREATE SCHEMA IF NOT EXISTS hyspit_process;

-- 1. Take raw HySPLIT data and create a time-based division: aggregate trajectories to 24-hr buckets

create table if not exists hysplit_process.traj_runs_time_tiles as (
    select r.*
    from (
        select name_source,
               id_source,
               run,
               init_dt,
               ntile(3) over (partition by run, id_source, init_dt order by hour_along) as t,
               height,
               date_part('year', init_dt)                                                                   AS init_year,
               traj_dt,
               geom
            from hysplit_process.clean_trajectories
);

CREATE index traj_run_time_tiles_sort_idx 
ON hysplit_process.traj_runs_time_tiles (id_source, name_source, run, timezone('UTC', init_dt), t, traj_dt desc);


-- 2. Create a crosswaalk with model sites and 24-hr daate buckets

CREATE TABLE IF NOT EXISTS hysplit_process.traj_runs_dates_modeling AS(
             select distinct on (
                id_source,
                name_source,
                run,
                init_dt,
                t) name_source,
                   id_source,
                   run,
                   init_dt, 
                   init_year,
                   t,
                   traj_dt
             from hysplit_process.traj_runs_time_tiles
             order by id_source, name_source, run, init_dt, t, traj_dt desc
);

CREATE index traj_runs_dates_model_idx
ON hysplit_process.traj_runs_dates_modeling (id_source, name_source, run, t);

-- 3. Aggregaate positions and heights for all trajectories using the tables created above.

CREATE TABLE IF NOT EXISTS hysplit_process.clean_aggregate_trajs AS (
    with agg_trajs_by_t as (
             select name_source,
                    id_source,
                    run,
                    t,
                    init_dt,
                    avg(st_x(geom)) as avg_lon,
                    avg(st_y(geom)) as avg_lat,
                    avg(height)     as avg_height
             from hysplit_process.traj_runs_time_tiles
             group by name_source, id_source, run, init_dt, t
         )
    select t.name_source,
           t.id_source,
           t.run,
           t.t,
           t.init_dt,
           d.traj_dt,
           geography(st_setsrid(st_makepoint(t.avg_lon, t.avg_lat), 4326)) as geom,
           t.avg_height
    from agg_trajs_by_t as t
             left join hysplit_process.traj_runs_dates_modeling as d
                       using (run, t, id_source, name_source, init_dt)
);

create index clean_daily_hour_trajs_geom_idx on hysplit_process.clean_daily_hour_trajectories 
using gist(geom);


-- 4. Calculate distances from aggregates air parcel trajectories to all ZCTAs in a 10-mile radius. 

drop table hysplit_process.trajectories_zcta_overlap;
create table hysplit_process.trajectories_zcta_overlap as (
    select t.name_source,
           t.id_source,
           t.run,
           t.t,
           t.init_dt,
           date(t.init_dt) AS init_dt_date,
           t.traj_dt,
           DATE(t.traj_dt) AS traj_dt_date,
           t.avg_height,
           t.geom,
           z.zcta_id,
           z.statefp,
           st_distance(geography(t.geom), geography(z.final_centroid)) as distance_nearest
    from hysplit_process.clean_aggregate_trajs as t
             cross join lateral (
        select zcta_id,
               statefp,
               final_centroid
        from us_geoms_raw_data.zctas_continental_centroids
        where st_dwithin(geography(t.geom), geography(final_centroid), 160934) -- 10 miles
        order by t.geom <-> final_centroid
        ) as z
);


CREATE index trajectories_zcta_overlap_merge_idx ON hysplit_process.trajectories_zcta_overlap 
(id_source, init_dt_date);

