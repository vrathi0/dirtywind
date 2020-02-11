
/*
Create clean trajectory table

This query creates a point-trajectory table converting planar coordinates to geom 
objects in PostGIS, and also removing trajectory points after the predicted wind
plumes touch the ground (height = 0). The removal doesn't drop the trajectory but
only remove the points after the plume reaches the surface.

A new table is create in a processing schema: hysplit_process. 

*/

create schema if not exists hysplit_process;
drop table hysplit_process.clean_trajectories;
create table if not exists hysplit_process.clean_trajectories as (
    with geom_build as (
        select name_source,
               id_source,
               run,
               hour_along,
               height,
               traj_dt,
               traj_dt_i                                as init_dt,
               st_setsrid(st_makepoint(lon, lat), 4326) as geom
        from hysplit_partitions.trajectories_master
    ), filter_trajs_zeros as (
        select *
        from (
                 select *,
                        count(*)
                        filter (where height = 0)
                            over (partition by run, date_part('year', init_dt), id_source  order by traj_dt) as ct0
                 from geom_build
             ) sub
        where ct0 = 0
    ) select * from filter_trajs_zeros
);

 create index traj_geom_point_idx on hysplit_process.clean_trajectories using gist (geom);
 CREATE index traj_run_point_idx ON hysplit_process.clean_trajectories USING btree(run, hour_along); 

