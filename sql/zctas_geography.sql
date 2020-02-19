/*
ZCTAS Data Transformation
*/

-- 1. Continental ZCTAs only

CREATE SCHEMA IF NOT EXISTS us_geoms_raw_data;
DROP TABLE IF EXISTS us_geoms_raw_data.zctas_continental;

create table us_geoms_raw_data.zctas_continental as (
        select z.zcta5ce00         as zcta_id,
               s.statefp,
               st_transform(z.geom, 4326) as geom,
               st
        from us_geoms_raw_data.tl_2010_us_zcta500 as z
                 left join us_geoms_raw_data.tl_2017_us_state as s
                           on (st_within(z.geom, s.geom))
        where statefp not in ('02', '60', '66', '69', '72', '78')
);

create index zctas_continental_geom_idx on us_geoms_raw_data.zctas_continental
using gist(geom);



-- 2. ZCTAs Centroids: only include those whose centroids are inside the geometry!

DROP TABLE IF EXISTS us_geoms_raw_data.zctas_continental_centroids;
create table us_geoms_raw_data.zctas_continental_centroids as (
    with zctas_2000_filtered as (
        select z.zcta5ce00         as zcta_id,
               s.statefp,
               z.geom              as geom,
               st_centroid(z.geom) as geom_centroid
        from us_geoms_raw_data.tl_2010_us_zcta500 as z
                 left join us_geoms_raw_data.tl_2017_us_state as s
                           on (st_within(z.geom, s.geom))
        where statefp not in ('02', '60', '66', '69', '72', '78')
    ),
         zctas_2000_centroids as (
             select zcta_id,
                    statefp,
                    st_within(geom_centroid, geom) as centroid_in_polygon,
                    case
                        when (st_within(geom_centroid, geom))
                            then st_transform(geom_centroid, 4326)
                        else st_transform(st_pointonsurface(geom), 4326)
                        end  as final_centroid
             from zctas_2000_filtered
         )
    select zcta_id,
           statefp,
           geography(final_centroid) AS final_centroid
    from zctas_2000_centroids
);

CREATE index zctas_continental_geom_idx ON us_geoms_raw_data.zctas_continental_centroids
USING gist(final_centroid)

