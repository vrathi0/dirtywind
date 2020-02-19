library(splitr)
library(dplyr)
library(RPostgres)
library(DBI)
library(purrr)
library(furrr)
library(future)
devtools::load_all()


# Load elements to database
con <- dbConnect(drv=RPostgres::Postgres(),
                 user=Sys.getenv('USER'),
                 password=Sys.getenv('PASSWORD'),
                 host=Sys.getenv('HOST',),
                 port=Sys.getenv('PORT'),
                 dbname=Sys.getenv('DBNAME'))


# Alternative to connection object
# cred <- list(drv=RPostgres::Postgres(),
#               user=Sys.getenv('USER'),
#               password=Sys.getenv('PASSWORD'),
#               host=Sys.getenv('HOST',),
#               port=Sys.getenv('PORT'),
#               dbname=Sys.getenv('DBNAME'),
#               options=glue::glue("-c search_path=hysplit"),
#               maxSize=30
# )


cred <- list(drv=RPostgres::Postgres(),
             user=Sys.getenv('USER'),
             password=Sys.getenv('PASSWORD'),
             host=Sys.getenv('HOST',),
             port=Sys.getenv('PORT'),
             dbname=Sys.getenv('DBNAME'),
             maxSize=30
)

# dirtywind::load_plant_data(conn = con,
#                            schema = 'hysplit',
#                            table_name = 'coal_plants',
#                            save_local = TRUE,
#                            overwrite = FALSE)

# Build parameter data.frame to run HYSPLIT
query <-  "
    select distinct on (facility_id, latitude, longitude, facility_name) facility_id,
                                                                         latitude,
                                                                         longitude,
                                                                         facility_name,
                                                                         stack_height
    from hysplit.coal_plants
    where year = 2005;
"

query_df <- dbGetQuery(con, query)

paramemter_df <- model_inputs_unit(query = query,
                  con=con,
                  timedelta = '1 month',
                  start_date = as.Date('2005-01-01'),
                  end_date = as.Date('2005-12-31'),
                  duration = 72,
                  daily_hours = c(0, 6, 12, 18))

###############################################################################
################################### NOT RUN ###################################
###############################################################################

plants_2006 <- read.csv('data/coal_plant_inventory_all_years.csv') %>%
  dplyr::select( facility_id,
                 latitude,
                 longitude,
                 facility_name,
                 stack_height,
                 year) %>%
  filter(year == 2006) %>%
  group_by(facility_id,
           latitude,
           longitude,
           facility_name,
           stack_height) %>%
  distinct() %>%
  write.csv('data/plants_2006.csv',
            row.names = FALSE)

paramemter_df <- model_inputs_unit(timedelta = '1 month',
                                   start_date = as.Date('2006-01-01'),
                                   end_date = as.Date('2006-12-31'),
                                   duration = 72,
                                   daily_hours = c(0, 6, 12, 18),
                                   local_file = 'data/plants_2006.csv')

###############################################################################
###############################################################################


public_ids <- c(
                '52.35.6.124',
                '34.208.111.91',
                '34.209.41.9'
                )

cls <- make_cluster_ec2(public_ids)

plan(list(tweak(cluster, workers = cls), multisession))

creds_aws <- list(
             user=Sys.getenv('USER'),
             password=Sys.getenv('PASSWORD'),
             host='db.cicala-projects.com',
             port='5432',
             dbname=Sys.getenv('DBNAME'),
             maxSize=30
)


system.time(
  test_hysplit <-
  paramemter_df %>%
  mutate(model_traj = furrr::future_pmap(list(
                               'lat' = latitude,
                               'lon' = longitude,
                               'height' = stack_height,
                               'name_source' = facility_name,
                               'id_source' = facility_id,
                               'duration' = duration,
                               'days' = seq_dates,
                               'daily_hours' = daily_hours,
                               'direction' = 'forward',
                               'met_type' = 'reanalysis',
                               'met_dir' = '/home/ubuntu/met',
                               'exec_dir' = "/home/ubuntu/hysplit",
                               'clean_up' = FALSE,
                               'db' = TRUE,
                               'schema' = 'hysplit_partitions',
                               'table_name' = 'trajectories_master',
                               'cred'= list(creds_aws)
                              ),
                           dirtywind::hysplit_trajectory_parallel_master)
  )
)


parallel_hysplit(parameters_df=parameter_barry,
                 creds=creds,
                 met_type='reanalysis',
                 clean_up=TRUE,
                 public_ip=c("34.220.174.56", "34.219.10.249"),
                 ec2=FALSE)
