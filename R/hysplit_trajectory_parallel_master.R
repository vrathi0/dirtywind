#' Conduct HYSPLIT trajectory runs in parallel using futures
#'
#' The function executes single/multiple forward or backward HYSPLIT trajectory
#' runs using specified meteorological datasets.
#'
#' @param lat,lon,height The receptor position in terms of latitude and
#'   longitude (both in decimal degrees), and height in meters above ground
#'   level.
#' @param duration The duration of each model run (whether it is in the forward
#'   direction or running backwards) in hours.
#' @param days A vector of days that the model will run. This is combined with
#'   the `daily_hours` to produce a series of date-times.
#' @param daily_hours A vector of daily hours for initiations of runs across the
#'   given `days`. Use values from from `0` to `23`.
#' @param direction An option to select whether to conduct the model in the
#'   `"forward"` (default) or `"backward"` directions.
#' @param met_type The type of meteorological data files to use. The options
#'   are: `"reanalysis"` (NCAR/NCEP global reanalysis data, the default),
#'   `"gdas1"` and `"gdas0.5"` (Global Data Assimilation System 1-degree and
#'   0.5-degree resolution data), `"narr"` (North American Regional Reanalysis),
#'   `"gfs0.25"` (Global Forecast System 0.25 degree data), and `"nam12"` (North
#'   American Mesoscale Forecast System, 12-km/6-hour resolution data).
#' @param vert_motion A numbered option to select the method used to simulation
#'   vertical motion. The methods are: `0` (input model data), `1` (isobaric),
#'   `2` (isentropic), `3` (constant density), `4` (isosigma), `5` (from
#'   divergence), `6` (remap MSL to AGL), `7` (average data), and `8` (damped
#'   magnitude).
#' @param model_height The upper limit of the model domain in meters.
#' @param extended_met An option to report additional meteorological data along
#'   each output trajectory.
#' @param config A configuration list serves to internally generate the
#'   `SETUP.CFG` file. This list can be easily created by using the
#'   [set_config()] function. If `NULL`, then the default configuration list
#'   will be generated.
#' @param ascdata An ascdata list that will be used to create the `ASCDATA.CFG`
#'   file. This list can be provided through use of the [set_ascdata()]
#'   function. If `NULL`, then the default ascdata list will be generated.
#' @param traj_name An optional, descriptive name for the output file
#'   collection.
#' @param binary_path An optional path to a HYSPLIT trajectory model binary.
#'   When not specified, the model binary will be chosen from several available
#'   in the package (based on the user's platform).
#' @param met_dir An optional file path for storage and access of meteorological
#'   data files.
#' @param exec_dir An optional file path for the working directory of the model
#'   input and output files.
#' @param clean_up An option to make the `exec_dir` directory clean after
#'   completion of all trajectory runs. By default, this is set to `TRUE`.
#'   
#' @examples
#' \dontrun{
#' library(lubridate)
#' 
#' # Run a trajectory model 4 times a day
#' # for 6 days in 2012 using NCEP/NCAR
#' # reanalysis data
#' trajectory <-
#'   hysplit_trajectory(
#'     lat = 50.108,
#'     lon = -122.942,
#'     height = 100,
#'     duration = 48,
#'     days = seq(
#'       lubridate::ymd("2012-02-22"),
#'       lubridate::ymd("2012-02-27"),
#'       by = "1 day"
#'     ),
#'     daily_hours = c(0, 6, 12, 18)
#'   )
#' }
#' 
#' @export hysplit_trajectory_parallel_master 
hysplit_trajectory_parallel_master <- function(lat = 49.263,
                                               lon = -123.250,
                                               height = 50,
                                               duration = 24,
                                               days = NULL,
                                               daily_hours = 0,
                                               direction = "forward",
                                               met_type = "reanalysis",
                                               vert_motion = 0,
                                               model_height = 20000,
                                               extended_met = FALSE,
                                               config = NULL,
                                               ascdata = NULL,
                                               traj_name = NULL,
                                               binary_path = NULL,
                                               met_dir = NULL,
                                               exec_dir = NULL,
                                               clean_up = TRUE,
                                               name_source = NULL,
                                               id_source = NULL,
                                               db = TRUE,
                                               cred = NULL,
                                               table_name = 'trajectories_hysplit',
                                               schema = 'hysplit') {
  
  # If the execution dir isn't specified, use the working directory
  if (is.null(exec_dir)) exec_dir <- getwd()
  
  # If the meteorology dir isn't specified, use the working directory
  if (is.null(met_dir)) met_dir <- getwd()
  
  # Set the path for the `hyts_std` binary file
  binary_path <- 
    set_binary_path(
      binary_path = binary_path,
      binary_name = "hyts_std"
    )

  # Get the system type
  system_type <- get_os()
  
  # Generate name of output folder
  if (is.null(traj_name)) {
    folder_name <- paste0("traj-", 
                          as.character(Sys.getpid()), "-",
                          sample(1:1e10, 1), "-",
                          format(Sys.time(), "%Y-%m-%d-%H-%M-%S"))
  } else if (!is.null(traj_name)) {
    folder_name <- traj_name
  }
  
  if (is.null(config)) {
    config_list <- set_config()
  } else {
    config_list <- config
  }
  
  if (is.null(ascdata)) {
    ascdata_list <- set_ascdata()
  } else {
    ascdata_list <- ascdata
  }
  
  # Modify the default `SETUP.CFG` file when the option for extended
  # meteorology is `TRUE`
  if (isTRUE(extended_met)) {
    
    tm_names <-
      config_list %>%
      names() %>%
      vapply(
        FUN.VALUE = logical(1),
        USE.NAMES = FALSE,
        FUN = function(y) y %>% tidy_grepl("^tm_")
      ) %>%
      which()
    
    config_list[tm_names] <- 1
  }
  
 
  # Stop function if there are vectors of different
  # length for `lat` and `lon`
  if (length(lat) != length(lon)) {
    stop("The coordinate vectors are not the same length.", call. = FALSE)
  }
  
  # Download any necessary meteorological data files
  # and return a vector of the all files required
  met_files <- 
    download_met_files(
      met_type = met_type,
      days = days,
      duration = duration,
      direction = direction,
      met_dir = met_dir
    )
  
  # Generate a tibble of receptor sites
  receptors_tbl <- 
    dplyr::tibble(lat = lat, 
                  lon = lon,
                  id_source = id_source,
                  name_source = name_source) %>%
    dplyr::group_by(lat, lon, id_source, name_source) %>% 
    tidyr::expand(height = height) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(receptor = dplyr::row_number()) %>%
    dplyr::select(receptor, dplyr::everything())
  
  # Get vector of receptor indices
  receptors <- seq(nrow(receptors_tbl))
  
  # Create a dataframe for the ensemble
  ensemble_tbl <- dplyr::tibble()
  
  recep_file_path_stack <- c()
  
  # For every set of coordinates, perform a set
  # of model runs
  for (receptor in receptors) {
    
    receptor_vals <- 
      get_receptor_values(
        receptors_tbl = receptors_tbl,
        receptor_i = receptor
      )
    
    receptor_i <- receptor_vals$receptor
    lat_i <- receptor_vals$lat
    lon_i <- receptor_vals$lon
    height_i <- receptor_vals$height
    
    list_run_days <- days %>% as.character()
    
    # Make loop with all run days using future/foreach
    future_map(
               .x = list_run_days,
               .f = ~ dirtywind::trajectory_day_parallel(
                          run_day = .x,
                          daily_hours = daily_hours,
                          exec_dir = exec_dir,
                          id_source = id_source,
                          folder_name = folder_name,
                          direction = direction,
                          traj_name = traj_name,
                          receptor_i = receptor_i,
                          lat_i = lat_i,
                          lon_i = lon_i,
                          height_i = height_i,
                          duration = duration,
                          vert_motion = vert_motion,
                          model_height = model_height, 
                          met_files = met_files, 
                          system_type = system_type,
                          met_dir = met_dir,
                          config_list = config_list,
                          ascdata_list = ascdata_list,
                          binary_path = binary_path)
    )
    recep_file_path <<- file.path(exec_dir, id_source, folder_name)
    print(glue::glue("This is the path: {recep_file_path}"))


    # Obtain a trajectory data frame
    traj_tbl <-
      dirtywind::trajectory_read(output_folder = recep_file_path) %>%
      dplyr::as_tibble() %>%
      dplyr::mutate(
        receptor = receptor_i,
        lat_i = lat_i,
        lon_i = lon_i,
        height_i = height_i
      )
    
    ensemble_tbl <-
      ensemble_tbl %>%
      dplyr::bind_rows(traj_tbl)
  }

  if (clean_up) {
    unlink(file.path(exec_dir, traj_output_files()), force = TRUE)
    unlink(recep_file_path_stack, recursive = TRUE, force = TRUE)
  }
  
  ensemble_tbl <-
    ensemble_tbl %>%
    dplyr::select(-c(year, month, day, hour)) %>%
    dplyr::select(
      receptor,
      hour_along,
      traj_dt,
      lat,
      lon,
      height,
      traj_dt_i,
      lat_i,
      lon_i,
      height_i,
      dplyr::everything()
    ) %>%
    dplyr::group_by(
      receptor, hour_along, traj_dt, traj_dt_i, lat_i, lon_i, height_i) %>%
    dplyr::slice(1) %>%
    dplyr::ungroup()
  
  if (direction == "forward") {
    
    ensemble_tbl <-
      ensemble_tbl %>%
      dplyr::arrange(receptor, traj_dt_i)
    
  } else {
    
    ensemble_tbl <-
      ensemble_tbl %>%
      dplyr::arrange(receptor, traj_dt_i, dplyr::desc(hour_along))
  }
  
  ensemble_tbl_complete  <- ensemble_tbl %>%
    dplyr::right_join(
      ensemble_tbl %>%
        dplyr::select(receptor, traj_dt_i, lat_i, lon_i, height_i) %>%
        dplyr::distinct() %>%
        dplyr::mutate(run = dplyr::row_number()),
      by = c("receptor", "traj_dt_i", "lat_i", "lon_i", "height_i")
    ) %>%
    dplyr::select(run, dplyr::everything()) 
   
  if (isTRUE(db)) {

      con <- dirtywind::mydb(cred)

      table_hysplit_sql_id = DBI::Id(schema = schema,
                                  table = table_name)

      ens_table <- ensemble_tbl_complete  %>%
          mutate(name_source = name_source,
                 id_source = id_source) %>%
          dplyr::select(name_source, id_source, dplyr::everything()) 
          
      DBI::dbWriteTable(conn = con, 
                        name = table_hysplit_sql_id,
                        value = ens_table,
                        append = TRUE,
                        overwrite = FALSE)

  } else {
    ensemble_tbl_complete  %>%
          mutate(name_source = name_source,
                 id_source = id_source) %>%
          dplyr::select(name_source, id_source, dplyr::everything())
  }

}
