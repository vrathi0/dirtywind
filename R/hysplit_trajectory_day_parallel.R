#' HySPLIT daily trajectory - Individual estimation for day/hours to facilitate 
#' parallel estimation
#'
#' This function will estimate aa HySPLIT trajectory for a single day and a vector
#' of hours, as shown in the \code{splitr::hysplit_trajetory} and the same named function
#' in this library. The parameters are the same used in those functions. 
#' Ideally this function is for internal use of \code{hysplit_trajectory_parallel_master}.

#' @export trajectory_day_parallel
trajectory_day_parallel  <- function(run_day,
                                     traj_name,
                                     daily_hours,
                                     exec_dir,
                                     id_source,
                                     folder_name,
                                     direction,
                                     receptor_i,
                                     lat_i,
                                     lon_i,
                                     height_i,
                                     duration,
                                     vert_motion,
                                     model_height,
                                     met_files,
                                     system_type,
                                     met_dir,
                                     config_list,
                                     ascdata_list,
                                     binary_path
                                     ){

    # Define starting time parameters
    start_year_GMT <- to_short_year(run_day)
    start_month_GMT <- to_short_month(run_day)
    start_day_GMT <- to_short_day(run_day)
    
    # Sort daily starting hours if given as
    # numeric values
    if (inherits(daily_hours, "numeric")) {
      daily_hours <- formatC(sort(daily_hours), width = 2, flag = 0)
    }
    
    # Make nested loop with daily beginning hours
    for (j in daily_hours) {
      
      start_hour_GMT <- j
      
      if (start_year_GMT > 40) {
        full_year_GMT <- paste0("19", start_year_GMT)
      } else {
        full_year_GMT <- paste0("20", start_year_GMT)
      }

      # Create model folder name for parallel execution
      model_folder_name <- paste0("model-", 
                                  as.character(Sys.getpid()), '-', 
                                  format(Sys.time(), "%Y-%m-%d-%H-%M-%S"), '-',
                                  start_year_GMT, '-',
                                  start_month_GMT, '-',
                                  start_day_GMT, '-',
                                  start_hour_GMT)

      model_folder_path  <- file.path(exec_dir, model_folder_name)


      if (!dir.exists(model_folder_path)){
          dir.create(model_folder_path)
      }
      
      # Write the config and ascdata lists to files in
      # the `exec` directory
      config_list %>% write_config_list(dir = model_folder_path)
      ascdata_list %>% write_ascdata_list(dir = model_folder_path)
      
      # Construct the output filename string for this
      # model run
      output_filename <-
        get_traj_output_filename(
          traj_name = traj_name,
          site = receptor_i,
          direction = direction,
          year = start_year_GMT,
          month = start_month_GMT,
          day = start_day_GMT,
          hour = start_hour_GMT,
          lat = lat_i,
          lon = lon_i,
          height = height_i,
          duration = duration
        )
      
      trajectory_files <<- c(trajectory_files, output_filename)
      
      # Write the CONTROL file
      write_traj_control_file(
        start_year_GMT = start_year_GMT,
        start_month_GMT = start_month_GMT,
        start_day_GMT = start_day_GMT,
        start_hour_GMT = start_hour_GMT,
        lat = lat_i,
        lon = lon_i,
        height = height_i,
        direction = direction,
        duration = duration,
        vert_motion = vert_motion,
        model_height = model_height,
        met_files = met_files,
        output_filename = output_filename,
        system_type = system_type,
        met_dir = met_dir,
        exec_dir = model_folder_path
      )
      
      # The CONTROL file is now complete and in the
      # working directory, so, execute the model run
      sys_cmd <- 
        paste0(
          "(cd \"",
          model_folder_path,
          "\" && \"",
          binary_path,
          "\" ",
          to_log_dev(system_type = system_type),
          ")"
        )
      execute_on_system(sys_cmd, system_type = system_type)

      recep_file_path <<- file.path(exec_dir, id_source, folder_name)
      print(recep_file_path)
      message(recep_file_path)
    
      recep_file_path_stack <<- 
        c(recep_file_path_stack, file.path(exec_dir, id_source))
      
      # Create the output folder if it doesn't exist
      if (!dir.exists(recep_file_path)) {
        dir.create(path = recep_file_path, recursive = TRUE)
      }
      
      # Move files into the output folder
      file.copy(
        from = file.path(model_folder_path, trajectory_files),
        to = recep_file_path,
        copy.mode = TRUE
      )
      
      unlink(file.path(model_folder_path, trajectory_files), force = TRUE)
     
    }
    



}
