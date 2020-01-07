#' Run hysplit_model in parallel using a parameter dataframe and the futures library
#'
#' The run_hysplit_parallel will estimate trajectories using the parameters defined by the user. 
#' The function \code{model_inputs_unit} gives a functional parameter \code{data.frame} that works 
#' with this function. 
#'
#' The implementation of the parallelization depends on furrr and the futures packages. Each session or
#' core will estiamte one row (model-row) and will push the output to a database if \code{db = TRUE}. 
#' For this a list of credentials is needed (cannot use a normal DBI connection due to the use of a pool
#' connection) 
#'
#' @param parameters_df A data.frame or tibble with all the model parameters as defined in model_inputs_unit.
#' @param creds A list with all the \code{RPostgres::dbConnect()} arguments.
#' @param direction Direction of modeling (i.e. \code{forward} or \code{backwards}) \code{backwards} is the default
#' @param met_type Reanalysis data for the model (see \code{splitr} documentation).
#' @param met_dir A meteo storage dir. \code{'met'} is the default one.
#' @param clean_up Should all modeling related files be deleted? Default is \code{TRUE}.

#' @export parallel_hysplit
parallel_hysplit  <- function(parameters_df,
                              creds,
                              direction = 'forward',
                              met_type,
                              mer_dir = here::here('met'),
                              clean_up = TRUE,
                              public_ip = NULL,
                              ec2=FALSE){

    if (isTRUE(ec2)){
        n_nodes  <- length(public_ip)
        cls  <- make_cluster_ec2(public_ip)
        message(glue::glue('Socket cluster with {n_nodes} for {public_ip}'))

        plan(list(tweak(cluster, workers = cls), multicore))
    } else {
        message('Code running locally')
        plan(multicore)
    }

  parameters_df %>%
  dplyr::mutate(model_traj =  furrr::future_pmap(list('lat' = latitude,
                               'lon' = longitude,
                               'height' = stack_height,
                               'name_source' = facility_name,
                               'id_source' = facility_id,
                               'duration' = duration,
                               'days' = seq_dates,
                               'daily_hours' = daily_hours,
                               'direction' = 'forward',
                               'met_type' = 'reanalysis',
                               'met_dir' = here::here('met'),
                               'exec_dir' = here::here("hysplit"),
                               'clean_up' = FALSE,
                               'cred'= list(creds)
                              ),
                           dirtywind::hysplit_trajectory,
                           .progress = TRUE)
  )

}

