#' Utility function to build a model parameter tibble in time 
#' @param query PostgreSQL query string with unique identifier per source unit (lat, lon, and height).
#' @param con A DBI Connection object (ideally from dbConnect()).
#' @param duration duration of the HySPLIT trajectory model.
#' @param start_date start date of modeling. See splitr documentation for more details.
#' @param end_date end date of modeling. See splitr documentation for more details.
#' @param daily_hours number of hours for modeling points within day. See splitr for more details.
#' @param timedelta time units for modeling. Default is 1 month.


#' @export model_inputs_unit
model_inputs_unit  <- function(query, 
                               con,
                               timedelta = '1 month',
                               start_date,
                               end_date,
                               duration,
                               daily_hours,
                               local_file = NULL) {

    if (is.character(start_date)) {
        start_date = as.Date(start_date)
    }

    if (is.character(end_date)) {
        end_date = as.Date(end_date)
    }

    if (is.null(local_file)){
        if(is.null(query)){
            stop('A SQL query string is needed to build the parameters data.frame')
        }
        
        query_units <- dbGetQuery(conn=con,
                                  statement=query)
    } else {
        query_units <- read.csv(local_file)
    }

    query_parameters <- query_units %>% 
        tibble::as_tibble(.) %>% 
        tidyr::expand(tidyr::nesting(facility_id, latitude,longitude, facility_name, stack_height), 
                      start_date = seq.Date(start_date, end_date, timedelta)) %>%
        tibble::add_column(duration = duration,
                    daily_hours = list(daily_hours),
                        ) %>%
        mutate(seq_dates = map(start_date, ~seq(.x, 
                                                .x + months(1) - lubridate::days(1), 
                                                '1 day')))

    return(query_parameters)

}
