#' Function for SQL DB management

#' @export send_output_db
send_output_db  <- function(traj_model, con, table_name, schema, ...) {

    table_out  <- glue("{schema}.{table_name}")

    if(inherits(model, 'traj_model')) {
        db_out  <- dplyr::as.tbl(traj_model$traj_df)
        dbWriteTable(conn=con,
                     name=table_out,
                     row.names=FALSE,
                     append=TRUE,
                     copy=TRUE,
                     ...)
    }

}


