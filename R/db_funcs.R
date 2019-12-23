#' Function for SQL DB management

#' @export send_output_db
send_output_db  <- function(df, con, table_name, schema, ...) {

    if(is.null(con)){
        con <- pool::dbPool(RPostgres::Postgres(),
                            user=Sys.getenv('USER'),
                            password=Sys.getenv('PASSWORD'),
                            host=Sys.getenv('HOST',),
                            port=Sys.getenv('PORT'),
                            dbname=Sys.getenv('DBNAME'),
                            options=glue::glue("-c search_path={schema}"))
    }

    #table_out  <- DBI::SQL(glue::glue('{schema}.{table_name}'))

    message(glue::glue('Sending {dim(df)[1]} rows to dataabase'))
    DBI::dbWriteTable(conn = con,
                 name = table_name,
                 value = df,
                 row.names = FALSE,
                 append = TRUE,
                 copy = TRUE,
                 ...)
}
