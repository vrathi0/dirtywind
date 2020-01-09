#' Function for SQL DB management

#' @export send_output_db
send_output_db  <- function(df, cred, table_name, schema, ...) {

    if(is.null(cred)){
        stop('You have to define a list with credential values for database.')
    } else {
        con <- mydb(cred)
    }

    message(glue::glue('Sending {dim(df)[1]} rows to dataabase'))
    DBI::dbWriteTable(conn = con,
                 name = table_name,
                 value = df,
                 row.names = FALSE,
                 overwrite=FALSE,
                 append = TRUE,
                 copy = TRUE,
                 ...)
}

#' @export mydb
mydb <- function(cred) {
  
    if (exists(".cred") && !is.null(.cred) && !identical(.cred, cred)) {
    if (exists(".pool") && !is.null(.pool)) {
      pool::poolClose(.pool)
      .pool <<- NULL
    }
    .cred <<- NULL
  }
  if (!exists(".pool") || is.null(.pool)) {
    .pool <<- do.call(pool::dbPool, cred)
    .cred <<- cred
  }
  conn <- pool::poolCheckout(.pool)
  # hack to always return the pool object, don't "leak" it
  do.call(on.exit, list(substitute(suppressWarnings(pool::poolReturn(conn)))),
          envir = parent.frame())
  conn
}

