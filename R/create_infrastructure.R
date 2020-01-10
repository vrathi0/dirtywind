#' Create local directories for running HYSPLIT using splitr 
#' 
#' \code{create_splitr_dirs()} will creaate a set of directories to store and run the HySPLIT model. 
#'
#' @param root_path initial PATH to create all the directories. By default, PATH is set at the root of the
#'        project
#'

#' @export create_split_dirs()
 
create_split_dirs  <- function(path=NULL,
                               overwrite=FALSE){

    if(is.null(path)) {
        warning(glue::glue('Directories are created in {here::here()}'))
    }

    folders <- list(here::here('met'), here::here('data'), here::here('figs'), here::here('hysplit'))

    if (isTRUE(overwrite)) {
        lapply(folders, function(x) {
                   unlink(x, recursive = TRUE)
                   message(glue::glue('Creating folder: {x}'))
                   dir.create(x)
                               }
        )
    } else {
        message('Folder already exists! Keeping all things equal')
    }
}
