#' Load pollution origin data (coal-plan) to database following DisperseR vignette
#' https://htmlpreview.github.io/?https://github.com/garbulinskamaja/disperseR/blob/master/vignettesHTML/Vignette_Units_Preparation.html
#'
#' @param conn A DBI-style connection object to the database.
#' @param schema Database schema name.
#' @param table_name Database table name (within schema).
#' @param save_local If `TRUE` not use the database and save a local CSV file (default is `FALSE`).
#'

# Make sure data.table knows we know we're using it
.datatable.aware = TRUE

#' @export load_plant_data
load_plant_data  <- function(conn,
                             schema,
                             table_name,
                             save_local=FALSE,
                             overwrite=FALSE) {


    if(isFALSE(save_local) & missing(conn)) {
        stop('conn DBI object is missing without default')
    }

    if(isFALSE(save_local)) {
        schemas_db = dbGetQuery(conn, 'select schema_name from information_schema.schemata')[[1]]

        if(! schema %in% schemas_db) {
            print(glue::glue('Creating database schema {schema}'))
            dbExecute(conn, glue('create schema if not exists {schema}'))
        }

        if (table_name %in%  dbListTables(conn, table_shema=schema)) {
            print(glue::glue('{table_name} already exists in the schema: {schema}. If you want',
                       'to overwrite, set overwrite=TRUE'))
        }
    }


    url_emissions <- "https://dataverse.harvard.edu/api/access/datafile/3086908?gbrecs=true"
    point_inputs  <- "ftp://newftp.epa.gov/air/emismod/2014/v2/2014fd/emissions/2014fd_inputs_point.zip"
    
    emissions_filename  <- 'AMPD_Unit_with_Sulfur_Content_and_Regulations_with_Facility_Attributes.csv'
    if (emissions_filename %in% list.files()){
        message('Reading emissions file from local system')
        emissions_data  <-  read.csv(emissions_filename)
    } else {
        emissions_data  <- read.csv(url_emissions)
    }

    if ('2014fd_inputs_point.zip' %in% list.files()){
        temp_path <- tempdir()
        unzip('2014fd_inputs_point.zip', exdir=temp_path)
        csv_path <- file.path(temp_path, 
                              '2014fd_cb6_14j/inputs/ptegu/ptegu_2014NEIv2_POINT_20171103_final_21dec2017_nf_v2.csv') 
        points_df  <- data.table::fread(csv_path, skip=18)
 
    } else {
        temp_path <- tempdir()
        zip_path  <- file.path(temp_path, '2014fd_cb6_14j.zip')
        filenames_ftp  <- RCurl::getURL("ftp://newftp.epa.gov/air/emismod/2014/v2/2014fd/emissions/",
                                        ftp.use.epsv = FALSE,
                                        dirlistonly = TRUE)
        
        filenames_list  <- strsplit(filenames_ftp, "\n")[[1]] 
        
        if ('2014fd_inputs_point.zip' %in% filenames_list) {
            download.file(point_inputs, zip_path)
            unzip(zip_path, exdir=temp_path)
            csv_path <- file.path(temp_path, 
                                  '2014fd_cb6_14j/inputs/ptegu/ptegu_2014NEIv2_POINT_20171103_final_21dec2017_nf_v2.csv') 
            points_df  <- data.table::fread(csv_path, skip=18)
        }
    }
    
    unlink(temp_path)

    # Clean data following Vignette preparation
    require(data.table)
    d_nei_unique <- unique(points_df[, .(
                                    facility_name,
                                    Facility.ID..ORISPL. = oris_facility_code,
                                    Unit.ID = oris_boiler_id,
                                    stkhgt,
                                    stkdiam,
                                    stktemp,
                                    stkvel,
                                    latitude,
                                    longitude
                                    )])

    d_nei_unique <- d_nei_unique[Facility.ID..ORISPL. != "" & Unit.ID != ""]
    d_nei_unique <- d_nei_unique[, Facility.ID..ORISPL. := as.numeric(d_nei_unique$Facility.ID..ORISPL.)]
   
    d_nei_clean <- as.data.frame(d_nei_unique) %>% 
      dplyr::select(facility_name, 
             'facility_id' = 'Facility.ID..ORISPL.',
             'unit_id' = 'Unit.ID',
             'stack_height' = 'stkhgt',
             'stack_diam' = 'stkdiam',
             'stack_temp' = 'stktemp',
             latitude, longitude) %>% 
      dplyr::mutate(facilname = stringr::str_to_upper(facility_name),
             latitude = round(latitude, 3),
             longitude = round(longitude, 3),
             stack_height = measurements::conv_unit(stack_height, 'ft', 'm'),
             stack_diam = measurements::conv_unit(stack_diam, 'ft', 'm'),
             stack_temp = measurements::conv_unit(stack_temp, 'F', 'K')
             ) %>% 
      as_tibble() 

    years  <- c(1995:2015)
    year_plants  <- lapply(years, function(x){
                               emissions_data_year  <- emissions_data %>%
                                   as.data.frame(.) %>%
                                   filter(Year == x)

                               emission_data_coal <- emissions_data_year %>% 
                               dplyr::select('facility_id' = 'Facility.ID..ORISPL.',
                                              'unit_id' = 'Unit.ID',
                                              'facility_name' = 'Facility.Name.x',
                                              'month' = 'Month',
                                              'year' = 'Year',
                                              'latitude' = 'Facility.Latitude.x',
                                              'longitude' = 'Facility.Longitude.x', 
                                              'is_coal' = 'Fuel1.IsCoal',
                                              'state' = 'State.y',
                                              'county' = 'County.x',
                                              'fips' = 'FIPS',
                                              'so2_tons' = 'SO2..tons.',
                                              'co2_tons' = 'CO2..short.tons.',
                                              'nox_tons' = 'NOx..tons.',
                                              'has_so2_scrub' = "Has.SO2.Scrub",
                                              'has_pm_scrub' = "Has.PM.Scrub" ,
                                              'has_nox_scrub' = "Has.NOx.Scrub"  
                                ) %>% 
                                dplyr::filter((is_coal == 1)) %>% 
                                dplyr::group_by(facility_id, 
                                          unit_id, 
                                          facility_name,
                                          latitude, 
                                          longitude, 
                                          year,
                                          state, 
                                          county, 
                                          fips) %>% 
                                dplyr::summarize(so2_tons = sum(so2_tons, na.rm = TRUE),
                                          co2_tons = sum(co2_tons, na.rm = TRUE),
                                          nox_tons = sum(nox_tons, na.rm = TRUE),
                                          has_so2_scrub = max(has_so2_scrub),
                                          has_pm_scrub = max(has_pm_scrub),
                                          has_nox_scrub = max(has_nox_scrub)
                                ) %>% 
                                dplyr::left_join(d_nei_clean, by=c('facility_id', 'unit_id')) %>% 
                                dplyr::select(-latitude.y, -longitude.y, -facility_name.y,
                                              latitude = 'latitude.x',
                                              longitude = 'longitude.x',
                                              facility_name = 'facility_name.x')

                                emission_data_coal %>%
                                    dplyr::mutate(stack_height = ifelse(is.na(stack_height), 
                                                                 mean(emission_data_coal$stack_height, na.rm=TRUE),
                                                                 stack_height)
                                )
                                #return(emission_data_coal_imp)
             })

    emissions_all_years <- do.call(rbind, year_plants) %>%
         dplyr::distinct(facility_id, unit_id, .keep_all = TRUE)

    if (isTRUE(save_local)) {
        if(!dir.exists(here::here('data'))){
            dir.create(here::here('data'))
        } else {
            write.csv(emissions_all_years, 
                      here::here('data', 'coal_plant_inventory_all_years.csv'), 
                      row.names = FALSE)
        }
    } else {
        print('Uploading to database')
        RPostgres::dbWriteTable(
                     con = conn,
                     name = c(schema, table_name),
                     value = emissions_all_years,
                     row.names = FALSE,
                     copy = TRUE,
                     overwrite = overwrite
                     )
      }
}
