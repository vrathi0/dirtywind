#' Load pollution origin data (coal-plan) to database following DisperseR vignette
#' https://htmlpreview.github.io/?https://github.com/garbulinskamaja/disperseR/blob/master/vignettesHTML/Vignette_Units_Preparation.html
#'
#' @param conn A DBI-style connection object to the database.
#' @param schema Database schema name.
#' @param table_name Database table name (within schema).
#' @param save_local If `TRUE` not use the database and save a local CSV file (default is `FALSE`).
#'

#' @export load_plant_data
load_plant_data  <- function(conn,
                             schema,
                             table_name,
                             save_local=FALSE){


    if(isFALSE(save_local) & missing(conn)) {
        stop('conn DBI object is missing without default')
    }


    url_emissions <- "https://dataverse.harvard.edu/api/access/datafile/3086908?gbrecs=true"
    point_inputs  <- "ftp://newftp.epa.gov/air/emismod/2014/v2/2014fd/emissions/2014fd_inputs_point.zip"
    
    emissions_df  <- read.csv(url_emissions)

    temp_path <- tempdir()
    zip_path  <- file.path(temp_path, '2014fd_cb6_14j.zip')

    filenames_ftp  <- getURL("ftp://newftp.epa.gov/air/emismod/2014/v2/2014fd/emissions/",
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

    unlink(temp_path)

    # Clean data following Vignette preparation

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
      dplyr::mutate(facilname = str_to_upper(facility_name),
             latitude = round(latitude, 3),
             longitude = round(longitude, 3),
             stack_height = conv_unit(stack_height, 'ft', 'm'),
             stack_diam = conv_unit(stack_diam, 'ft', 'm'),
             stack_temp = conv_unit(stack_temp, 'F', 'K')
             ) %>% 
      as_tibble() 

    emission_data_coal <- emissions_data %>% 
      dplyr::select('facility_id' = 'Facility.ID..ORISPL.',
                    'unit_id' = 'Unit.ID',
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
      dplyr::filter((is_coal ==1)) %>% 
      dplyr::group_by(facility_id, 
                unit_id, 
                latitude, 
                longitude, 
                state, 
                county, 
                fips,
                year) %>% 
      dplyr::summarize(so2_tons = sum(so2_tons, na.rm = TRUE),
                co2_tons = sum(co2_tons, na.rm = TRUE),
                nox_tons = sum(nox_tons, na.rm = TRUE),
                has_so2_scrub = max(has_so2_scrub),
                has_pm_scrub = max(has_pm_scrub),
                has_nox_scrub = max(has_nox_scrub)
      ) %>% 
      dplyr::left_join(d_nei_clean, by=c('facility_id', 'unit_id')) %>% 
      dplyr::select(-latitude.y, -longitude.y,
                    latitude = 'latitude.x',
                    longitude = 'longitude.x')

      emission_data_coal_imp  <- emission_data_coal %>%
          mutate(stack_height = ifelse(is.na(stack_height), 
                                       mean(emission_data_coal$stack_height, na.rm=TRUE),
                                       stack_height)
      )

      if (isTRUE(save_local)) {
          write.csv(emission_data_coal_imp, 
                    'coal_plant_inventory_all_years.csv', 
                    row.names = FALSE)
      } else {

          dbWriteTable(
                       con = conn,
                       name = glue("{schema}.{table_name}"),
                       value = emission_data_coal_imp,
                       row.names = FALSE,
                       copy = TRUE,
                       ...
                       )

      }
}
