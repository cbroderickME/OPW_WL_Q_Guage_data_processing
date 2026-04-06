################################################################################
## Program to download long term hydro data from the OPW
################################################################################

rm(list = ls()); gc()

# set time
Sys.setenv(TZ = "GMT")

### Load libraries
library(readxl)
library(lubridate)
library(statar)
library(httr)
library(rjson)
library(jsonlite)
library(dplyr)
library(stringr)
library(RANN)
library(here)
library(zoo)  # Needed for na.approx used in hourly interpolation


# Create a named vector with codes and descriptions
flow_data_codes <- c(
  "31" = "Flow data estimated using a rating curve considered to be of good quality and inspected water level data. Data may contain some error but is considered acceptable for general use.",
  "32" = "As per Code 31, but using water level data of Code 32.",
  "36" = "Flow data estimated using a rating curve considered to be of fair quality and inspected or corrected water level data. Data may contain a fair degree of error and should be treated with some caution.",
  "46" = "Flow data estimated using a rating curve considered to be of poor quality and inspected or corrected water level data. Data may contain significant error and should only be used for indicative purposes.",
  "56" = "Flow data estimated using an extrapolated rating curve (see Section 3.2) and inspected or corrected water level data. Reliability is unknown; data should be treated with caution.",
  "96" = "Flow data estimated using a provisional rating curve. Data may be subject to revision following retrospective assessment with recent flow measurements.",
  "101" = "Flow data estimated using unreliable water level data. Data is suspected to be erroneous and must only be used with caution.",
  "151" = "Unusable data: dry channel or logger malfunction.",
  "254" = "Flow data estimated using unchecked water level data. Data is provisional and must be used with caution.",
  "255" = "Missing: no data available."
)

### set working directory
wkdir<-"C:/Users/CBroderick/Downloads/OPW/wkdir"
setwd(wkdir)
dir.create(paste0(wkdir, "/../Q_Output_", Sys.Date()))
dir.create(paste0(wkdir, "/../Q_Output_", Sys.Date() ,"/Gauges"))
dir.create(paste0(wkdir, "/../Q_Output_", Sys.Date() ,"/log"))

### Get a list of all the potential gauges available
sensors <- list(code = c( "0001", "0002", "0003", "OD"), name = c("Discharge", "temp", "voltage", "datum") )
# JSON
station_details_url<- 'https://waterlevel.ie/geojson/'
data_stations <- GET(station_details_url)
data_json <- content(data_stations, as = "text",encoding = "UTF-8")
jfile <- fromJSON(data_json)
df_station_details <- as.data.frame(jfile)
colnames(df_station_details)
make_station_details <- function(id,coords, name)
{
  stations <- list()
  stations$id <- str_sub( id , -5,-1)
  temp <- unlist(coords)
  stations$long<- temp[c(TRUE,FALSE)]
  stations$lat <- temp[c(FALSE,TRUE)]
  stations$name <- name
  return( as.data.frame(stations) )
}
mystations<- make_station_details(df_station_details$features.properties$ref , df_station_details$features.geometry$coordinates %>% as.vector() , df_station_details$features.properties$name)

#mystations<-mystations%>%filter(id=='11001')

gauge_lst = as.numeric(as.character(mystations$id))
gauge_lst_nme = as.character(mystations$name)

# save a list of the gauges with download data available
gauge_process_lst = matrix(NA, length(gauge_lst), 1)
gauge_process_lst_nme = matrix(NA, length(gauge_lst), 1)
gauge_process_log = matrix(NA, length(gauge_lst), 1)
gauge_process_LnkStatus_wl=matrix(NA, length(gauge_lst), 1)
gauge_process_LnkStatus_HD=matrix(NA, length(gauge_lst), 1)

# run a loop to download all datasets from OPW website
# the program works by attempting to download the data for all stations
# if they are not processed successfully then they are assumed not to have the required data sets
# Information on whether a gauge was processed successfully is stored in a log file

for (val in seq(from=1, to=length(gauge_lst))){

  val=1
  gauge_sel<-gauge_lst[val]
  print(gauge_sel)
  gauge_sel_adj<-str_pad(gauge_sel, 5, "pad"=0)
  
  source<-paste0("http://waterlevel.ie/hydro-data/data/internet/stations/0/", gauge_sel_adj, "/Q/Discharge_complete.zip")
  LnkHD<-httr::GET(source)
  print(gauge_sel)
  print(LnkHD)
  gauge_process_LnkStatus_HD[val]<-LnkHD$status_code
  
  tryCatch({
    gauge_sel<-gauge_lst[val]
    gauge_sel_adj<-str_pad(gauge_sel, 5, "pad"=0)
    gauge_process_lst[val]<-gauge_lst[val]
    gauge_process_lst_nme[val]<-gauge_lst_nme[val]
    
    ######## Record the gauge ID
    dest_dir<-paste0(wkdir, "/../Q_Output_", Sys.Date() ,"/Gauges/GaugeID_", gauge_sel)
    dir.create(dest_dir)
    setwd(dest_dir)
    
    ######## Download long term stage height above OD from HYDRO-DATA -----------------------------
    download.file(source, destfile = "Discharge_complete.zip", quiet = TRUE)
    
    ######## unzip and load file
    unzip("Discharge_complete.zip")
    file.remove("Discharge_complete.zip")
    
    complete<-read.csv(list.files(pattern = glob2rx("tsvalues*")), sep=";", skip=10, header=TRUE)
    complete<-as_tibble(complete)
    file.remove(list.files(pattern = glob2rx("tsvalues*")))
    
    ######## Convert Timestamp  format
    complete$Timestamp = as_datetime(complete$X.Timestamp, tz = 'GMT')
    
    ######## sort data based on the Timestamp
    comp_series_srt_id <- rev(order(as.numeric(complete$Timestamp)))
    comp_series_srt = complete[comp_series_srt_id,]
    
    ### set up datasets to write
    Discharge_raw<-comp_series_srt$Value
    Timestamp_raw <- comp_series_srt$Timestamp
    Quality_raw <- comp_series_srt$Quality.Code
    
    # create a dataframe with data to save
    comp_series_raw <- cbind.data.frame(Timestamp_raw, Discharge_raw, Quality_raw)
    
    #write out as RDS file
    saveRDS(comp_series_raw, 'Raw_Discharge.rds')
    write.csv(comp_series_raw, 'Raw_Discharge.csv', row.names = FALSE)
    
    ########################### PROCESS INTERPOLATED AND INFILLED DATASETS ######################################
    # Remove rows with missing values
    comp_series_raw <- comp_series_raw %>% filter(!is.na(Discharge_raw))
    
    # Remove rows where Quality_raw is in the exclude list
    exclude_codes <- c(46, 101, 151, 255)
    comp_series_raw <- comp_series_raw %>%
      filter(!(Quality_raw %in% exclude_codes))
    
    ######## Actual time stamp
    act_ts = comp_series_raw$Timestamp_raw
    
    ######## create artifical time stamp (from 2024 to current time) and merge
    gen_ts = rev(seq(ymd_hm('2019-01-01 00:00', tz = "GMT"), Sys.time()+days(15), by = '15 mins'))
    gen_ts = lubridate::round_date(gen_ts, "15 minutes")
    
    # linearly interpolate
    y_query= approx(comp_series_raw$Timestamp_raw, comp_series_raw$Discharge_raw, gen_ts, method="linear", rule=1)
    
    # find the distance of each query point to the  nearest observed point in minutes
    x_query_nn<-nn2(data=comp_series_raw$Timestamp_raw, query=y_query$x, k=1)
    x_query_nn$nn.dists=x_query_nn$nn.dists/60
    
    # if the distance between the  query point and  the  nearest observed point is >15 mins then
    # recode the interpolated point as nan
    y_query_rm<-x_query_nn$nn.dists>15
    y_query$y[y_query_rm]=NA
    
    # tidy up
    comp_series_interp<-as.data.frame(cbind(y_query$x, y_query$y))
    names(comp_series_interp)<-c('Timestamp', 'Value')
    comp_series_interp<-as_tibble(comp_series_interp)
    comp_series_interp$Timestamp<-as.POSIXct(comp_series_interp$Timestamp, origin='1970-01-01')
    
    ### set up datasets to write
    Discharges_interp<-comp_series_interp$Value
    Timestamp_interp <- comp_series_interp$Timestamp
    
    # create a dataframe with data to save
    comp_series_interp <- cbind.data.frame(Timestamp_interp, Discharges_interp)
    
    #write out as RDS file
    saveRDS(comp_series_interp, 'Interp_Discharge.rds')
    write.csv(comp_series_interp, 'Interp_Discharge.csv', row.names = FALSE)
    
    ### HOURLY INTERPOLATION - Equivalent to Python's resample('60T').mean().interpolate(limit=6)
    
    # Convert the 15-min data to hourly mean values
    # require at least 2 valid (non-NA) 15-minute values per hour to compute the hourly mean—otherwise set that hour’s value to NA
    comp_series_hourly <- comp_series_interp %>%
      mutate(Timestamp_interp = lubridate::ceiling_date(Timestamp_interp, unit = "hour")) %>%
      group_by(Timestamp_interp) %>%
      summarise(
        n_values = sum(!is.na(Discharges_interp)),
        Value = if_else(n_values >= 2,
                        mean(Discharges_interp, na.rm = TRUE),
                        NA_real_)
      ) %>%
      select(-n_values) %>%  # remove helper column if you want
      ungroup()
    
    # Replace NaNs resulting from all-NA groups with NA
    comp_series_hourly$Value[is.nan(comp_series_hourly$Value)] <- NA
    
    # Interpolate only internal gaps (limit = 6), equivalent to `interpolate(limit=6, limit_area='inside')`
    comp_series_hourly$Value <- na.approx(comp_series_hourly$Value, maxgap = 6, na.rm = FALSE)
    
    # Save as RDS and CSV
    saveRDS(comp_series_hourly, 'Hourly_Interp_Discharge.rds')
    write.csv(comp_series_hourly, 'Hourly_Interp_Discharge.csv', row.names = FALSE)
    
    
    ### DAILY INTERPOLATION - Equivalent to Python's resample('D').mean(), min 18 hours of data, no interpolation
    
    # Aggregate 15-min interpolated data to daily means
    # Require at least 12 hours = 48 valid 15-min observations per day
    comp_series_daily <- comp_series_interp %>%
      mutate(Date = as.Date(Timestamp_interp)) %>%
      group_by(Date) %>%
      summarise(
        n_values = sum(!is.na(Discharges_interp)),
        Discharge_m3s = if_else(n_values >= 72,  # 12 hours × 4 = 72 intervals
                                mean(Discharges_interp, na.rm = TRUE),
                                NA_real_)
      ) %>%
      ungroup()
    
    # Rename for consistency (optional, already Discharge_m3s)
    colnames(comp_series_daily) <- c("Date", "Discharge_m3s")
    
    # Save daily discharge without interpolation of missing days
    saveRDS(comp_series_daily, 'Daily_Discharge_00_00.rds')
    write.csv(comp_series_daily, 'Daily_Discharge_00_00.csv', row.names = FALSE)    
    
    
    ### DAILY MEAN DISCHARGE BETWEEN 09:00 AND 21:00 ONLY (No interpolation, min 18 hours of data)
    
    comp_series_daily <- comp_series_interp %>%
      filter(format(Timestamp_interp, "%H:%M:%S") >= "09:00:00" & format(Timestamp_interp, "%H:%M:%S") < "21:00:00") %>%
      mutate(Date = as.Date(Timestamp_interp)) %>%
      group_by(Date) %>%
      summarise(
        n_values = sum(!is.na(Discharges_interp)),
        Discharge_m3s = if_else(n_values >= 72,  # 18 hours × 4 = 72 readings
                                mean(Discharges_interp, na.rm = TRUE),
                                NA_real_)
      ) %>%
      ungroup()
    
    # Save daily discharge without interpolating missing days
    saveRDS(comp_series_daily, 'Daily_Discharge_09_21.rds')
    write.csv(comp_series_daily, 'Daily_Discharge_09_21.csv', row.names = FALSE)
    
    
    #save whether the gauge data was processed
    gauge_process_log[val] = TRUE 
  }, error=function(e){})
}

## write log file to show which gauges were processed
log_processed<-as.data.frame(cbind(unlist(gauge_process_lst), unlist(gauge_process_lst_nme), as.character(gauge_process_log), unlist(gauge_process_LnkStatus_wl), unlist(gauge_process_LnkStatus_HD)))
names(log_processed)<-c("Gauge_ID",  "Gauge_Name",  "If_Processed", "Status on wl.ie", "Status on HD complete")
write.csv(log_processed, paste0(wkdir, "/../Q_Output_", Sys.Date() ,"/log/", "Log_Processed_LTermObs_Q.csv", sep=""), row.names = FALSE)

