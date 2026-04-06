################################################################################
## Program to download long term hydro data from the OPW
## Updated to calculate daily means for both 00-00 and 09-09 time intervals
################################################################################

rm(list = ls()); gc()

# set time
Sys.setenv(TZ = "GMT")

### load libraries
if (!require("pacman")) install.packages("pacman")
pacman::p_load("readxl","Stack","lubridate","statar","httr","rjson","jsonlite","dplyr","stringr", "RANN", "here")

### set working directory
wkdir<-"C:/Users/CBroderick/Downloads/Ai_Project"
setwd(wkdir)
dir.create(paste0(wkdir, "/WL_Output_", Sys.Date()))
dir.create(paste0(wkdir, "/WL_Output_", Sys.Date() ,"/Gauges"))
dir.create(paste0(wkdir, "/WL_Output_", Sys.Date() ,"/log"))

### Get a list of all the potential gauges available
sensors <- list(code = c( "0001", "0002", "0003", "OD"), name = c("level", "temp", "voltage", "datum") )
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
mystations<- make_station_details(df_station_details$features.properties$ref , df_station_details$features.geometry.coordinates %>% as.vector() , df_station_details$features.properties$name)

gauge_lst = as.numeric(as.character(mystations$id))
gauge_lst_nme = as.character(mystations$name)

# save a list of the gauges with download data available
gauge_process_lst = matrix(NA, length(gauge_lst), 1)
gauge_process_lst_nme = matrix(NA, length(gauge_lst), 1)
gauge_process_log = matrix(NA, length(gauge_lst), 1)
gauge_process_LnkStatus_wl=matrix(NA, length(gauge_lst), 1)
gauge_process_LnkStatus_HD=matrix(NA, length(gauge_lst), 1)


gauge_lst=11001
# run a loop to download all datasets from OPW website
for (val in seq(from=1, to=length(gauge_lst))){
  #val=1  # Comment out to process all gauges
  
  print(val)
  gauge_sel<-gauge_lst[val]
  gauge_sel_adj<-str_pad(gauge_sel, 5, "pad"=0)
  
  source<-paste0("http://waterlevel.ie/hydro-data/data/internet/stations/0/", gauge_sel_adj, "/S/Waterlevel_complete.zip")
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
    dest_dir<-paste0(wkdir, "/WL_Output_", Sys.Date() ,"/Gauges/GaugeID_", gauge_sel)
    dir.create(dest_dir, recursive = TRUE, showWarnings = FALSE)
    setwd(dest_dir)
    
    ######## Download long term stage height above OD from HYDRO-DATA -----------------------------
    download.file(source, destfile = "Waterlevel_complete.zip", quiet = TRUE)
    
    ######## unzip and load file
    unzip("Waterlevel_complete.zip")
    file.remove("Waterlevel_complete.zip")
    
    complete<-read.csv(list.files(pattern = glob2rx("tsvalues*")), sep=";", skip=10, header=TRUE)
    complete<-as_tibble(complete)
    file.remove(list.files(pattern = glob2rx("tsvalues*")))
    
    ######## Convert Timestamp format
    complete$Timestamp = as_datetime(complete$X.Timestamp, tz = 'GMT')
    
    ######## sort data based on the Timestamp
    comp_series_srt_id <- rev(order(as.numeric(complete$Timestamp)))
    comp_series_srt = complete[comp_series_srt_id,]
    
    ### set up datasets to write
    Level_raw<-comp_series_srt$Value
    Timestamp_raw <- comp_series_srt$Timestamp
    Quality_raw <- comp_series_srt$Quality.Code
    
    # create a dataframe with data to save
    comp_series_raw <- cbind.data.frame(Timestamp_raw, Level_raw, Quality_raw)
    
    #write out as RDS file
    saveRDS(comp_series_raw, 'Raw_Level.rds')
    
    ########################### PROCESS INTERPOLATED AND INFILLED DATASETS ######################################
    ######## Actual time stamp
    act_ts = comp_series_srt$Timestamp
    
    ######## create artificial time stamp (from 1940 to current time) and merge
    gen_ts = rev(seq(ymd_hm('1940-06-01 00:00', tz = "GMT"), Sys.time()+days(15), by = '15 mins'))
    gen_ts = lubridate::round_date(gen_ts, "15 minutes")
    
    # linearly interpolate
    y_query= approx(comp_series_srt$Timestamp, comp_series_srt$Value, gen_ts, method="linear", rule=1)
    
    # find the distance of each query point to the  nearest observed point in minutes
    x_query_nn<-nn2(data=comp_series_srt$Timestamp, query=y_query$x, k=1)
    x_query_nn$nn.dists=x_query_nn$nn.dists/60
    
    # if the distance between the query point and the nearest observed point is >15 mins then
    # recode the interpolated point as NA
    y_query_rm<-x_query_nn$nn.dists > 15
    y_query$y[y_query_rm] = NA
    
    # tidy up
    comp_series_interp<-as.data.frame(cbind(y_query$x, y_query$y))
    names(comp_series_interp)<-c('Timestamp', 'Value')
    comp_series_interp<-as_tibble(comp_series_interp)
    comp_series_interp$Timestamp<-as.POSIXct(comp_series_interp$Timestamp, origin='1970-01-01', tz="GMT")
    
    ### set up datasets to write
    Levels_interp<-comp_series_interp$Value
    Timestamp_interp <- comp_series_interp$Timestamp
    
    # create a dataframe with data to save
    comp_series_interp <- cbind.data.frame(Timestamp_interp, Levels_interp)
    
    #write out as RDS file
    saveRDS(comp_series_interp, 'Interp_Level.rds')
    write.csv(comp_series_interp, 'Interp_Level.csv', row.names = FALSE)
    
    ############# Calculate Mean Daily Level 00-00 #############
    
    daily_avg_dir <- paste0(dest_dir, "/Daily_Avg")
    if (!dir.exists(daily_avg_dir)) dir.create(daily_avg_dir)
    
    # Midnight-to-midnight daily average
    daily_level_avg_00to00 <- comp_series_interp %>%
      mutate(date = as.Date(Timestamp_interp)) %>%
      group_by(date) %>%
      summarise(
        n_obs = sum(!is.na(Levels_interp)),
        Mean_Level = if_else(n_obs > 0,
                             mean(Levels_interp, na.rm = TRUE),
                             NA_real_)
      ) %>%
      ungroup()
    
    saveRDS(daily_level_avg_00to00, file = paste0(daily_avg_dir, "/Daily_Mean_Level_00to00.rds"))
    write.csv(daily_level_avg_00to00, file = paste0(daily_avg_dir, "/Daily_Mean_Level_00to00.csv"), row.names = FALSE)
    
    ############# Calculate Mean Daily Level 09-09 #############
    
    daily_level_avg_09to09 <- comp_series_interp %>%
      mutate(date_9_9 = as.Date(Timestamp_interp - hours(9))) %>%  # shift by 9 hours to get 9am-to-9am days
      group_by(date_9_9) %>%
      summarise(
        n_obs = sum(!is.na(Levels_interp)),
        Mean_Level = if_else(n_obs > 0,
                             mean(Levels_interp, na.rm = TRUE),
                             NA_real_)
      ) %>%
      ungroup()
    
    saveRDS(daily_level_avg_09to09, file = paste0(daily_avg_dir, "/Daily_Mean_Level_09to09.rds"))
    write.csv(daily_level_avg_09to09, file = paste0(daily_avg_dir, "/Daily_Mean_Level_09to09.csv"), row.names = FALSE)
    
    #save whether the gauge data was processed
    gauge_process_log[val] = TRUE 
  }, error=function(e){
    message("Error processing gauge: ", gauge_sel, "\n", e)
    gauge_process_log[val] = FALSE
  })
  
}

## write log file to show which gauges were processed
log_processed<-as.data.frame(cbind(unlist(gauge_process_lst), unlist(gauge_process_lst_nme), as.character(gauge_process_log), unlist(gauge_process_LnkStatus_wl), unlist(gauge_process_LnkStatus_HD)))
names(log_processed)<-c("Gauge_ID",  "Gauge_Name",  "If_Processed", "Status on wl.ie", "Status on HD complete")
write.csv(log_processed, paste0(wkdir, "/WL_Output_", Sys.Date() ,"/log/", "Log_Processed_LTermObs_StgHght.csv", sep=""), row.names = FALSE)
