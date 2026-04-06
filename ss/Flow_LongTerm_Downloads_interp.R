################################################################################
## Program to download long term Flow data (CUMECS) from OPW website (hydro-data)
################################################################################

# clear datasets
rm(list = ls())
gc()

# set time
Sys.setenv(TZ = "GMT")

### load libraries
if (!require("pacman")) install.packages("pacman")
pacman::p_load("readxl", "lubridate", "statar","httr","rjson","jsonlite","dplyr","stringr", "RANN", "here", "feather")

### set working directory
wkdir<-"E:/RScripts/Dwnld_LongTerm_Hydo_OPW"
setwd(wkdir)

### Get a list of all the potential gauges available from waterlevel.ie
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
mystations<- make_station_details(df_station_details$features.properties$ref , df_station_details$features.geometry$coordinates %>% as.vector() , df_station_details$features.properties$name)

#mystations<-mystations[1:6,]

gauge_lst = as.numeric(as.character(mystations$id))
gauge_lst_nme = as.character(mystations$name)

# save a list of the gauges with download data available
gauge_process_lst = matrix(NA, length(gauge_lst), 1)
gauge_process_lst_nme = matrix(NA, length(gauge_lst), 1)
gauge_process_log = matrix(FALSE, length(gauge_lst), 1)
gauge_process_inter = matrix(NA, length(gauge_lst), 1)
gauge_process_LnkStatus_HD=matrix(NA, length(gauge_lst), 1)

# run a loop to download all datasets from OPW websites
# the programme works by attempting to download and appended the data for all stations
# if they are not processed successfully then they are assumed not to have one of the required datasets
# Information on whether a gauge was processed successfully is stored in a log file
for (val in seq(from=1, to=length(gauge_lst))){
  #val=5
# record whether the data could be downloaded from waterlevel and hydrodata websites
gauge_sel<-gauge_lst[val]
gauge_sel_adj<-str_pad(gauge_sel, 5, "pad"=0)
unlink(paste(wkdir, "/Output/Gauges/GaugeID_", gauge_sel, sep=""), recursive = TRUE)
print(gauge_sel_adj)

LnkHD<-httr::GET(paste("http://waterlevel.ie/hydro-data/stations/", gauge_sel_adj, "/Parameter/Q/complete.zip", sep=""))
gauge_process_LnkStatus_HD[val]<-LnkHD$status_code

# print(val)
  tryCatch({
    gauge_sel<-gauge_lst[val]
    gauge_sel_adj<-str_pad(gauge_sel, 5, "pad"=0)
    gauge_process_lst[val]<-gauge_lst[val]
    gauge_process_lst_nme[val]<-gauge_lst_nme[val]

    ######## Record the gauge ID
    dir.create(paste(wkdir, "/Output/Gauges/GaugeID_", gauge_sel, sep=""))
    setwd(paste(wkdir, "/Output/Gauges/GaugeID_", gauge_sel, sep=""))

    ######## Download long term Flow from HYDRO-DATA -----------------------------
    download.file(paste("http://waterlevel.ie/hydro-data/stations/", gauge_sel_adj, "/Parameter/Q/complete.zip", sep=""), paste("Flw_complete.zip", sep=""))

    ######## unzip and load file
    unzip("Flw_complete.zip")
    complete<-read.csv(list.files(pattern = glob2rx("tsdata*")), sep="\t", skip=7, header=TRUE)
    complete<-as_tibble(complete)
    file.remove(list.files(pattern = glob2rx("tsdata*")))

    ######## Convert datetime format
    complete$Date = as.POSIXct(complete$Date, tz = 'GMT')

    ######## Download last months stage height above OD from HYDRO-DATA ---------------------------
    download.file(paste("http://waterlevel.ie/hydro-data/stations/", gauge_sel_adj, "/Parameter/Q/thismonth.zip", sep=""), paste("Flw_thismonth.zip", sep=""))

    ######## unzip and load file
    unzip("Flw_thismonth.zip")
    thismonth<-read.csv(list.files(pattern = glob2rx("tsdata*")), sep="\t", skip=7, header=TRUE)
    thismonth<-as_tibble(thismonth)
    file.remove(list.files(pattern = glob2rx("tsdata*")))

    ######## Convert datetime format
    thismonth$Date = as.POSIXct(thismonth$Date, tz = 'GMT')

    ######## Join Datasets but in order of complete, this month and realtime
    # anti_join(x,y) return all rows from x where there are not matching values in y, keeping just columns from x.
    thismonth_req <- anti_join(thismonth, complete, by = "Date")
    comp_series<-rbind(complete[, 1:3], thismonth_req[, 1:3])

    ######## test duplicate values
    if(length(which(duplicated(comp_series$Date)))>0){
      ("Duplicate dates")
      print(paste0(gauge_sel, "Duplicate values"))
    }

    ######## sort data based on the datestamp
    comp_series_srt_id <- rev(order(as.numeric(comp_series$Date)))
    comp_series_srt = comp_series[comp_series_srt_id,]

    ### set up datasets to write
    Cumces_raw<-comp_series_srt$Value
    Date_raw <- comp_series_srt$Date
    Quality_raw <- comp_series_srt$Quality

    # create a dataframe with data to save
    comp_series_raw <- cbind.data.frame(Date_raw, Cumces_raw, Quality_raw)

    #write out as RDS file
    saveRDS(comp_series_raw, 'Raw_Flow.rds')

    ########################### PROCESS INTERPOLATED AND INFILLED DATASETS ######################################
    ######## Actual time stamp
    act_ts = comp_series_srt$Date

    ######## create artifical time stamp (from 1940 to current time) and merge
    gen_ts = rev(seq(ymd_hm('1940-06-01 00:00', tz = "GMT"), lubridate::round_date(Sys.time(), "15 minutes") , by = '15 mins'))
    gen_ts = lubridate::round_date(gen_ts, "15 minutes")

    # linearly interpolate
    y_query= approx(comp_series_srt$Date, comp_series_srt$Value, gen_ts, method="linear", rule=1)

    # find the distance of each query point to the  nearest observed point in minutes
    x_query_nn<-nn2(data=comp_series_srt$Date, query=y_query$x, k=1)
    x_query_nn$nn.dists=x_query_nn$nn.dists/60

    # if the distance between the  query point and  the  nearest observed point is >25 mins then
    # recode the interpolated point as nan
    y_query_rm<-x_query_nn$nn.dists>15
    y_query$y[y_query_rm]=NA

    # tidy up
    comp_series_interp<-as.data.frame(cbind(y_query$x, y_query$y))
    names(comp_series_interp)<-c('Date', 'Value')
    comp_series_interp<-as_tibble(comp_series_interp)
    comp_series_interp$Date<-as.POSIXct(comp_series_interp$Date, origin='1970-01-01')

    ### set up datasets to write
    Cumces_interp<-comp_series_interp$Value
    Date_interp <- comp_series_interp$Date

    # create a dataframe with data to save
    comp_series_interp <- cbind.data.frame(Date_interp, Cumces_interp)

    #write out as RDS file
    saveRDS(comp_series_interp, 'Interp_Flow.rds')
    
    #write out as feather file
    write_feather(comp_series_interp, 'Interp_Flow.feather')
    
    # save whether the gauge data was processed
    gauge_process_log[val] = TRUE 
    
  }, error=function(e){})
}

## write log file to show which gauges were processed
log_processed<-as.data.frame(cbind(unlist(gauge_process_lst), unlist(gauge_process_lst_nme), as.character(gauge_process_log), unlist(gauge_process_LnkStatus_HD)))
names(log_processed)<-c("Gauge_ID",  "Gauge_Name",  "If_Processed", "Status on HD complete")
write.csv(log_processed, paste(wkdir, "/Output/log/Log_Processed_LTermObs_Cumces_", Sys.Date(), ".csv", sep=""), row.names = FALSE)
