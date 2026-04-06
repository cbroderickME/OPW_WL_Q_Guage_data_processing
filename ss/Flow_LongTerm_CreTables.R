#################################################################################################################
## READ IN 15MIN DATA WHICH HAS BEEN DOWNLOADED FROM WATERLEVEL.IE/Hydro-Data AND CREATE TABLES OF THE TIMESERIES
#################################################################################################################
# Clean variables
rm(list = ls())
gc()

# set time 
Sys.setenv(TZ = "GMT")

# load libraries
if (!require("pacman")) install.packages("pacman")
pacman::p_load("dplyr", "here")

# set working directory
wkdir<-here()
setwd(wkdir)

# Read in metadata showing which stations are available and can be processed
log_processed<-read.csv(paste(wkdir, "/Output/log/Log_Processed_LTermObs_Cumces.csv", sep=""))

# Select only those stations which can be processed
Flw_time_series<-log_processed[which(log_processed$If_Processed==TRUE),]

# Create new column for timeseries
Flw_time_series$time_series=NA

for (i in seq(1, nrow(Flw_time_series))){
  print(i)
  # read in the first station
  if (i==1){
    # change directory
    # read in first station from the repository
    gauge_sel<-Flw_time_series$Gauge_ID[i]
    setwd(paste(wkdir, "/Output/Gauges/GaugeID_", gauge_sel, sep=""))
    complete1<-readRDS('Interp_Flow.rds')
    # add the time series to an additional column
    Flw_time_series$time_series[i]<-list(complete1)
    # change the column names
    complete1 = complete1[, 1:2]
    names(complete1)=c("Date", paste("ID_", gauge_sel, sep=""))
  } else{
    
    # read in second station onwards
    gauge_sel<-Flw_time_series$Gauge_ID[i]
    setwd(paste(wkdir, "/Dwnld_LongTerm_Hydo_OPW/Data/GaugeID_", gauge_sel, sep=""))
    complete2plus<-readRDS('Interp_Flow.rds')
    Flw_time_series$time_series[i]<-list(complete2plus)
    complete2plus<-complete2plus[,1:2]
    names(complete2plus)=c("Date", paste("ID_", gauge_sel, sep=""))
    complete1 <- full_join(complete1, complete2plus, by="Date")
  }
}
# rename datasets based on whether the data is saved in a row or column
Flw_complete_OPW_TS_col<-complete1
Flw_complete_OPW_TS_row<-Flw_time_series

# save the tables 
# firstly ts per column
saveRDS(Flw_complete_OPW_TS_col, paste0(wkdir, "/Output/Tables/Flow_complete_OPW_TS_col.rds"))
# secondly ts per row
saveRDS(Flw_complete_OPW_TS_row, paste0(wkdir, "/Output/Tables/Flow_complete_OPW_TS_row.rds"))
