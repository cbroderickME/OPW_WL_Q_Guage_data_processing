# Clean variables
rm(list = ls())
gc()

# set time 
Sys.setenv(TZ = "GMT")

# load libraries
if (!require("pacman")) install.packages("pacman")
pacman::p_load("dplyr", "lubridate")

# set working directory
wkdir<-"E:/RScripts/Dwnld_LongTerm_Hydo_OPW"
setwd(wkdir)

# Read in metadata showing which stations are available and can be processed
log_processed<-read.csv(paste(wkdir, "/Q_Output_2024-06-18/log/Log_Processed_LTermObs_Q_2024-06-22.csv", sep=""))

# Select only those stations which can be processed
Flw_time_series<-log_processed[which(log_processed$If_Processed==TRUE),]
for (i in seq(1, nrow(Flw_time_series))){
    gauge_sel<-Flw_time_series$Gauge_ID[i]
    print(gauge_sel)
    setwd(paste(wkdir, "/Output/Gauges/GaugeID_", gauge_sel, sep=""))
    complete1<-tibble(readRDS('Interp_Flow.rds'))
    write.csv(x = complete1, file = "Flw_time_series.csv", col.names = TRUE, row.names = FALSE)
}
