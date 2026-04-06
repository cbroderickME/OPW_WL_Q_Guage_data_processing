################################################################################
## OPW Long-Term Water Level Data Downloader - Full Script with RDS Storage
##
## Purpose:
## - Downloads long-term water level data from OPW (waterlevel.ie)
## - Produces datasets at multiple time resolutions:
##     1. Raw 15-min
##     2. Interpolated 15-min series
##     3. Hourly
##     4. Daily 00–00
##     5. Daily 09–09
## - Computes proportion of missing values
## - Logs first/last timestamps, first regular 15-min block, and % missing
## - Saves all datasets as RDS and CSV
## - Generates interactive Plotly plot of past year for each gauge
##
## QC Codes:
## 31: Inspected water level, approved for general use
## 32: Corrected version of Code 31
## 41: Poor measured water level
## 42: Corrected version of Code 41
## 101: Unreliable water level
## 151: Unusable (dry/logging error)
## 254: Unchecked imported water level, provisional
## 255: Missing / no data available
##
## Requirements:
## - R packages: httr, jsonlite, dplyr, stringr, lubridate, zoo, tibble, plotly, htmlwidgets
################################################################################

rm(list = ls()); gc()
Sys.setenv(TZ = "GMT")

library(httr)
library(jsonlite)
library(dplyr)
library(stringr)
library(lubridate)
library(zoo)
library(tibble)
library(plotly)
library(htmlwidgets)

################################################################################
# USER SETTINGS
################################################################################
allowed_missing_hourly_pct <- 25   # % missing allowed per hour
allowed_missing_daily_pct  <- 5    # % missing allowed per day
plot_gauge_id <- 1041              # Optional: single gauge for plotting

################################################################################
# DERIVED THRESHOLDS
################################################################################
intervals_per_hour <- 4             # 15-min intervals per hour
intervals_per_day  <- 96            # 15-min intervals per day
max_missing_hourly <- floor(intervals_per_hour * allowed_missing_hourly_pct / 100)
max_missing_daily  <- floor(intervals_per_day * allowed_missing_daily_pct / 100)

################################################################################
# WORKING DIRECTORIES
################################################################################
wkdir <- file.path("C:", "Users", "CBroderick", "Downloads", "OPW")
if (!dir.exists(wkdir)) dir.create(wkdir, recursive = TRUE)
setwd(wkdir)

out_dir <- file.path(wkdir, paste0("WL_Output_", Sys.Date()))
dir.create(out_dir, showWarnings = FALSE)
dir.create(file.path(out_dir, "Gauges"), showWarnings = FALSE)
dir.create(file.path(out_dir, "log"), showWarnings = FALSE)

################################################################################
# GET GAUGE LIST
################################################################################
station_details_url <- 'https://waterlevel.ie/geojson/'
jfile <- fromJSON(content(GET(station_details_url), as = "text", encoding = "UTF-8"))

mystations <- tibble(
  id   = as.numeric(str_sub(jfile$features$properties$ref, -5, -1)),
  name = jfile$features$properties$name
)

gauge_lst <- mystations$id
gauge_lst_nme <- mystations$name

################################################################################
# INITIALISE LOG
################################################################################
log_tbl <- tibble(
  Gauge_ID = gauge_lst,
  Gauge_Name = gauge_lst_nme,
  LinkStatus_HD = NA,
  First_Valid_Timestamp = NA,
  First_Regular_15min_Timestamp = NA,
  Start_Date = NA,
  End_Date = NA,
  Prop_Missing_15min = NA,
  Prop_Missing_Hourly = NA,
  Prop_Missing_Daily_00_00 = NA,
  Prop_Missing_Daily_09_09 = NA,
  Data_Regularity = NA,
  Processed = NA
)

#gauge_lst=1041
################################################################################
# MAIN LOOP (DOWNLOAD + PROCESS)
################################################################################
for (val in seq_along(gauge_lst)) {
 # val=1
  gauge_sel <- gauge_lst[val]
  gauge_sel_adj <- str_pad(gauge_sel, 5, pad = "0")
  cat("Processing gauge:", gauge_sel, "\n")
  
  dest_dir <- file.path(out_dir, "Gauges", paste0("GaugeID_", gauge_sel))
  dir.create(dest_dir, showWarnings = FALSE)
  setwd(dest_dir)
  
  source <- paste0(
    "http://waterlevel.ie/hydro-data/data/internet/stations/0/",
    gauge_sel_adj, "/S/Waterlevel_complete.zip"
  )
  
  response <- GET(source)
  log_tbl$LinkStatus_HD[val] <- response$status_code
  if (response$status_code != 200) {
    log_tbl$Processed[val] <- "Download_Failed"
    next
  }
  
  tryCatch({
    # DOWNLOAD + READ RAW
    download.file(source, "Waterlevel_complete.zip", quiet = TRUE)
    unzip("Waterlevel_complete.zip")
    files <- list.files(pattern = "tsvalues")
    complete <- read.csv(files, sep = ";", skip = 10)
    file.remove(files)
    
    complete$Timestamp <- as_datetime(complete$X.Timestamp, tz="GMT")
    
    # QC Filtering
    qc_include <- c(31, 32, 41, 42, 101, 254)
    comp_raw <- complete %>%
      transmute(Timestamp = Timestamp, Level = Value, Quality = Quality.Code) %>%
      arrange(Timestamp) %>%
      filter(!is.na(Level), Quality %in% qc_include)
    
    # Log first valid timestamp
    log_tbl$First_Valid_Timestamp[val] <- min(comp_raw$Timestamp)
    
    # Identify first regular 15-min block
    dt_diff <- diff(as.numeric(comp_raw$Timestamp))/60
    is_15 <- dt_diff == 15
    rle_15 <- rle(is_15)
    idx_run <- which(rle_15$values == TRUE & rle_15$lengths >= 4)
    if (length(idx_run) > 0) {
      run_start <- sum(rle_15$lengths[seq_len(idx_run[1]-1)]) + 1
      log_tbl$First_Regular_15min_Timestamp[val] <- comp_raw$Timestamp[run_start]
      log_tbl$Data_Regularity[val] <- "Regular"
    } else {
      log_tbl$Data_Regularity[val] <- "Irregular"
    }
    
    # Start/End dates
    start_time <- min(comp_raw$Timestamp, na.rm=TRUE)
    end_time   <- max(comp_raw$Timestamp, na.rm=TRUE)
    log_tbl$Start_Date[val] <- start_time
    log_tbl$End_Date[val]   <- end_time
    
    # 15-min interpolation
    full_ts <- seq(start_time, end_time, by = "15 min")
    comp_15min <- tibble(Timestamp = full_ts) %>%
      left_join(comp_raw %>% select(Timestamp, Level), by = "Timestamp")
    comp_15min$Level <- zoo::na.approx(comp_15min$Level, x=comp_15min$Timestamp,
                                       maxgap=1, na.rm=FALSE)
    log_tbl$Prop_Missing_15min[val] <- mean(is.na(comp_15min$Level))
    
    # Hourly aggregation
    hourly <- comp_15min %>%
      mutate(Hour = floor_date(Timestamp, "hour")) %>%
      group_by(Hour) %>%
      summarise(
        n_total = n(), n_valid = sum(!is.na(Level)),
        Level = if_else((n_total - n_valid) <= max_missing_hourly,
                        mean(Level, na.rm=TRUE),
                        NA_real_)
      ) %>%
      ungroup()
    hourly$Level <- na.approx(hourly$Level, maxgap=3, na.rm=FALSE)
    log_tbl$Prop_Missing_Hourly[val] <- mean(is.na(hourly$Level))

    
    # Daily 00–00
    daily_00 <- comp_15min %>%
      mutate(Date = as.Date(Timestamp)) %>%
      group_by(Date) %>%
      summarise(
        n_missing = sum(is.na(Level)),
        Level = if_else(n_missing <= max_missing_daily, mean(Level, na.rm=TRUE), NA_real_)
      ) %>%
      ungroup()
    log_tbl$Prop_Missing_Daily_00_00[val] <- mean(is.na(daily_00$Level))

    
    # Daily 09–09
    daily_09 <- comp_15min %>%
      mutate(Date = as.Date(Timestamp - hours(9))) %>%
      group_by(Date) %>%
      summarise(
        n_missing = sum(is.na(Level)),
        Level = if_else(n_missing <= max_missing_daily, mean(Level, na.rm=TRUE), NA_real_)
      ) %>%
      ungroup()
    log_tbl$Prop_Missing_Daily_09_09[val] <- mean(is.na(daily_09$Level))
    
    
    saveRDS(daily_09, file.path(dest_dir, "Daily_Level_09_09.rds"))
    saveRDS(comp_raw, file.path(dest_dir, "Raw_Level.rds"))
    saveRDS(comp_15min, file.path(dest_dir, "Interp_15min_Level.rds"))
    saveRDS(hourly, file.path(dest_dir, "Hourly_Level.rds"))    
    saveRDS(daily_00, file.path(dest_dir, "Daily_Level_00_00.rds"))    
    
    
    log_tbl$Processed[val] <- TRUE
    
    ################################################################################
    # PLOTLY PLOT FOR PAST YEAR (15-min, hourly, daily 00-00, daily 09-09)
    ################################################################################
    start_plot <- end_time - 365*24*60*60  # last 365 days from last valid timestamp
    comp_raw_plot<- subset(tibble(comp_raw), Timestamp >= start_plot)
    reg15_plot <- subset(comp_15min, Timestamp >= start_plot)
    hourly_plot <- subset(hourly, Hour >= start_plot)
    daily00_plot <- subset(daily_00, Date >= start_plot)
    daily09_plot <- subset(daily_09, Date >= start_plot)
    
    fig <- plot_ly() %>%
      add_lines(x = comp_raw_plot$Timestamp, y = comp_raw_plot$Level, name = "15-min",
                line = list(color='grey', width=1.5)) %>%
      add_lines(x = reg15_plot$Timestamp, y = reg15_plot$Level, name = "15-min",
                line = list(color='blue', width=1.5)) %>%
      add_lines(x = hourly_plot$Hour, y = hourly_plot$Level, name = "Hourly",
                line = list(color='green', width=2)) %>%
      add_lines(x = daily00_plot$Date, y = daily00_plot$Level, name = "Daily 00-00",
                line = list(color='red', width=2, dash='dash')) %>%
      add_lines(x = daily09_plot$Date, y = daily09_plot$Level, name = "Daily 09-09",
                line = list(color='orange', width=2, dash='dot')) %>%
      layout(title = paste("Level - Gauge", gauge_sel),
             xaxis = list(title = "Date"),
             yaxis = list(title = "Level (m)"))
    
    html_file <- file.path(dest_dir, paste0("Gauge_", gauge_sel, "_Year_from_end_date.html"))
    htmlwidgets::saveWidget(fig, html_file)
    cat("Plot saved to:", html_file, "\n")
    
  }, error = function(e) {
    log_tbl$Processed[val] <- "Error"
  })
}

################################################################################
# SAVE LOG (with proper datetime format)
################################################################################
log_tbl_out <- log_tbl %>%
  mutate(
    First_Valid_Timestamp = format(as.POSIXct(First_Valid_Timestamp, tz="GMT"), "%Y-%m-%d %H:%M:%S"),
    First_Regular_15min_Timestamp = format(as.POSIXct(First_Regular_15min_Timestamp, tz="GMT"), "%Y-%m-%d %H:%M:%S"),
    Start_Date = format(as.POSIXct(Start_Date, tz="GMT"), "%Y-%m-%d %H:%M:%S"),
    End_Date = format(as.POSIXct(End_Date, tz="GMT"), "%Y-%m-%d %H:%M:%S")
  )

write.csv(log_tbl_out, file.path(out_dir, "log", "Log_Processed_LTermObs_Level.csv"), row.names=FALSE)
cat("Processing complete.\n")