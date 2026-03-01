################################################################################
## OPW Long-Term Hydrometric Data Downloader - Full Script with RDS Storage
##
## README / Overview
##
## Purpose:
## - Downloads long-term discharge (flow) data from OPW (waterlevel.ie)
## - Produces datasets at multiple time resolutions:
##     1. Raw
##     2. Regular 15-min intervals
##     3. Hourly
##     4. Daily 00–00
##     5. Daily 09–09
## - Computes proportion of missing values per dataset
## - Logs first regular 15-min block, start/end dates, and % missing values
## - Saves all datasets as RDS (faster than CSV)
## - Generates interactive Plotly plot of the past year for a selected gauge
##
## QC Codes:
## - Codes 31, 32, 36 represent varying levels of data quality (good/fair/poor)
## - Other codes indicate missing or unreliable data
##
## Requirements:
## - R packages: httr, jsonlite, dplyr, stringr, lubridate, zoo, tibble, plotly, htmlwidgets
##
## Usage:
## 1. Set working directory and output folders (wkdir)
## 2. Adjust settings:
##      - allowed_missing_hourly_pct: % missing allowed per hour
##      - allowed_missing_daily_pct: % missing allowed per day
##      - plot_gauge_id: gauge ID for interactive Plotly plot
## 3. Run the script. Outputs:
##      - RDS files for each time resolution
##      - Interactive plot HTML per gauge (15-min, hourly, daily 00-00, daily 09-09)
##      - Log CSV with processing details
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
# QC CODE TABLE
################################################################################
qc_codes <- tibble(
  Code = c(31, 32, 36, 46, 56, 96, 101, 151, 254, 255),
  Description = c(
    "Flow data estimated using a rating curve that it is considered to be of good quality and inspected water level data – Data may contain some error, but is considered to be of acceptable quality for general use",
    "As per Code 31, but using water level data of Code 32",
    "Flow data estimated using a rating curve that it is considered to be of fair quality and inspected or corrected water level data – Data may contain a fair degree of error and should therefore be treated with some caution",
    "Flow data estimated using a rating curve that it is considered to be of poor quality and inspected or corrected water level data – Data may contain a significant degree of error and should therefore be used for indicative purposes only",
    "Flow data estimated using an extrapolated rating curve and inspected or corrected water level data – Reliability unknown, should be treated with caution",
    "Flow data estimated using a provisional rating curve – data may be subject to revision following retrospective assessment of rating curves with the most recent flow measurements",
    "Flow data estimated using unreliable water level data – Data suspected erroneous and must only be used with caution",
    "Unusable data: Dry channel, logger malfunction",
    "Flow data estimated using unchecked water level data – Data is provisional only and must be used with caution",
    "Missing / no data available"
  )
)

################################################################################
# USER SETTINGS
################################################################################
allowed_missing_hourly_pct <- 25   # % missing allowed per hour (0-100)
allowed_missing_daily_pct  <- 5    # % missing allowed per day (0-100)

################################################################################
# DERIVED THRESHOLDS
################################################################################
intervals_per_hour <- 4
intervals_per_day  <- 96
max_missing_hourly <- floor(intervals_per_hour * allowed_missing_hourly_pct / 100)
max_missing_daily  <- floor(intervals_per_day * allowed_missing_daily_pct / 100)

################################################################################
# WORKING DIRECTORY
################################################################################
wkdir <- file.path("C:", "Users", "CBroderick", "Downloads", "OPW")
if (!dir.exists(wkdir)) dir.create(wkdir, recursive = TRUE)
setwd(wkdir)

out_dir <- file.path(wkdir, paste0("Q_Output_", Sys.Date()))
dir.create(out_dir, showWarnings = FALSE)
dir.create(file.path(out_dir, "Gauges"), showWarnings = FALSE)
dir.create(file.path(out_dir, "log"), showWarnings = FALSE)

################################################################################
# GET GAUGE LIST
################################################################################
station_details_url <- "https://waterlevel.ie/geojson/"
data_stations <- GET(station_details_url)
jfile <- fromJSON(content(data_stations, as = "text", encoding = "UTF-8"))

mystations <- tibble(
  id   = str_sub(jfile$features$properties$ref, -5, -1),
  name = jfile$features$properties$name
)

gauge_lst <- as.numeric(mystations$id)
gauge_lst_nme <- mystations$name

################################################################################
# INITIALISE LOG
################################################################################
log_tbl <- tibble(
  Gauge_ID = gauge_lst,
  Gauge_Name = gauge_lst_nme,
  gauge_process_LnkStatus_HD = NA,
  First_Regular_15min_Timestamp = NA,
  Start_Date = NA,
  End_Date = NA,
  Allowed_Missing_Hourly_pct = allowed_missing_hourly_pct,
  Allowed_Missing_Daily_pct  = allowed_missing_daily_pct,
  Prop_Missing_15min = NA,
  Prop_Missing_Hourly = NA,
  Prop_Missing_Daily_00_00 = NA,
  Prop_Missing_Daily_09_09 = NA,
  Processed = NA
)

################################################################################
# MAIN LOOP (DOWNLOAD + PROCESS)
################################################################################
#gauge_lst=1041

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
    gauge_sel_adj, "/Q/Discharge_complete.zip"
  )
  
  response <- httr::GET(source)
  log_tbl$gauge_process_LnkStatus_HD[val] <- response$status_code
  if (response$status_code != 200) {
    log_tbl$Processed[val] <- "Download_Failed"
    next
  }
  
  tryCatch({
    download.file(source, "Discharge_complete.zip", quiet = TRUE)
    unzip("Discharge_complete.zip")
    complete <- read.csv(list.files(pattern = "tsvalues"), sep = ";", skip = 10)
    complete$Timestamp <- as_datetime(complete$X.Timestamp, tz = "GMT")
    
    comp_raw <- complete %>%
      transmute(Timestamp = Timestamp, Discharge = Value, Quality = Quality.Code) %>%
      arrange(Timestamp) %>%
      filter(!is.na(Discharge), Quality %in% c(31, 32, 36, 96, 254))
    
    # Identify first regular 15-min block
    dt_diff <- diff(as.numeric(comp_raw$Timestamp)) / 60
    is_15 <- dt_diff == 15
    rle_15 <- rle(is_15)
    idx_run <- which(rle_15$values == TRUE & rle_15$lengths >= 4)
    if (length(idx_run) == 0) {
      log_tbl$Processed[val] <- "No_Regular_Block"
      next
    }
    run_start <- sum(rle_15$lengths[seq_len(idx_run[1] - 1)]) + 1
    log_tbl$First_Regular_15min_Timestamp[val] <- comp_raw$Timestamp[run_start]
    
    start_time <- min(comp_raw$Timestamp, na.rm = TRUE)
    end_time   <- max(comp_raw$Timestamp, na.rm = TRUE)
    log_tbl$Start_Date[val] <- start_time
    log_tbl$End_Date[val] <- end_time
    
    # 15-min full series
    full_ts <- seq(start_time, end_time, by = "15 min")
    comp_full <- tibble(Timestamp = full_ts) %>%
      left_join(comp_raw %>% select(Timestamp, Discharge), by = "Timestamp")
    comp_full$Discharge <- zoo::na.approx(comp_full$Discharge, x = comp_full$Timestamp,
                                          maxgap = 1, na.rm = FALSE)
    log_tbl$Prop_Missing_15min[val] <- mean(is.na(comp_full$Discharge))
    
    # Hourly aggregation
    hourly <- comp_full %>%
      mutate(Hour = floor_date(Timestamp, "hour")) %>%
      group_by(Hour) %>%
      summarise(
        n_total = n(), n_valid = sum(!is.na(Discharge)),
        Discharge = if_else((n_total - n_valid) <= max_missing_hourly,
                            mean(Discharge, na.rm = TRUE), NA_real_)
      ) %>% ungroup()
    log_tbl$Prop_Missing_Hourly[val] <- mean(is.na(hourly$Discharge))
    
    # Daily 00-00 aggregation
    daily_00 <- comp_full %>%
      mutate(Date = as.Date(Timestamp)) %>%
      group_by(Date) %>%
      summarise(
        n_total = n(), n_valid = sum(!is.na(Discharge)),
        Discharge = if_else((n_total - n_valid) <= max_missing_daily,
                            mean(Discharge, na.rm = TRUE), NA_real_)
      ) %>% ungroup()
    log_tbl$Prop_Missing_Daily_00_00[val] <- mean(is.na(daily_00$Discharge))
    
    # Daily 09-09 aggregation
    daily_09 <- comp_full %>%
      mutate(Date = as.Date(Timestamp - hours(9))) %>%
      group_by(Date) %>%
      summarise(
        n_total = n(), n_valid = sum(!is.na(Discharge)),
        Discharge = if_else((n_total - n_valid) <= max_missing_daily,
                            mean(Discharge, na.rm = TRUE), NA_real_)
      ) %>% ungroup()
    log_tbl$Prop_Missing_Daily_09_09[val] <- mean(is.na(daily_09$Discharge))
    
    # Save RDS
    saveRDS(comp_raw, file.path(dest_dir, "Raw_Discharge.rds"))
    saveRDS(comp_full, file.path(dest_dir, "Regular_15min_Discharge.rds"))
    saveRDS(hourly, file.path(dest_dir, "Hourly_Discharge.rds"))
    saveRDS(daily_00, file.path(dest_dir, "Daily_Discharge_00_00.rds"))
    saveRDS(daily_09, file.path(dest_dir, "Daily_Discharge_09_09.rds"))
    
    log_tbl$Processed[val] <- TRUE
    
    ################################################################################
    # PLOTLY PLOT FOR PAST YEAR (15-min, hourly, daily 00-00, daily 09-09)
    ################################################################################
    start_plot <- end_time - 365*24*60*60  # last 365 days from last valid timestamp
    comp_raw_plot<- subset(tibble(comp_raw), Timestamp >= start_plot)
    reg15_plot <- subset(comp_full, Timestamp >= start_plot)
    hourly_plot <- subset(hourly, Hour >= start_plot)
    daily00_plot <- subset(daily_00, Date >= start_plot)
    daily09_plot <- subset(daily_09, Date >= start_plot)
    
    fig <- plot_ly() %>%
      add_lines(x = comp_raw_plot$Timestamp, y = comp_raw_plot$Discharge, name = "15-min",
                line = list(color='grey', width=1.5)) %>%
      add_lines(x = reg15_plot$Timestamp, y = reg15_plot$Discharge, name = "15-min",
                line = list(color='blue', width=1.5)) %>%
      add_lines(x = hourly_plot$Hour, y = hourly_plot$Discharge, name = "Hourly",
                line = list(color='green', width=2)) %>%
      add_lines(x = daily00_plot$Date, y = daily00_plot$Discharge, name = "Daily 00-00",
                line = list(color='red', width=2, dash='dash')) %>%
      add_lines(x = daily09_plot$Date, y = daily09_plot$Discharge, name = "Daily 09-09",
                line = list(color='orange', width=2, dash='dot')) %>%
      layout(title = paste("Discharge - Gauge", gauge_sel),
             xaxis = list(title = "Date"),
             yaxis = list(title = "Discharge (m³/s)"))
    
    html_file <- file.path(dest_dir, paste0("Gauge_", gauge_sel, "_Year_from_end_date.html"))
    htmlwidgets::saveWidget(fig, html_file)
    cat("Plot saved to:", html_file, "\n")
    
  }, error = function(e) {
    log_tbl$Processed[val] <- "Error"
  })
}

################################################################################
# SAVE LOG (Proper Date Formatting)
################################################################################
log_tbl_out <- log_tbl %>%
  mutate(
    First_Regular_15min_Timestamp = if (!all(is.na(First_Regular_15min_Timestamp))) {
      format(as.POSIXct(First_Regular_15min_Timestamp, tz="GMT"), "%Y-%m-%d %H:%M:%S")
    } else { NA_character_ },
    Start_Date = if (!all(is.na(Start_Date))) {
      format(as.POSIXct(Start_Date, tz="GMT"), "%Y-%m-%d %H:%M:%S")
    } else { NA_character_ },
    End_Date = if (!all(is.na(End_Date))) {
      format(as.POSIXct(End_Date, tz="GMT"), "%Y-%m-%d %H:%M:%S")
    } else { NA_character_ }
  )

write.csv(log_tbl_out,
          file.path(out_dir, "log", "Log_Processed_LTermObs_Q.csv"),
          row.names = FALSE)

cat("Processing complete.\n")