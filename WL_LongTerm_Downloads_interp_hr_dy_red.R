################################################################################
## OPW Long-Term water_level Downloader – EPA-Style Streamlined (Optimized)
################################################################################

rm(list=ls()); gc()
Sys.setenv(TZ="GMT")

library(fasttime)
library(httr)
library(jsonlite)
library(data.table)
library(stringr)
library(lubridate)
library(zoo)
library(plotly)
library(htmlwidgets)
library(tibble)
library(dplyr)

################################################################################
# QC CODE TABLE
################################################################################
# see: K:\Fluvial\Hydrometric_Network\OPW_Gauges\Dwnld_LongTerm_Hydo_OPW\QC_codes

################################################################################
# QC SETTINGS
################################################################################

exclude_qc <- c(101, 151)

################################################################################
# USER SETTINGS
################################################################################
allowed_missing_hourly_pct <- 25
allowed_missing_daily_pct  <- 5
start_year <- 1990
plot_years <- 5

intervals_per_hour <- 4
intervals_per_day  <- 96
max_missing_hourly <- floor(intervals_per_hour * allowed_missing_hourly_pct / 100)
max_missing_daily  <- floor(intervals_per_day * allowed_missing_daily_pct / 100)

wkdir <- "C:/Users/CBroderick/Downloads/OPW/Data"
if(!dir.exists(wkdir)) dir.create(wkdir, recursive=TRUE)

out_dir <- file.path(wkdir, paste0("WL_Output_", Sys.Date()-1))
dir.create(out_dir, showWarnings=FALSE)
dir.create(file.path(out_dir,"Gauges"), showWarnings=FALSE)
dir.create(file.path(out_dir,"log"), showWarnings=FALSE)

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
  First_Valid_Raw_Timestamp = NA,
  First_Regular_15min_Timestamp = NA,
  Start_Date = NA,
  End_Date = NA,
  Allowed_Missing_Hourly_pct = allowed_missing_hourly_pct,
  Allowed_Missing_Daily_pct  = allowed_missing_daily_pct,
  Prop_Missing_15min = NA,
  Prop_Missing_Hourly = NA,
  Prop_Missing_Daily_09_09 = NA,
  Processed = NA
)

################################################################################
# QC AUDIT COLLECTOR
################################################################################
qc_audit <- tibble(
  Gauge_ID = character(),
  QC_Code = character(),
  Count = numeric()
)

#gauge_lst=3055
################################################################################
# MAIN LOOP
################################################################################
for(val in seq_along(gauge_lst)) {
  
  #val=1
  gauge_sel <- gauge_lst[val]
  gauge_sel_adj <- str_pad(gauge_sel, 5, pad="0")
  cat("Processing gauge:", gauge_sel, "\n")
  
  dest_dir <- file.path(out_dir, "Gauges", paste0("GaugeID_", gauge_sel))
  dir.create(dest_dir, showWarnings=FALSE)
  
  setwd(dest_dir)
  source <- paste0(
    "http://waterlevel.ie/hydro-data/data/internet/stations/0/",
    gauge_sel_adj, "/S/Waterlevel_complete.zip"
  )
  
  
  response <- httr::GET(source)
  log_tbl$gauge_process_LnkStatus_HD[val] <- response$status_code
  if(response$status_code != 200){
    log_tbl$Processed[val] <- "Download_Failed"
    next
  }
  
  tryCatch({
    # DOWNLOAD + UNZIP
    download.file(source, "Waterlevel_complete.zip", quiet = TRUE)
    unzip("Waterlevel_complete.zip")
    csv_file <- list.files(pattern="tsvalues", full.names = TRUE)
    
    # READ CSV
    complete <- fread(csv_file, skip=10)
    setnames(complete, old = names(complete), new = c("Timestamp", "Value", "Quality.Code"))
    complete[, Timestamp := fastPOSIXct(Timestamp, tz="GMT")]
    
    ################################################################################
    # QC AUDIT
    ################################################################################
    qc_counts <- complete[, .N, by = Quality.Code][, .(
      Gauge_ID = as.character(gauge_sel),
      QC_Code = as.character(Quality.Code),
      Count = N
    )]
    qc_audit <- rbind(qc_audit, qc_counts)
    
    ################################################################################
    # PROCESSING FILTER (NON-DESTRUCTIVE)
    ################################################################################
    comp_raw <- complete[, .(
      Timestamp,
      water_level = Value,
      Quality = Quality.Code
    )]
    
    # Flag usable data
    comp_raw[, Use_Flag := fifelse(
      !is.na(water_level) &
        !Quality %in% exclude_qc &
        year(Timestamp) >= start_year,
      1L, 0L
    )]
    
    dt <- comp_raw[Use_Flag==1, .(Timestamp, water_level, Quality)]
    
    if(nrow(dt)==0){
      log_tbl$Processed[val] <- "No_Data_1990_onwards"
      next
    }
    
    ###############################################################################
    # FIRST VALID RAW TIMESTAMP
    ###############################################################################
    first_valid_raw <- min(dt$Timestamp)
    log_tbl$First_Valid_Raw_Timestamp[val] <- first_valid_raw
    
    ###############################################################################
    # FIRST 24 HOURS OF REGULAR 15-MIN SAMPLING
    ###############################################################################
    ts_sorted <- sort(unique(dt$Timestamp))
    dt_diff <- as.numeric(diff(ts_sorted), units = "mins")
    is_15min <- dt_diff == 15
    required_intervals <- 96
    rle_15 <- rle(is_15min)
    run_positions <- which(rle_15$values & rle_15$lengths >= required_intervals)
    
    if(length(run_positions) > 0){
      cumulative_lengths <- cumsum(rle_15$lengths)
      run_start_index <- if(run_positions[1] == 1) 1 else cumulative_lengths[run_positions[1]-1]+1
      first_regular <- ts_sorted[run_start_index]
    } else first_regular <- NA
    
    log_tbl$First_Regular_15min_Timestamp[val] <- first_regular
    log_tbl$Start_Date[val] <- min(dt$Timestamp)
    log_tbl$End_Date[val] <- max(dt$Timestamp)
    
    ################################################################################
    # 1️⃣ REGULAR 15-MIN INTERPOLATION
    ################################################################################
    start_time <- floor_date(min(dt$Timestamp), "15 minutes")
    end_time   <- ceiling_date(max(dt$Timestamp), "15 minutes")
    full_ts <- seq(start_time, end_time, by="15 min")
    
    dt_full <- data.table(Timestamp=full_ts)
    setkey(dt_full, Timestamp)
    setkey(dt, Timestamp)
    dt_full <- dt[dt_full, on="Timestamp"]
    dt_full[, water_level := zoo::na.approx(water_level, x=Timestamp, maxgap=1, na.rm=FALSE)]
    log_tbl$Prop_Missing_15min[val] <- mean(is.na(dt_full$water_level))
    
    ################################################################################
    # 2️⃣ HOURLY AGGREGATION
    ################################################################################
    dt_full[, Hour := floor_date(Timestamp, "hour")]
    hourly_dt <- dt_full[, .(
      n_total = .N,
      n_valid = sum(!is.na(water_level)),
      water_level = if(.N - sum(!is.na(water_level)) <= max_missing_hourly) mean(water_level, na.rm=TRUE) else NA_real_
    ), by=Hour]
    
    full_hours <- seq(floor_date(start_time, "hour"), ceiling_date(end_time, "hour"), by="hour")
    hourly_dt <- data.table(Hour = full_hours)[hourly_dt, on="Hour"]
    log_tbl$Prop_Missing_Hourly[val] <- mean(is.na(hourly_dt$water_level))
    
    ################################################################################
    # 3️⃣ DAILY 09-09 AGGREGATION
    ################################################################################
    dt_full[, Date_09 := as.Date(Timestamp - hours(9))]
    daily_dt <- dt_full[, .(
      n_total = .N,
      n_valid = sum(!is.na(water_level)),
      water_level = if(.N - sum(!is.na(water_level)) <= max_missing_daily) mean(water_level, na.rm=TRUE) else NA_real_
    ), by=Date_09]
    
    full_days <- seq(as.Date(floor_date(start_time - hours(9),"day")),
                     as.Date(ceiling_date(end_time - hours(9),"day")),
                     by="day")
    daily_dt <- data.table(Date_09 = full_days)[daily_dt, on="Date_09"]
    log_tbl$Prop_Missing_Daily_09_09[val] <- mean(is.na(daily_dt$water_level))
    
    ################################################################################
    # SAVE RDS
    ################################################################################
    saveRDS(comp_raw, file.path(dest_dir,"Raw_water_level.rds"))
    saveRDS(dt_full, file.path(dest_dir,"Regular_15min_water_level.rds"))
    saveRDS(hourly_dt, file.path(dest_dir,"Hourly_water_level.rds"))
    saveRDS(daily_dt, file.path(dest_dir,"Daily_water_level_09_09.rds"))
    
    log_tbl$Processed[val] <- TRUE
    
    ################################################################################
    # MULTI-RESOLUTION INTERACTIVE PLOT
    ################################################################################
    # ---- Prepare raw plot with QC code ----
    raw_plot <- data.frame(
      Timestamp = comp_raw$Timestamp,
      water_level = comp_raw$water_level,
      Quality   = comp_raw$Quality,
      Type      = "Raw (Unfiltered)"
    )
    
    # ---- Prepare other datasets with same columns ----
    reg15_plot <- data.frame(
      Timestamp = dt_full$Timestamp,
      water_level = dt_full$water_level,
      Quality   = as.integer(NA),
      Type      = "Regular 15-min"
    )
    
    hourly_plot <- data.frame(
      Timestamp = hourly_dt$Hour,
      water_level = hourly_dt$water_level,
      Quality   = as.integer(NA),
      Type      = "Hourly"
    )
    
    daily_plot <- data.frame(
      Timestamp = daily_dt$Date_09,
      water_level = daily_dt$water_level,
      Quality   = as.integer(NA),
      Type      = "Daily (09-09)"
    )
    
    # ---- Ensure Timestamps are POSIXct ----
    raw_plot$Timestamp    <- as.POSIXct(raw_plot$Timestamp, tz="GMT")
    reg15_plot$Timestamp  <- as.POSIXct(reg15_plot$Timestamp, tz="GMT")
    hourly_plot$Timestamp <- as.POSIXct(hourly_plot$Timestamp, tz="GMT")
    daily_plot$Timestamp  <- as.POSIXct(daily_plot$Timestamp, tz="GMT")
    
    # ---- Combine safely ----
    all_plot <- rbind(
      raw_plot,
      reg15_plot,
      hourly_plot,
      daily_plot
    )
    
    # ---- Restrict to last N years ----
    all_plot <- all_plot[all_plot$Timestamp >= max(all_plot$Timestamp, na.rm=TRUE)-years(plot_years), ]
    
    # ---- Plot with QC hover for raw only ----
    p <- plot_ly()
    
    # Raw (Unfiltered) with QC code in hover
    p <- add_trace(p,
                   data = subset(all_plot, Type=="Raw (Unfiltered)"),
                   x = ~Timestamp,
                   y = ~water_level,
                   type = "scattergl",
                   mode = "markers",
                   marker = list(size=3, color="black"),
                   name = "Raw (Unfiltered)",
                   hovertemplate="Date: %{x}<br>water_level: %{y:.2f} m³/s<br>QC Code: %{customdata}<extra></extra>",
                   customdata = ~Quality)
    
    # Regular 15-min
    p <- add_trace(p,
                   data = subset(all_plot, Type=="Regular 15-min"),
                   x = ~Timestamp,
                   y = ~water_level,
                   type = "scattergl",
                   mode = "lines",
                   line = list(color="green", width=1),
                   name = "Regular 15-min",
                   hovertemplate="15-min<br>%{x}<br>%{y:.2f} m³/s<extra></extra>")
    
    # Hourly
    p <- add_trace(p,
                   data = subset(all_plot, Type=="Hourly"),
                   x = ~Timestamp,
                   y = ~water_level,
                   type = "scattergl",
                   mode = "lines",
                   line = list(color="purple", width=2),
                   name = "Hourly",
                   hovertemplate="Hourly<br>%{x}<br>%{y:.2f} m³/s<extra></extra>")
    
    # Daily
    p <- add_trace(p,
                   data = subset(all_plot, Type=="Daily (09-09)"),
                   x = ~Timestamp,
                   y = ~water_level,
                   type = "scattergl",
                   mode = "lines+markers",
                   line = list(color="blue", width=3),
                   marker = list(size=5),
                   name = "Daily (09-09)",
                   hovertemplate="Daily 09-09<br>%{x}<br>%{y:.2f} m³/s<extra></extra>")
    
    saveWidget(p, file.path(dest_dir,
                            paste0("Gauge_",gauge_sel,
                                   "_All_Resolutions_Last",plot_years,
                                   "Years_Interactive.html")),
               selfcontained=FALSE)
    
  }, error=function(e){
    log_tbl$Processed[val] <- "Error"
  })
}

################################################################################
# SAVE LOG
################################################################################
log_tbl_out <- log_tbl %>%
  mutate(
    First_Regular_15min_Timestamp = if (!all(is.na(First_Regular_15min_Timestamp))) {
      format(as.POSIXct(First_Regular_15min_Timestamp, tz="GMT"), "%Y-%m-%d %H:%M:%S")
    } else { NA_character_ },
    First_Valid_Raw_Timestamp = if (!all(is.na(First_Valid_Raw_Timestamp))) {
      format(as.POSIXct(First_Valid_Raw_Timestamp, tz="GMT"), "%Y-%m-%d %H:%M:%S")
    } else { NA_character_ },
    Start_Date = if (!all(is.na(Start_Date))) {
      format(as.POSIXct(Start_Date, tz="GMT"), "%Y-%m-%d %H:%M:%S")
    } else { NA_character_ },
    End_Date = if (!all(is.na(End_Date))) {
      format(as.POSIXct(End_Date, tz="GMT"), "%Y-%m-%d %H:%M:%S")
    } else { NA_character_ }
  )

write.csv(log_tbl_out, file.path(out_dir,"log","Log_Processed_LTermObs_Q.csv"), row.names=FALSE)

################################################################################
# QC AUDIT SUMMARY
################################################################################
qc_summary <- qc_audit %>%
  group_by(QC_Code) %>%
  summarise(Total_Count=sum(Count),
            Gauges_Found_In=n_distinct(Gauge_ID),
            Gauge_List=paste(unique(Gauge_ID),collapse=", "),
            .groups="drop") %>%
  arrange(desc(Total_Count))

write.csv(qc_summary, file.path(out_dir,"log","QC_Audit_Summary_All_Gauges.csv"), row.names=FALSE)

cat("Processing complete.\n")