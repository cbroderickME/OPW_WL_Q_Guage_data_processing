################################################################################
## BATCH DAILY QC COMPARISON PLOTS (RAW vs FILTERED + QC SUBPLOT)
################################################################################

rm(list=ls()); gc()
Sys.setenv(TZ="GMT")

library(data.table)
library(lubridate)
library(plotly)
library(zoo)
library(tibble)
library(htmlwidgets)

################################################################################
# BASE DIRECTORY
################################################################################

base_dir <- "C:/Users/CBroderick/Downloads/OPW/Data/WL_Output_2026-04-01/Gauges"

# Find all GaugeID folders
gauge_dirs <- list.dirs(base_dir, full.names = TRUE, recursive = FALSE)
gauge_dirs <- gauge_dirs[grepl("GaugeID_", basename(gauge_dirs))]

cat("Found", length(gauge_dirs), "gauges\n")

################################################################################
# QC CODE TABLE
################################################################################
# see: K:\Fluvial\Hydrometric_Network\OPW_Gauges\Dwnld_LongTerm_Hydo_OPW\QC_codes

################################################################################
# QC SETTINGS
################################################################################

exclude_qc <- c()

################################################################################
# LOOP THROUGH GAUGES
################################################################################
#gauge_dirs = gauge_dirs[1]
#gauge_dir = gauge_dirs[1]
for (gauge_dir in gauge_dirs) {
  
  tryCatch({
    
    gauge_id <- sub("GaugeID_", "", basename(gauge_dir))
    cat("Processing Gauge:", gauge_id, "\n")
    
    file_path <- file.path(gauge_dir, "Raw_water_level.rds")
    
    # Skip if file missing
    if (!file.exists(file_path)) {
      cat("  -> Missing file, skipping\n")
      next
    }
    
    ##############################################################################
    # LOAD DATA
    ##############################################################################
    
    comp_raw <- readRDS(file_path)
    dt <- as.data.table(comp_raw)
    dt[, Timestamp := as.POSIXct(Timestamp, tz="GMT")]
    dt[, Date := as.Date(Timestamp)]
    
    ##############################################################################
    # DAILY RAW
    ##############################################################################
    
    daily_raw <- dt[, .(
      Raw_water_level = mean(water_level, na.rm=TRUE),
      n_raw = .N
    ), by = Date]
    
    ##############################################################################
    # DAILY FILTERED
    ##############################################################################
    
    dt[, Use_Flag := fifelse(
      !is.na(water_level) & !Quality %in% exclude_qc,
      1L, 0L
    )]
    
    daily_filtered <- dt[Use_Flag == 1, .(
      Filtered_water_level = mean(water_level, na.rm=TRUE),
      n_filtered = .N
    ), by = Date]
    
    ##############################################################################
    # DOMINANT QC CODE
    ##############################################################################
    
    qc_daily <- dt[!is.na(Quality), .N, by = .(Date, Quality)]
    qc_daily <- qc_daily[order(Date, -N)]
    qc_dom <- qc_daily[, .SD[1], by = Date]
    setnames(qc_dom, "Quality", "Dominant_QC")
    
    ##############################################################################
    # MERGE ALL
    ##############################################################################
    
    daily_all <- Reduce(function(x,y) merge(x,y,by="Date",all=TRUE),
                        list(daily_raw, daily_filtered, qc_dom))
    daily_all[, Date := as.POSIXct(Date, tz="GMT")]
    
    ##############################################################################
    # PLOTS
    ##############################################################################
    
    p1 <- plot_ly(daily_all, x = ~Date) %>%
      add_lines(y = ~Raw_water_level, name = "Raw Daily Mean", line = list(color = "black"),
                customdata = ~Dominant_QC,
                hovertemplate = paste(
                  "Raw<br>%{x}<br>",
                  "water_level: %{y:.2f} m³/s<br>",
                  "Dominant QC: %{customdata}<extra></extra>"
                )) %>%
      add_lines(y = ~Filtered_water_level, name = "Filtered Daily Mean", line = list(color = "blue"),
                customdata = ~Dominant_QC,
                hovertemplate = paste(
                  "Filtered<br>%{x}<br>",
                  "water_level: %{y:.2f} m³/s<br>",
                  "Dominant QC: %{customdata}<extra></extra>"
                ))
    
    p2 <- plot_ly(daily_all, x = ~Date, y = ~Dominant_QC,
                  type = "scatter", mode = "markers+lines",
                  name = "Dominant QC Code",
                  marker = list(size = 6, color = "red"),
                  line = list(shape="hv"),
                  hovertemplate = "Date: %{x}<br>QC Code: %{y}<extra></extra>")
    
    final_plot <- subplot(p1, p2, nrows = 2, shareX = TRUE, heights = c(0.7, 0.3), titleY = TRUE) %>%
      layout(
        title = paste("Gauge", gauge_id, "- Daily 00:00 Comparison"),
        yaxis = list(title = "water_level (m)"),
        yaxis2 = list(title = "Dominant QC Code"),
        margin = list(t = 150),
        annotations = list(
          list(
            xref = "paper", yref = "paper",
            x = 0, y = 1.15,
            xanchor = "left", yanchor = "top",
            text = paste0("QC codes filtered: ", paste(exclude_qc, collapse = ", ")),
            showarrow = FALSE,
            align = "left",
            font = list(size = 12)
          )
        )
      )
    
    out_file <- file.path(gauge_dir, paste0("Gauge_", gauge_id, "_Daily_0000_QC_Comparison.html"))
    
    saveWidget(final_plot, out_file, selfcontained = TRUE,
               title = paste0("Gauge ", gauge_id, " Daily QC Comparison"))
    
    cat("  -> Saved\n")
    
  }, error = function(e) {
    cat("  -> Error processing Gauge:", basename(gauge_dir), ":", e$message, "\n")
    next
  })
  
}

cat("All gauges processed.\n")