################################################################################
## DAILY 00-00 QC COMPARISON PLOT (RAW vs FILTERED + QC SUBPLOT)
################################################################################

rm(list=ls()); gc()
Sys.setenv(TZ="GMT")

library(data.table)
library(lubridate)
library(plotly)
library(zoo)
library(tibble)


################################################################################
# QC CODE TABLE
################################################################################


################################################################################
# USER INPUT
################################################################################

gauge_id <- 1041   # <-- CHANGE
base_dir <- "K:/Fluvial/Hydrometric_Network/OPW_Gauges/Dwnld_LongTerm_Hydo_OPW/Data"

gauge_dir <- file.path(base_dir, paste0("GaugeID_", gauge_id))

################################################################################
# LOAD DATA
################################################################################

comp_raw <- readRDS(file.path(gauge_dir, "Raw_Discharge.rds"))

################################################################################
# PREP DATA
################################################################################

dt <- as.data.table(comp_raw)

# Ensure timestamp format
dt[, Timestamp := as.POSIXct(Timestamp, tz="GMT")]

# Add calendar day (00–00)
dt[, Date := as.Date(Timestamp)]

################################################################################
# DAILY RAW (NO QC FILTER)
################################################################################

daily_raw <- dt[, .(
  Raw_Discharge = mean(Discharge, na.rm=TRUE),
  n_raw = .N
), by = Date]

################################################################################
# DAILY FILTERED (QC APPLIED)
################################################################################

good_qc <- c(31,32,36,56,96,254)

dt[, Use_Flag := fifelse(
  !is.na(Discharge) & Quality %in% good_qc,
  1L, 0L
)]

daily_filtered <- dt[Use_Flag == 1, .(
  Filtered_Discharge = mean(Discharge, na.rm=TRUE),
  n_filtered = .N
), by = Date]

################################################################################
# DOMINANT QC CODE PER DAY
################################################################################

qc_daily <- dt[!is.na(Quality), .N, by = .(Date, Quality)]
qc_daily <- qc_daily[order(Date, -N)]

# Keep most frequent QC per day
qc_dom <- qc_daily[, .SD[1], by = Date]
setnames(qc_dom, "Quality", "Dominant_QC")

################################################################################
# MERGE ALL
################################################################################

daily_all <- Reduce(function(x,y) merge(x,y,by="Date",all=TRUE),
                    list(daily_raw, daily_filtered, qc_dom))

daily_all[, Date := as.POSIXct(Date, tz="GMT")]

################################################################################
# PLOT 1: DISCHARGE (WITH QC IN HOVER)
################################################################################

p1 <- plot_ly(daily_all, x = ~Date)

# Raw
p1 <- add_lines(p1,
                y = ~Raw_Discharge,
                name = "Raw Daily Mean",
                line = list(color = "black"),
                customdata = ~Dominant_QC,
                hovertemplate = paste(
                  "Raw<br>",
                  "%{x}<br>",
                  "Discharge: %{y:.2f} m³/s<br>",
                  "Dominant QC: %{customdata}",
                  "<extra></extra>"
                ))

# Filtered
p1 <- add_lines(p1,
                y = ~Filtered_Discharge,
                name = "Filtered Daily Mean",
                line = list(color = "blue"),
                customdata = ~Dominant_QC,
                hovertemplate = paste(
                  "Filtered<br>",
                  "%{x}<br>",
                  "Discharge: %{y:.2f} m³/s<br>",
                  "Dominant QC: %{customdata}",
                  "<extra></extra>"
                ))

################################################################################
# PLOT 2: DOMINANT QC CODE
################################################################################

p2 <- plot_ly(daily_all,
              x = ~Date,
              y = ~Dominant_QC,
              type = "scatter",
              mode = "markers+lines",
              name = "Dominant QC Code",
              marker = list(size = 6, color = "red"),
              line = list(shape="hv"),
              hovertemplate = "Date: %{x}<br>QC Code: %{y}<extra></extra>")

################################################################################
# COMBINE (LINKED SUBPLOTS)
################################################################################
################################################################################
# QC CODE MEANINGS
################################################################################

qc_meanings <- data.table(
  Code = c(31,32,36,56,96,254),
  Meaning = c(
    "Good measurement",
    "Checked",
    "Verified",
    "Adjusted",
    "Estimated",
    "Suspect / flagged"
  )
)

# Prepare text for annotation
qc_text <- paste(paste0("QC ", qc_meanings$Code, ": ", qc_meanings$Meaning), collapse = "<br>")

################################################################################
# COMBINE (LINKED SUBPLOTS) WITH QC ANNOTATION
################################################################################

final_plot <- subplot(p1, p2,
                      nrows = 2,
                      shareX = TRUE,
                      heights = c(0.7, 0.3),
                      titleY = TRUE) %>%
  layout(
    title = paste("Gauge", gauge_id, "- Daily 00:00 Comparison"),
    yaxis = list(title = "Discharge (m³/s)"),
    yaxis2 = list(title = "Dominant QC Code"),
    margin = list(t = 150),  # extra top margin for annotation
    annotations = list(
      list(
        xref = "paper", yref = "paper",
        x = 0, y = 1.15,  # position above the plot
        xanchor = "left", yanchor = "top",
        text = qc_text,
        showarrow = FALSE,
        align = "left",
        font = list(size = 12)
      )
    )
  )

################################################################################
# SAVE
################################################################################

htmlwidgets::saveWidget(
  final_plot,
  file.path(gauge_dir,
            paste0("Gauge_", gauge_id, "_Daily_0000_QC_Comparison.html")),
  selfcontained = FALSE
)

cat("Plot saved.\n")