#!/usr/bin/env Rscript
# ============================================================
# NIGERIA IM REPOSITORY BUILDER - FAST LONG/WIDE VERSION
# WITH SOCIAL MOBILIZATION INTEGRATION
# OPTIMIZED WITH DATA.TABLE (NO ROWWISE)
# ============================================================

library(data.table)
library(stringr)
library(lubridate)
library(qs)

# ============================================================
# SOCIAL MOBILIZATION MAPPING
# ============================================================
# Code to description mapping
sm_code_mapping <- c(
  "1" = "Traditional_Leader",
  "2" = "Town_Announcer",
  "3" = "Announcement_from_Mosque",
  "4" = "Radio",
  "5" = "Newspaper",
  "6" = "Poster_Leaflets",
  "7" = "Banner_Hoarding",
  "8" = "Relative_Neighbour_Friend",
  "9" = "Health_Worker",
  "10" = "VCM_UNICEF",
  "11" = "Rally_School_children",
  "12" = "Not_Aware",
  "13" = "Other"
)

# Create SM column names
sm_col_names <- paste0("sm_", tolower(gsub(" ", "_", gsub("-", "_", sm_code_mapping))))

# ============================================================
# 0) INPUT / OUTPUT
# ============================================================
rds_file <- "C:/Users/TOURE/Documents/PADACORD/IM/7178.rds"
out_file <- "C:/Users/TOURE/Mes documents/REPOSITORIES/IM_raw_data/IM_level/NIE_IM_JAN_2025.csv"

# ============================================================
# 1) READ RAW DATA (FAST)
# ============================================================
cat("=" %>% paste(rep("=", 60), collapse = ""), "\n")
cat("NIGERIA IM REPOSITORY BUILDER - OPTIMIZED VERSION\n")
cat("=" %>% paste(rep("=", 60), collapse = ""), "\n\n")

cat("Reading data...\n")
AB <- as.data.table(qread(rds_file))

cat("Initial rows:", nrow(AB), "\n")

# Clean state column
AB[, `:=`(
  Country = "NIE",
  states = trimws(as.character(states))
)]
AB[states %in% c("", "NA", "null", NA), states := NA]
AB <- AB[!is.na(states)]

cat("After cleaning states:", nrow(AB), "\n")

# ============================================================
# 2) BASE REPOSITORY PREP (FAST VECTORIZED)
# ============================================================
cat("\nPreparing base repository...\n")

# Filter valid dates
AC <- AB[!is.na(today) & !is.na(states)]

# Convert dates
AC[, `:=`(
  today = as.Date(today),
  DateMonitor = as.Date(DateMonitor),
  year = year(as.Date(today))
)]

# Filter year > 2019
AC <- AC[year > 2019]

cat("After date filtering:", nrow(AC), "\n")

# Get column groups
imm_cols <- grep("^Imm_Seen_house", names(AC), value = TRUE)
unimm_cols <- grep("^unimm_h", names(AC), value = TRUE)
sm_cols <- grep("^SourceInfo_house", names(AC), value = TRUE)

cat("Found", length(imm_cols), "Imm_Seen_house columns\n")
cat("Found", length(unimm_cols), "unimm_h columns\n")
cat("Found", length(sm_cols), "Social Mobilization columns\n")

# FIX: Convert columns to numeric before using rowSums
if (length(imm_cols) > 0) {
  # Convert each Imm_Seen_house column to numeric
  for (col in imm_cols) {
    set(AC, j = col, value = as.numeric(as.character(AC[[col]])))
    set(AC, which(is.na(AC[[col]])), col, 0)
  }
  AC[, u5_FM := rowSums(.SD, na.rm = TRUE), .SDcols = imm_cols]
} else {
  AC[, u5_FM := 0]
}

if (length(unimm_cols) > 0) {
  # Convert each unimm_h column to numeric
  for (col in unimm_cols) {
    set(AC, j = col, value = as.numeric(as.character(AC[[col]])))
    set(AC, which(is.na(AC[[col]])), col, 0)
  }
  AC[, missed_child := rowSums(.SD, na.rm = TRUE), .SDcols = unimm_cols]
} else {
  AC[, missed_child := 0]
}

AC[, u5_present := u5_FM + missed_child]
AC[, month := month(today)]

# Fix vaccine types (vectorized)
if ("vactype_other" %in% names(AC)) {
  AC[vactype_other %in% c("b0pv"), vactype_other := "bOPV"]
  AC[vactype_other %in% c("cmopv2"), vactype_other := "mOPV"]
  AC[vactype_other %in% c("fiPv", "fipv plus", "fIPV plus", "Fipv plus nopv2", "Fipv+nopv2"), vactype_other := "FIPV+nOPV2"]
  AC[vactype_other == "FIPV+NOPV2", vactype_other := "bOPV"]
  AC[vactype_other %in% c("Hpv"), vactype_other := "HPV"]
  AC[vactype_other %in% c("N opv", "N opv3"), vactype_other := "nOPV2"]
}

# Rename key columns for consistency
setnames(AC, "states", "Region", skip_absent = TRUE)
setnames(AC, "lgas", "District", skip_absent = TRUE)
setnames(AC, "today", "date", skip_absent = TRUE)

# ============================================================
# 3) OPTIMIZED SOCIAL MOBILIZATION PROCESSING (NO ROWWISE)
# ============================================================

if (length(sm_cols) > 0) {
  cat("\nProcessing Social Mobilization data (optimized)...\n")
  
  # Add row ID for tracking
  AC[, rowid := .I]
  
  # Melt all SM columns into long format (FAST)
  AC_melt <- melt(AC, id.vars = "rowid", measure.vars = sm_cols, 
                  value.name = "sm_value", na.rm = TRUE)
  
  # Extract codes from values using vectorized string extraction
  AC_melt[, code := str_extract(sm_value, "\\b([1-9]|1[0-3])\\b")]
  AC_melt <- AC_melt[!is.na(code)]
  AC_melt[, value := 1L]
  
  # Remove duplicates per rowid and code
  AC_melt <- unique(AC_melt, by = c("rowid", "code"))
  
  # Cast to wide format (one-hot encoding) - VERY FAST
  AC_wide <- dcast(AC_melt, rowid ~ code, value.var = "value", fill = 0, fun.aggregate = sum)
  
  # Rename columns to friendly names
  setnames(AC_wide, names(sm_code_mapping), sm_col_names, skip_absent = TRUE)
  
  # Merge back to original data
  AC <- merge(AC, AC_wide, by = "rowid", all.x = TRUE)
  
  # Fill NA with 0 for SM columns
  for (col in sm_col_names) {
    if (col %in% names(AC)) {
      set(AC, which(is.na(AC[[col]])), col, 0L)
    } else {
      set(AC, j = col, value = 0L)
    }
  }
  
  # Calculate awareness metrics (vectorized)
  not_aware_col <- sm_col_names[which(names(sm_code_mapping) == "12")]
  if (not_aware_col %in% names(AC)) {
    AC[, sm_total_sources := rowSums(.SD, na.rm = TRUE), .SDcols = sm_col_names]
    AC[, caregiver_aware := sm_total_sources > 0 & get(not_aware_col) == 0]
  } else {
    AC[, sm_total_sources := rowSums(.SD, na.rm = TRUE), .SDcols = sm_col_names]
    AC[, caregiver_aware := sm_total_sources > 0]
  }
  
  # Clean up temporary objects
  rm(AC_melt, AC_wide)
  gc()
  
  cat("SM processing complete. Created", length(sm_col_names), "awareness columns.\n")
  
} else {
  cat("\nNo social mobilization columns found.\n")
  # Create empty SM columns for consistency
  for (col in sm_col_names) {
    set(AC, j = col, value = 0L)
  }
  AC[, `:=`(sm_total_sources = 0, caregiver_aware = FALSE)]
}

# Remove rowid if it exists
if ("rowid" %in% names(AC)) {
  AC[, rowid := NULL]
}

# ============================================================
# 4) ADD ROUND NUMBER, VACCINE TYPE, RESPONSE
# ============================================================
cat("\nAdding round numbers and vaccine types...\n")

AC[, roundNumber := as.character(case_when(
  month == 1 ~ "Rnd1",
  month == 2 ~ "Rnd2",
  month == 3 ~ "Rnd3",
  month == 4 ~ "Rnd4",
  month == 5 ~ "Rnd5",
  month == 6 ~ "Rnd6",
  month == 7 ~ "Rnd7",
  month == 8 ~ "Rnd8",
  month == 9 ~ "Rnd9",
  month == 10 ~ "Rnd10",
  month == 11 ~ "Rnd11",
  month == 12 ~ "Rnd12",
  TRUE ~ NA_character_
))]

# Override specific rounds
AC[year == 2025 & month == 1, roundNumber := "Rnd1"]
AC[year == 2024 & month %in% c(11, 12), roundNumber := fifelse(month == 12, "Rnd5", "Rnd4")]
AC[year == 2024 & month %in% c(9, 10), roundNumber := "Rnd3"]
AC[year == 2024 & month %in% c(2, 3), roundNumber := "Rnd1"]
AC[year == 2024 & month %in% c(4, 5, 6), roundNumber := "Rnd2"]
AC[year == 2023 & month == 1, roundNumber := "Rnd1"]
AC[year == 2023 & month == 5, roundNumber := "Rnd1"]
AC[year == 2023 & month %in% c(6, 7, 8), roundNumber := "Rnd2"]
AC[year == 2023 & month %in% c(9, 10), roundNumber := "Rnd3"]
AC[year == 2023 & month == 11, roundNumber := "Rnd4"]
AC[year == 2023 & month == 12, roundNumber := "Rnd5"]

# Add Vaccine.type
AC[, Vaccine.type := as.character(case_when(
  year == 2025 & month == 1 ~ "nOPV2",
  year == 2024 & month %in% c(2, 3, 4, 5, 6, 9, 10, 11, 12) ~ "nOPV2",
  year == 2023 & month == 1 ~ "nOPV2",
  year == 2023 & month == 5 ~ "fIPV+nOPV2",
  year == 2023 & month == 7 ~ "fIPV+nOPV2",
  year == 2023 & month == 8 ~ "nOPV2",
  year == 2023 & month == 9 ~ "fIPV+nOPV2",
  year == 2023 & month %in% c(10, 11, 12) ~ "nOPV2",
  year == 2022 & month == 7 ~ "nOPV2",
  year == 2021 & month %in% c(3, 4, 5, 6, 7, 8, 9, 10) ~ "nOPV2",
  year == 2021 & month %in% c(11, 12) ~ "bOPV",
  year == 2022 & month %in% c(1, 2, 3, 4, 5, 8, 9, 10, 11, 12) ~ "bOPV",
  year == 2020 & month %in% c(1, 2, 3, 12) ~ "nOPV2",
  TRUE ~ as.character(vactype)
))]

# Add Response
AC[, Response := as.character(case_when(
  year == 2025 & month == 1 ~ "OBR1",
  year == 2024 & month %in% c(2, 3, 4, 5, 6, 8, 9, 10, 11, 12) ~ "NIE-2024-nOPV2",
  year == 2020 & month %in% c(1, 2, 3) ~ "NGA-20DS-01-2020",
  year == 2020 & month == 12 ~ "NGA-5DS-10-2020",
  year == 2021 & month == 1 ~ "NGA-5DS-10-2020",
  year == 2021 & month == 3 ~ "NGA-2021-013-1",
  year == 2021 & month == 4 ~ "NGA-2021-011-1",
  year == 2021 & month == 5 ~ "NGA-2021-011-1",
  year == 2021 & month == 6 ~ "NGA-2021-016-1",
  year == 2021 & month == 7 ~ "NGA-2021-016-1",
  year == 2021 & month == 8 ~ "NGA-2021-019",
  year == 2021 & month == 9 ~ "NGA-2021-020-4",
  year == 2021 & month == 10 ~ "NGA-2021-020-2",
  year == 2021 & month == 11 ~ "NGA-2021-020-3",
  year == 2022 & month == 7 ~ "Kwara Response",
  year == 2022 & month == 8 ~ "Kwara Response",
  year == 2023 & month %in% c(5, 6) ~ "NIE-2023-04-02_nOPV",
  year == 2023 & month %in% c(7, 10, 11, 12) ~ "NIE-2023-07-03_nOPV",
  TRUE ~ as.character(siatype)
))]

# Final Vaccine.type override based on Response
AC[str_detect(Response, "nOPV"), Vaccine.type := "nOPV2"]
AC[str_detect(Response, "bOPV"), Vaccine.type := "bOPV"]

# ============================================================
# 5) BASE REPOSITORY AGGREGATION (including SM columns)
# ============================================================
cat("\nAggregating to district level...\n")

# Select columns for aggregation
agg_cols <- c("Country", "Region", "District", "Response", "Vaccine.type", "roundNumber")

# Function to safely sum with NA handling
safe_sum <- function(x) {
  if (all(is.na(x))) return(0)
  return(sum(x, na.rm = TRUE))
}

# Aggregate
AK_base <- AC[, .(
  start_date = as.Date(min(date, na.rm = TRUE)),
  end_date = as.Date(max(date, na.rm = TRUE)),
  year = year(as.Date(min(date, na.rm = TRUE))),
  u5_present = safe_sum(u5_present),
  u5_FM = safe_sum(u5_FM),
  missed_child = safe_sum(missed_child),
  cv = round(safe_sum(u5_FM) / safe_sum(u5_present), 2),
  
  # SM aggregations
  HH_visited = .N,
  caregivers_informed = safe_sum(caregiver_aware),
  pct_informed = round(safe_sum(caregiver_aware) / .N * 100, 2),
  sm_total_mentions = safe_sum(sm_total_sources),
  
  # Individual SM sources
  sm_traditional_leader = safe_sum(get(sm_col_names[1])),
  sm_town_announcer = safe_sum(get(sm_col_names[2])),
  sm_mosque_announcement = safe_sum(get(sm_col_names[3])),
  sm_radio = safe_sum(get(sm_col_names[4])),
  sm_newspaper = safe_sum(get(sm_col_names[5])),
  sm_poster_leaflets = safe_sum(get(sm_col_names[6])),
  sm_banner_hoarding = safe_sum(get(sm_col_names[7])),
  sm_relative_neighbour = safe_sum(get(sm_col_names[8])),
  sm_health_worker = safe_sum(get(sm_col_names[9])),
  sm_vcm_unicef = safe_sum(get(sm_col_names[10])),
  sm_rally_school = safe_sum(get(sm_col_names[11])),
  sm_not_aware = safe_sum(get(sm_col_names[12])),
  sm_other = safe_sum(get(sm_col_names[13]))
), by = agg_cols]

# Replace infinite values with NA
AK_base[is.infinite(cv), cv := NA_real_]
AK_base[is.infinite(pct_informed), pct_informed := NA_real_]

# Remove any rows with all NA values in key columns
AK_base <- AK_base[!is.na(Region) & !is.na(District)]

# ============================================================
# 6) QUALITY CONTROL CHECKS
# ============================================================
cat("\n=== QUALITY CONTROL CHECKS ===\n")

cat("\nSocial Mobilization Summary:\n")
cat("  Caregivers informed: ", sum(AK_base$caregivers_informed, na.rm = TRUE), "\n")
cat("  HH visited: ", sum(AK_base$HH_visited, na.rm = TRUE), "\n")
cat("  % Informed: ", round(sum(AK_base$caregivers_informed, na.rm = TRUE) / sum(AK_base$HH_visited, na.rm = TRUE) * 100, 2), "%\n")
cat("  Top SM sources:\n")
cat("    Health Worker: ", sum(AK_base$sm_health_worker, na.rm = TRUE), "\n")
cat("    Radio: ", sum(AK_base$sm_radio, na.rm = TRUE), "\n")
cat("    Relative/Neighbour: ", sum(AK_base$sm_relative_neighbour, na.rm = TRUE), "\n")
cat("    Town Announcer: ", sum(AK_base$sm_town_announcer, na.rm = TRUE), "\n")
cat("    Traditional Leader: ", sum(AK_base$sm_traditional_leader, na.rm = TRUE), "\n")
cat("    VCM/UNICEF: ", sum(AK_base$sm_vcm_unicef, na.rm = TRUE), "\n")

# Check for any remaining issues
cat("\nData Quality Checks:\n")
cat("  Rows with missing Region: ", sum(is.na(AK_base$Region)), "\n")
cat("  Rows with missing District: ", sum(is.na(AK_base$District)), "\n")
cat("  Rows with missing Response: ", sum(is.na(AK_base$Response)), "\n")
cat("  Rows with cv > 1: ", sum(AK_base$cv > 1, na.rm = TRUE), "\n")

# ============================================================
# 7) EXPORT
# ============================================================
cat("\nSaving output...\n")
fwrite(AK_base, out_file)

cat("\n", paste(rep("=", 60), collapse = ""), "\n")
cat("PROCESSING COMPLETE!\n")
cat(paste(rep("=", 60), collapse = ""), "\n\n")

cat("📊 Summary:\n")
cat("  Total rows saved:", format(nrow(AK_base), big.mark = ","), "\n")
cat("  Total columns:", ncol(AK_base), "\n")
cat("  SM columns created:", sum(grepl("^sm_", names(AK_base))), "\n")

# File size
if (file.exists(out_file)) {
  file_size_mb <- round(file.size(out_file) / (1024 * 1024), 2)
  cat("  Output file size:", file_size_mb, "MB\n")
}
cat("  Output location:", out_file, "\n\n")

cat("Social Mobilization columns added:\n")
cat("  - caregiver_aware, sm_total_sources, sm_total_mentions\n")
cat("  - pct_informed, caregivers_informed, HH_visited\n")
cat("  - 13 individual source columns (sm_traditional_leader, sm_radio, etc.)\n")

cat("\n✅ Done!\n")

# Show first few rows
cat("\nFirst 5 rows (selected columns):\n")
preview_cols <- c("Country", "Region", "District", "Response", "roundNumber", 
                  "HH_visited", "caregivers_informed", "pct_informed", "cv")
preview_cols <- preview_cols[preview_cols %in% names(AK_base)]
print(head(AK_base[, ..preview_cols], 5))