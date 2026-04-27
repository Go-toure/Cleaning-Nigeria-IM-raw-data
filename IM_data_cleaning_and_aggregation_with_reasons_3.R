#!/usr/bin/env Rscript
# ============================================================
# NIGERIA IM REPOSITORY BUILDER - WITH SOCIAL MOBILIZATION
# INTEGRATES REASON CLASSIFICATION + SM SOURCES
# ============================================================

library(tidyverse)
library(data.table)
library(stringr)
library(lubridate)
library(qs)

# ============================================================
# SOCIAL MOBILIZATION MAPPING
# ============================================================
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
# 1) READ RAW DATA
# ============================================================
cat("=" %>% paste(rep("=", 60), collapse = ""), "\n")
cat("NIGERIA IM REPOSITORY BUILDER - WITH SOCIAL MOBILIZATION\n")
cat("=" %>% paste(rep("=", 60), collapse = ""), "\n\n")

cat("Reading data...\n")
AB <- qread(rds_file) |>
  mutate(
    Country = "NIE",
    states = trimws(as.character(states)),
    states = na_if(states, ""),
    states = na_if(states, "NA"),
    states = na_if(states, "null")
  ) |>
  filter(!is.na(states))

cat("Initial rows:", nrow(AB), "\n")

# ============================================================
# 2) HELPER FUNCTIONS
# ============================================================
normalize_reason_text <- function(x) {
  x <- tolower(trimws(as.character(x)))
  x[x %in% c("", "na", "nan", "null")] <- NA_character_
  x <- iconv(x, from = "", to = "ASCII//TRANSLIT", sub = "")
  x <- gsub("[\r\n\t]+", " ", x)
  x <- gsub("[[:punct:]]+", " ", x)
  x <- gsub("\\s+", " ", x)
  x <- trimws(x)
  x
}

detect_main_reason <- function(x) {
  x0 <- normalize_reason_text(x)
  
  case_when(
    is.na(x0) | x0 == "" ~ NA_character_,
    str_detect(x0, "new born|newborn|new birth|newly born|zero dose|just gave birth|given birth|was born yesterday|child was born yesterday|delivered on|baby has four days|baby was just born") ~ "r_childnotborn",
    str_detect(x0, "security|security related issues") ~ "r_security",
    str_detect(x0, "finger mark|finger marked|not finger marked|no mark on left finger|vaccinated but not|immunized but no|finger marking erased|cleaned off|wrongly finger marked") ~ "r_vaccinated_but_not_FM",
    str_detect(x0, "team not visited|household not visited|house not visited|not revisited|never revisited|no revisit|team omitted|did not make effort|didn t make effort|team failed to check|missed by the team") ~ "r_house_not_visited",
    str_detect(x0, "asleep|sleeping|was sleeping|child sleeping|child was sleep|child was asleep|was slept|at sleep|child is sleeping|mother was sleeping") ~ "r_child_was_asleep",
    str_detect(x0, "visitor|visitors|came for visiting|came for a visit|just arrived|just came|from outside|holiday|omugwo|from another state|visiting child|visiting parent") ~ "r_child_is_a_visitor",
    str_detect(x0, "non compliance|refusal|refused|rejection|does not want|do not allow|did not allow|father refused|not interested|religious|traditional|cultural|no felt need|polio can be cured|polio has been eradicated|too many rounds|no caregiver consent|scared|afraid|vaccine.*safe") ~ "r_non_compliance",
    str_detect(x0, "absent|not around|not home|not at home|not present|not available|not found|wasn t present|went out|out of house|market|farm|school|play ground|travel|travelled|church|wedding|burial|meeting|river|work|office") ~ "r_childabsent",
    TRUE ~ "other_r"
  )
}

detect_abs_reason <- function(x) {
  x0 <- normalize_reason_text(x)
  
  case_when(
    is.na(x0) | x0 == "" ~ NA_character_,
    str_detect(x0, "travel|travelled|travelling|traveling|journey|transit|out of town|other state|outside the lga|trip|travel back|returned from journey") ~ "abs_reason_travelled",
    str_detect(x0, "farm|farming|bush|firewood") ~ "abs_reason_farm",
    str_detect(x0, "market|shop") ~ "abs_reason_market",
    str_detect(x0, "school|schools|shool|sch|islamiya|modiraza|qur an school") ~ "abs_reason_school",
    str_detect(x0, "play ground|playground|playing ground|play graunt|play grouwn|play grand|play groud|went to play|he was playing|play away") ~ "abs_reason_in_playground",
    TRUE ~ "abs_reason_other"
  )
}

detect_nc_reason <- function(x) {
  x0 <- normalize_reason_text(x)
  
  case_when(
    is.na(x0) | x0 == "" ~ NA_character_,
    str_detect(x0, "religious|cultural|traditional|announcement from mosque|religious beliefs") ~ "nc_reason_religious_cultural",
    str_detect(x0, "polio can be cured|polio free|polio has been eradicated|poliofree") ~ "nc_reason_poliofree",
    str_detect(x0, "vaccine.*safe|vaccines.*safe|afraid|scared|negative way|reacted on child|interaction|safety") ~ "nc_reason_vaccines_safety",
    str_detect(x0, "no felt need|no need|no perceived need") ~ "nc_reason_no_felt_need",
    str_detect(x0, "too many rounds|too many round|too many rnd") ~ "nc_reason_too_many_rnd",
    str_detect(x0, "father refused|father did not allow|does not want|do not allow|no caregiver consent|no care giver consent|no parental consent|caregiver refusal|mother requested not to give|refusal|refused|rejection") ~ "nc_reason_no_care_giver_consent",
    str_detect(x0, "child is sick|child was sick|child sick|child was ill|not healthy|unwell|hospital|admitted|not well|seriously sick") ~ "nc_reason_child_sick",
    str_detect(x0, "covid") ~ "nc_reason_covid_19",
    str_detect(x0, "nopv|n opv") ~ "nc_reason_nopvconcern",
    TRUE ~ "nc_reason_others"
  )
}

# ============================================================
# 3) BASE REPOSITORY PREP
# ============================================================
cat("\nPreparing base repository...\n")

AC <- AB |>
  filter(!is.na(today), !is.na(states)) |>
  mutate(
    today = as.Date(today),
    DateMonitor = as.Date(DateMonitor),
    across(starts_with("Imm_Seen_house"), as.numeric),
    across(starts_with("unimm_h"), as.numeric),
    year = as.numeric(year(today))
  ) |>
  filter(year > 2019) |>
  select(
    Country,
    Region = states,
    District = lgas,
    date = today,
    siatype,
    vactype,
    vactype_other,
    starts_with("Imm_Seen_house"),
    starts_with("unimm_h"),
    starts_with("SourceInfo_house"),  # Include SM columns
    everything()
  )

AD <- AC |>
  mutate(
    u5_FM = rowSums(across(starts_with("Imm_Seen_house")), na.rm = TRUE),
    missed_child = rowSums(across(starts_with("unimm_h")), na.rm = TRUE),
    u5_present = u5_FM + missed_child,
    month = month(date)
  )

# Fix vaccine types
AD$vactype_other[AD$vactype_other == "b0pv"] <- "bOPV"
AD$vactype_other[AD$vactype_other == "cmopv2"] <- "mOPV"
AD$vactype_other[AD$vactype_other == "fiPv"] <- "FIPV+nOPV2"
AD$vactype_other[AD$vactype_other == "fipv plus"] <- "FIPV+nOPV2"
AD$vactype_other[AD$vactype_other == "fIPV plus"] <- "FIPV+nOPV2"
AD$vactype_other[AD$vactype_other == "Fipv plus nopv2"] <- "FIPV+nOPV2"
AD$vactype_other[AD$vactype_other == "Fipv+nopv2"] <- "FIPV+nOPV2"
AD$vactype_other[AD$vactype_other == "FIPV+NOPV2"] <- "bOPV"
AD$vactype_other[AD$vactype_other == "Hpv"] <- "HPV"
AD$vactype_other[AD$vactype_other == "N opv"] <- "nOPV2"
AD$vactype_other[AD$vactype_other == "N opv3"] <- "nOPV2"

# ============================================================
# 4) SOCIAL MOBILIZATION PROCESSING (OPTIMIZED)
# ============================================================

# Identify SM columns
sm_cols <- grep("^SourceInfo_house", names(AD), value = TRUE)

if (length(sm_cols) > 0) {
  cat("\nProcessing Social Mobilization data...\n")
  
  # Convert to data.table for faster processing
  setDT(AD)
  
  # Add row ID
  AD[, rowid := .I]
  
  # Melt SM columns to long format
  AD_melt <- melt(AD, id.vars = "rowid", measure.vars = sm_cols, 
                  value.name = "sm_value", na.rm = TRUE)
  
  # Extract codes
  AD_melt[, code := str_extract(sm_value, "\\b([1-9]|1[0-3])\\b")]
  AD_melt <- AD_melt[!is.na(code)]
  AD_melt[, value := 1L]
  
  # Remove duplicates
  AD_melt <- unique(AD_melt, by = c("rowid", "code"))
  
  # Cast to wide format
  AD_wide <- dcast(AD_melt, rowid ~ code, value.var = "value", fill = 0, fun.aggregate = sum)
  
  # Rename columns
  setnames(AD_wide, names(sm_code_mapping), sm_col_names, skip_absent = TRUE)
  
  # Merge back
  AD <- merge(AD, AD_wide, by = "rowid", all.x = TRUE)
  
  # Fill NA with 0
  for (col in sm_col_names) {
    if (col %in% names(AD)) {
      set(AD, which(is.na(AD[[col]])), col, 0L)
    } else {
      set(AD, j = col, value = 0L)
    }
  }
  
  # Calculate awareness metrics
  not_aware_col <- sm_col_names[which(names(sm_code_mapping) == "12")]
  AD[, sm_total_sources := rowSums(.SD, na.rm = TRUE), .SDcols = sm_col_names]
  AD[, caregiver_aware := sm_total_sources > 0 & get(not_aware_col) == 0]
  
  # Convert back to tibble for rest of processing
  AD <- as_tibble(AD)
  
  # Clean up
  rm(AD_melt, AD_wide)
  gc()
  
  cat("SM processing complete. Created", length(sm_col_names), "awareness columns.\n")
  
} else {
  cat("\nNo social mobilization columns found.\n")
  # Create empty SM columns
  for (col in sm_col_names) {
    AD[[col]] <- 0L
  }
  AD$sm_total_sources <- 0
  AD$caregiver_aware <- FALSE
}

# ============================================================
# 5) ADD ROUND NUMBER, VACCINE TYPE, RESPONSE
# ============================================================
cat("\nAdding round numbers and vaccine types...\n")

AE <- AD |>
  mutate(
    roundNumber = case_when(
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
      month == 12 ~ "Rnd12"
    )
  ) |>
  mutate(
    roundNumber = case_when(
      year == 2025 & month == 1 ~ "Rnd1",
      year == 2024 & month == 12 ~ "Rnd5",
      year == 2024 & month == 11 ~ "Rnd4",
      year == 2024 & month == 10 ~ "Rnd3",
      year == 2024 & month == 9 ~ "Rnd3",
      year == 2024 & month == 2 ~ "Rnd1",
      year == 2024 & month == 3 ~ "Rnd1",
      year == 2024 & month == 4 ~ "Rnd2",
      year == 2024 & month == 5 ~ "Rnd2",
      year == 2024 & month == 6 ~ "Rnd2",
      year == 2023 & month == 1 ~ "Rnd1",
      year == 2023 & month == 5 ~ "Rnd1",
      year == 2023 & month == 7 ~ "Rnd2",
      year == 2023 & month == 8 ~ "Rnd2",
      year == 2023 & month == 9 ~ "Rnd3",
      year == 2023 & month == 10 ~ "Rnd3",
      year == 2023 & month == 11 ~ "Rnd4",
      year == 2023 & month == 12 ~ "Rnd5",
      year == 2023 & month == 6 ~ "Rnd2",
      TRUE ~ roundNumber
    )
  ) |>
  mutate(
    Vaccine.type = case_when(
      year == 2025 & month == 1 ~ "nOPV2",
      year == 2024 & month == 12 ~ "nOPV2",
      year == 2024 & month == 11 ~ "nOPV2",
      year == 2024 & month == 10 ~ "nOPV2",
      year == 2024 & month == 9 ~ "nOPV2",
      year == 2024 & month == 3 ~ "nOPV2",
      year == 2024 & month == 2 ~ "nOPV2",
      year == 2024 & month == 4 ~ "nOPV2",
      year == 2023 & month == 1 ~ "nOPV2",
      year == 2023 & month == 5 ~ "fIPV+nOPV2",
      year == 2023 & month == 7 ~ "fIPV+nOPV2",
      year == 2023 & month == 8 ~ "nOPV2",
      year == 2023 & month == 9 ~ "fIPV+nOPV2",
      year == 2023 & month == 10 ~ "nOPV2",
      year == 2023 & month == 11 ~ "nOPV2",
      year == 2023 & month == 12 ~ "nOPV2",
      year == 2022 & month == 7 ~ "nOPV2",
      year == 2021 & month == 3 ~ "nOPV2",
      year == 2021 & month == 4 ~ "nOPV2",
      year == 2021 & month == 5 ~ "nOPV2",
      year == 2021 & month == 6 ~ "nOPV2",
      year == 2021 & month == 7 ~ "nOPV2",
      year == 2021 & month == 8 ~ "nOPV2",
      year == 2021 & month == 9 ~ "nOPV2",
      year == 2021 & month == 10 ~ "nOPV2",
      year == 2020 & month == 1 ~ "nOPV2",
      year == 2020 & month == 3 ~ "nOPV2",
      year == 2021 & month == 11 ~ "bOPV",
      year == 2021 & month == 12 ~ "bOPV",
      year == 2022 & month == 1 ~ "bOPV",
      year == 2022 & month == 2 ~ "bOPV",
      year == 2022 & month == 3 ~ "bOPV",
      year == 2022 & month == 4 ~ "bOPV",
      year == 2022 & month == 5 ~ "bOPV",
      year == 2022 & month == 8 ~ "bOPV",
      year == 2022 & month == 9 ~ "bOPV",
      year == 2022 & month == 10 ~ "bOPV",
      year == 2022 & month == 11 ~ "bOPV",
      year == 2022 & month == 12 ~ "bOPV",
      TRUE ~ ""
    ),
    Vaccine.type = case_when(
      Vaccine.type == "other" ~ vactype_other,
      TRUE ~ Vaccine.type
    ),
    Response = case_when(
      year == 2025 & month == 1 ~ "OBR1",
      year == 2024 & month == 12 ~ "NIE-2024-nOPV2",
      year == 2024 & month == 11 ~ "NIE-2024-nOPV2",
      year == 2024 & month == 10 ~ "NIE-2024-nOPV2",
      year == 2024 & month == 9 ~ "NIE-2024-nOPV2",
      year == 2024 & month == 2 ~ "NIE-2024-nOPV2",
      year == 2024 & month == 3 ~ "NIE-2024-nOPV2",
      year == 2024 & month == 4 ~ "NIE-2024-nOPV2",
      year == 2020 & month == 1 ~ "NGA-20DS-01-2020",
      year == 2020 & month == 2 ~ "NGA-20DS-01-2020",
      year == 2020 & month == 3 ~ "NGA-20DS-01-2020",
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
      year == 2023 & month == 5 ~ "NIE-2023-04-02_nOPV",
      year == 2023 & month == 6 ~ "NIE-2023-04-02_nOPV",
      year == 2023 & month == 7 ~ "NIE-2023-07-03_nOPV",
      year == 2023 & month == 10 ~ "NIE-2023-07-03_nOPV",
      year == 2023 & month == 11 ~ "NIE-2023-07-03_nOPV",
      year == 2023 & month == 12 ~ "NIE-2023-07-03_nOPV2",
      year == 2024 & month == 8 ~ "NIE-2024-nOPV2",
      TRUE ~ siatype
    )
  ) |>
  mutate(
    Vaccine.type = case_when(
      str_detect(Response, "nOPV") ~ "nOPV2",
      str_detect(Response, "bOPV") ~ "bOPV",
      TRUE ~ vactype
    )
  )

# ============================================================
# 6) FAST LONG REASON TABLE
# ============================================================
cat("\nProcessing reasons...\n")

reason_cols_main <- grep("^NOimmReas_Child.*(?<!_other)$", names(AE), value = TRUE, perl = TRUE)
reason_cols_other <- grep("^NOimmReas_Child.*_other$", names(AE), value = TRUE)

AE_reasons <- AE |>
  mutate(row_id___ = row_number())

reason_long_main <- AE_reasons |>
  select(row_id___, Country, Region, District, Response, roundNumber, Vaccine.type,
         all_of(reason_cols_main)) |>
  pivot_longer(cols = all_of(reason_cols_main), names_to = "reason_source_col", values_to = "reason_raw") |>
  mutate(reason_raw = normalize_reason_text(reason_raw)) |>
  filter(!is.na(reason_raw), reason_raw != "")

if (length(reason_cols_other) > 0) {
  reason_long_other <- AE_reasons |>
    select(row_id___, all_of(reason_cols_other)) |>
    pivot_longer(cols = all_of(reason_cols_other), names_to = "reason_other_source_col", values_to = "reason_other_raw") |>
    mutate(
      reason_other_raw = normalize_reason_text(reason_other_raw),
      reason_source_col = str_remove(reason_other_source_col, "_other$")
    ) |>
    filter(!is.na(reason_other_raw), reason_other_raw != "")
  
  reason_long <- reason_long_main |>
    left_join(reason_long_other |> select(row_id___, reason_source_col, reason_other_raw),
              by = c("row_id___", "reason_source_col")) |>
    mutate(reason_final = case_when(
      !is.na(reason_other_raw) & reason_other_raw != "" ~ reason_other_raw,
      TRUE ~ reason_raw
    ))
} else {
  reason_long <- reason_long_main |>
    mutate(reason_final = reason_raw)
}

reason_long <- reason_long |>
  filter(!is.na(reason_final), reason_final != "") |>
  mutate(
    main_reason = detect_main_reason(reason_final),
    abs_reason = if_else(main_reason == "r_childabsent", detect_abs_reason(reason_final), NA_character_),
    nc_reason = if_else(main_reason == "r_non_compliance", detect_nc_reason(reason_final), NA_character_)
  )

# ============================================================
# 7) BUILD WIDE TABLES OF STANDARDIZED REASONS
# ============================================================
build_reason_wide_std <- function(data, reason_col, names_prefix = "") {
  data %>%
    filter(!is.na(.data[[reason_col]]), .data[[reason_col]] != "") %>%
    count(Country, Region, District, Response, roundNumber, Vaccine.type, .data[[reason_col]], name = "n") %>%
    pivot_wider(names_from = all_of(reason_col), values_from = n, values_fill = 0, names_prefix = names_prefix)
}

main_reason_wide <- build_reason_wide_std(reason_long, "main_reason")
abs_reason_wide <- build_reason_wide_std(reason_long, "abs_reason")
nc_reason_wide <- build_reason_wide_std(reason_long, "nc_reason")

# Expected columns
main_reason_vars <- c("r_childabsent", "r_house_not_visited", "r_vaccinated_but_not_FM",
                      "r_child_was_asleep", "r_child_is_a_visitor", "r_non_compliance",
                      "r_childnotborn", "r_security", "other_r")

abs_reason_vars <- c("abs_reason_other", "abs_reason_travelled", "abs_reason_farm",
                     "abs_reason_market", "abs_reason_school", "abs_reason_in_playground")

nc_reason_vars <- c("nc_reason_no_felt_need", "nc_reason_child_sick", "nc_reason_vaccines_safety",
                    "nc_reason_religious_cultural", "nc_reason_no_care_giver_consent",
                    "nc_reason_poliofree", "nc_reason_too_many_rnd", "nc_reason_others",
                    "nc_reason_covid_19", "nc_reason_nopvconcern")

for (v in setdiff(main_reason_vars, names(main_reason_wide))) main_reason_wide[[v]] <- 0L
for (v in setdiff(abs_reason_vars, names(abs_reason_wide))) abs_reason_wide[[v]] <- 0L
for (v in setdiff(nc_reason_vars, names(nc_reason_wide))) nc_reason_wide[[v]] <- 0L

# ============================================================
# 8) BASE REPOSITORY AGGREGATION (WITH SM COLUMNS)
# ============================================================
cat("\nAggregating to district level...\n")

AK_base <- AE |>
  select(Country, Region, District, date, Response, Vaccine.type, roundNumber,
         month, year, u5_present, u5_FM, missed_child,
         caregiver_aware, sm_total_sources, all_of(sm_col_names)) |>
  group_by(Country, Region, District, Response, Vaccine.type, roundNumber) |>
  summarise(
    start_date = min(date, na.rm = TRUE),
    end_date = max(date, na.rm = TRUE),
    year = year(start_date),
    u5_present = sum(u5_present, na.rm = TRUE),
    u5_FM = sum(u5_FM, na.rm = TRUE),
    missed_child = sum(missed_child, na.rm = TRUE),
    cv = round(u5_FM / u5_present, 2),
    
    # SM aggregations
    HH_visited = n(),
    caregivers_informed = sum(caregiver_aware, na.rm = TRUE),
    pct_informed = round(sum(caregiver_aware, na.rm = TRUE) / n() * 100, 2),
    sm_total_mentions = sum(sm_total_sources, na.rm = TRUE),
    
    # Individual SM sources
    sm_traditional_leader = sum(.data[[sm_col_names[1]]], na.rm = TRUE),
    sm_town_announcer = sum(.data[[sm_col_names[2]]], na.rm = TRUE),
    sm_mosque_announcement = sum(.data[[sm_col_names[3]]], na.rm = TRUE),
    sm_radio = sum(.data[[sm_col_names[4]]], na.rm = TRUE),
    sm_newspaper = sum(.data[[sm_col_names[5]]], na.rm = TRUE),
    sm_poster_leaflets = sum(.data[[sm_col_names[6]]], na.rm = TRUE),
    sm_banner_hoarding = sum(.data[[sm_col_names[7]]], na.rm = TRUE),
    sm_relative_neighbour = sum(.data[[sm_col_names[8]]], na.rm = TRUE),
    sm_health_worker = sum(.data[[sm_col_names[9]]], na.rm = TRUE),
    sm_vcm_unicef = sum(.data[[sm_col_names[10]]], na.rm = TRUE),
    sm_rally_school = sum(.data[[sm_col_names[11]]], na.rm = TRUE),
    sm_not_aware = sum(.data[[sm_col_names[12]]], na.rm = TRUE),
    sm_other = sum(.data[[sm_col_names[13]]], na.rm = TRUE),
    .groups = "drop"
  )

# ============================================================
# 9) MERGE REASON TABLES
# ============================================================
AK <- AK_base |>
  left_join(main_reason_wide, by = c("Country", "Region", "District", "Response", "roundNumber", "Vaccine.type")) |>
  left_join(abs_reason_wide, by = c("Country", "Region", "District", "Response", "roundNumber", "Vaccine.type")) |>
  left_join(nc_reason_wide, by = c("Country", "Region", "District", "Response", "roundNumber", "Vaccine.type")) |>
  mutate(
    across(all_of(c(main_reason_vars, abs_reason_vars, nc_reason_vars)), ~ replace_na(., 0L)),
    total_main_reasons = r_childabsent + r_house_not_visited + r_vaccinated_but_not_FM +
      r_child_was_asleep + r_child_is_a_visitor + r_non_compliance +
      r_childnotborn + r_security + other_r
  )

# ============================================================
# 10) QUALITY CONTROL CHECKS
# ============================================================
cat("\n=== QUALITY CONTROL CHECKS ===\n")

cat("\nMain reason totals:\n")
print(colSums(AK |> select(all_of(main_reason_vars)) |> as.data.frame(), na.rm = TRUE))

cat("\nAbsence reason totals:\n")
print(colSums(AK |> select(all_of(abs_reason_vars)) |> as.data.frame(), na.rm = TRUE))

cat("\nNon-compliance reason totals:\n")
print(colSums(AK |> select(all_of(nc_reason_vars)) |> as.data.frame(), na.rm = TRUE))

cat("\nSocial Mobilization Summary:\n")
cat("  Caregivers informed: ", sum(AK$caregivers_informed, na.rm = TRUE), "\n")
cat("  HH visited: ", sum(AK$HH_visited, na.rm = TRUE), "\n")
cat("  % Informed: ", round(sum(AK$caregivers_informed, na.rm = TRUE) / sum(AK$HH_visited, na.rm = TRUE) * 100, 2), "%\n")
cat("  Top SM sources:\n")
cat("    Health Worker: ", sum(AK$sm_health_worker, na.rm = TRUE), "\n")
cat("    Radio: ", sum(AK$sm_radio, na.rm = TRUE), "\n")
cat("    Relative/Neighbour: ", sum(AK$sm_relative_neighbour, na.rm = TRUE), "\n")

cat("\nRows where total_main_reasons > missed_child:\n")
print(
  AK |>
    filter(total_main_reasons > missed_child) |>
    select(Country, Region, District, Response, roundNumber, missed_child, total_main_reasons) |>
    head(20)
)

# ============================================================
# 11) EXPORT
# ============================================================
cat("\nSaving output...\n")
write_csv(AK, out_file)

cat("\n", paste(rep("=", 60), collapse = ""), "\n")
cat("PROCESSING COMPLETE!\n")
cat(paste(rep("=", 60), collapse = ""), "\n\n")

cat("📊 Summary:\n")
cat("  Total rows saved:", format(nrow(AK), big.mark = ","), "\n")
cat("  Total columns:", ncol(AK), "\n")
cat("  SM columns created:", sum(grepl("^sm_", names(AK))), "\n")
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
preview_cols <- preview_cols[preview_cols %in% names(AK)]
print(head(AK[, preview_cols], 5))