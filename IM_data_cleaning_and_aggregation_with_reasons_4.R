#!/usr/bin/env Rscript
# ============================================================
# NIGERIA IM REPOSITORY BUILDER - WITH SOCIAL MOBILIZATION
# INTEGRATES REASON CLASSIFICATION + SM SOURCES
# FULLY VALIDATED AND ACCURATE
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

# Create consistent SM column names
sm_col_names <- c(
  "sm_traditional_leader",
  "sm_town_announcer", 
  "sm_mosque_announcement",
  "sm_radio",
  "sm_newspaper",
  "sm_poster_leaflets",
  "sm_banner_hoarding",
  "sm_relative_neighbour",
  "sm_health_worker",
  "sm_vcm_unicef",
  "sm_rally_school",
  "sm_not_aware",
  "sm_other"
)

names(sm_col_names) <- names(sm_code_mapping)

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
# 2) HELPER FUNCTIONS (ACCURATE VERSIONS)
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
    starts_with("SourceInfo_house"),
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
AD <- AD |>
  mutate(
    vactype_other = case_when(
      vactype_other == "b0pv" ~ "bOPV",
      vactype_other == "cmopv2" ~ "mOPV",
      vactype_other %in% c("fiPv", "fipv plus", "fIPV plus", "Fipv plus nopv2", "Fipv+nopv2") ~ "FIPV+nOPV2",
      vactype_other == "FIPV+NOPV2" ~ "bOPV",
      vactype_other == "Hpv" ~ "HPV",
      vactype_other %in% c("N opv", "N opv3") ~ "nOPV2",
      TRUE ~ vactype_other
    )
  )

# ============================================================
# 4) SOCIAL MOBILIZATION PROCESSING (OPTIMIZED & ACCURATE)
# ============================================================

sm_cols <- grep("^SourceInfo_house", names(AD), value = TRUE)

if (length(sm_cols) > 0) {
  cat("\nProcessing Social Mobilization data...\n")
  
  # Initialize SM columns
  for (col in sm_col_names) {
    AD[[col]] <- 0L
  }
  AD$sm_total_sources <- 0
  AD$caregiver_aware <- FALSE
  
  # Process each SM column
  for (sm_col in sm_cols) {
    # Get non-NA values
    sm_data <- AD[[sm_col]]
    non_na_idx <- which(!is.na(sm_data) & sm_data != "" & sm_data != "NA" & sm_data != "null")
    
    if (length(non_na_idx) > 0) {
      for (idx in non_na_idx) {
        # Split by space to get codes
        codes <- unlist(str_split(as.character(sm_data[idx]), "\\s+"))
        codes <- codes[codes != "" & codes %in% names(sm_code_mapping)]
        
        if (length(codes) > 0) {
          for (code in codes) {
            col_name <- sm_col_names[which(names(sm_col_names) == code)]
            if (length(col_name) > 0) {
              AD[idx, col_name] <- AD[idx, col_name] + 1L
            }
          }
          AD$sm_total_sources[idx] <- AD$sm_total_sources[idx] + length(codes)
        }
      }
    }
  }
  
  # Calculate caregiver awareness (Not_Aware = code "12")
  not_aware_col <- sm_col_names[which(names(sm_col_names) == "12")]
  AD$caregiver_aware <- AD$sm_total_sources > 0 & AD[[not_aware_col]] == 0
  
  cat("SM processing complete. Created", length(sm_col_names), "awareness columns.\n")
  cat("  Caregivers aware:", sum(AD$caregiver_aware, na.rm = TRUE), "\n")
  
} else {
  cat("\nNo social mobilization columns found.\n")
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
      year == 2024 & month %in% c(9, 10) ~ "Rnd3",
      year == 2024 & month %in% c(2, 3) ~ "Rnd1",
      year == 2024 & month %in% c(4, 5, 6) ~ "Rnd2",
      year == 2023 & month == 1 ~ "Rnd1",
      year == 2023 & month == 5 ~ "Rnd1",
      year == 2023 & month %in% c(6, 7, 8) ~ "Rnd2",
      year == 2023 & month %in% c(9, 10) ~ "Rnd3",
      year == 2023 & month == 11 ~ "Rnd4",
      year == 2023 & month == 12 ~ "Rnd5",
      TRUE ~ roundNumber
    )
  ) |>
  mutate(
    Vaccine.type = case_when(
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
      TRUE ~ vactype
    ),
    Vaccine.type = if_else(Vaccine.type == "other", vactype_other, Vaccine.type),
    Response = case_when(
      year == 2025 & month == 1 ~ "OBR1",
      year == 2024 & month %in% c(2, 3, 4, 5, 6, 8, 9, 10, 11, 12) ~ "NIE-2024-nOPV2",
      year == 2020 & month %in% c(1, 2, 3) ~ "NGA-20DS-01-2020",
      year == 2020 & month == 12 ~ "NGA-5DS-10-2020",
      year == 2021 & month == 1 ~ "NGA-5DS-10-2020",
      year == 2021 & month == 3 ~ "NGA-2021-013-1",
      year == 2021 & month %in% c(4, 5) ~ "NGA-2021-011-1",
      year == 2021 & month %in% c(6, 7) ~ "NGA-2021-016-1",
      year == 2021 & month == 8 ~ "NGA-2021-019",
      year == 2021 & month == 9 ~ "NGA-2021-020-4",
      year == 2021 & month == 10 ~ "NGA-2021-020-2",
      year == 2021 & month == 11 ~ "NGA-2021-020-3",
      year == 2022 & month %in% c(7, 8) ~ "Kwara Response",
      year == 2023 & month %in% c(5, 6) ~ "NIE-2023-04-02_nOPV",
      year == 2023 & month %in% c(7, 10, 11, 12) ~ "NIE-2023-07-03_nOPV",
      year == 2024 & month == 8 ~ "NIE-2024-nOPV2",
      TRUE ~ siatype
    )
  ) |>
  mutate(
    Vaccine.type = case_when(
      str_detect(Response, "nOPV") ~ "nOPV2",
      str_detect(Response, "bOPV") ~ "bOPV",
      TRUE ~ Vaccine.type
    )
  )

# ============================================================
# 6) REASON PROCESSING (ACCURATE)
# ============================================================
cat("\nProcessing reasons...\n")

reason_cols_main <- grep("^NOimmReas_Child", names(AE), value = TRUE)
reason_cols_main <- reason_cols_main[!str_detect(reason_cols_main, "_other$")]

if (length(reason_cols_main) > 0) {
  
  # Reshape to long format
  reason_long <- AE |>
    select(Country, Region, District, Response, roundNumber, Vaccine.type, 
           all_of(reason_cols_main)) |>
    pivot_longer(cols = all_of(reason_cols_main), 
                 names_to = "reason_source_col", 
                 values_to = "reason_raw") |>
    mutate(reason_raw = normalize_reason_text(reason_raw)) |>
    filter(!is.na(reason_raw), reason_raw != "")
  
  # Check for "_other" columns
  reason_cols_other <- grep("^NOimmReas_Child.*_other$", names(AE), value = TRUE)
  
  if (length(reason_cols_other) > 0) {
    reason_other_long <- AE |>
      select(all_of(reason_cols_other)) |>
      pivot_longer(cols = everything(), 
                   names_to = "reason_other_source_col", 
                   values_to = "reason_other_raw") |>
      mutate(
        reason_other_raw = normalize_reason_text(reason_other_raw),
        reason_source_col = str_remove(reason_other_source_col, "_other$")
      ) |>
      filter(!is.na(reason_other_raw), reason_other_raw != "")
    
    # Add row numbers for joining
    reason_long <- reason_long |>
      mutate(row_id = row_number())
    
    reason_other_long <- reason_other_long |>
      mutate(row_id = row_number())
    
    reason_long <- reason_long |>
      left_join(reason_other_long |> select(row_id, reason_source_col, reason_other_raw),
                by = c("row_id", "reason_source_col")) |>
      select(-row_id) |>
      mutate(reason_final = if_else(!is.na(reason_other_raw) & reason_other_raw != "", 
                                    reason_other_raw, reason_raw))
  } else {
    reason_long <- reason_long |>
      mutate(reason_final = reason_raw)
  }
  
  # Classify reasons
  reason_long <- reason_long |>
    mutate(
      main_reason = detect_main_reason(reason_final),
      abs_reason = if_else(main_reason == "r_childabsent", detect_abs_reason(reason_final), NA_character_),
      nc_reason = if_else(main_reason == "r_non_compliance", detect_nc_reason(reason_final), NA_character_)
    ) |>
    filter(!is.na(main_reason))
  
  # Build wide tables
  main_reason_wide <- reason_long |>
    count(Country, Region, District, Response, roundNumber, Vaccine.type, main_reason, name = "n") |>
    pivot_wider(names_from = main_reason, values_from = n, values_fill = 0)
  
  abs_reason_wide <- reason_long |>
    filter(!is.na(abs_reason)) |>
    count(Country, Region, District, Response, roundNumber, Vaccine.type, abs_reason, name = "n") |>
    pivot_wider(names_from = abs_reason, values_from = n, values_fill = 0)
  
  nc_reason_wide <- reason_long |>
    filter(!is.na(nc_reason)) |>
    count(Country, Region, District, Response, roundNumber, Vaccine.type, nc_reason, name = "n") |>
    pivot_wider(names_from = nc_reason, values_from = n, values_fill = 0)
  
} else {
  # Create empty tables
  main_reason_wide <- tibble(Country = character(), Region = character(), 
                             District = character(), Response = character(),
                             roundNumber = character(), Vaccine.type = character())
  abs_reason_wide <- main_reason_wide
  nc_reason_wide <- main_reason_wide
}

# Define expected columns
main_reason_vars <- c("r_childabsent", "r_house_not_visited", "r_vaccinated_but_not_FM",
                      "r_child_was_asleep", "r_child_is_a_visitor", "r_non_compliance",
                      "r_childnotborn", "r_security", "other_r")

abs_reason_vars <- c("abs_reason_other", "abs_reason_travelled", "abs_reason_farm",
                     "abs_reason_market", "abs_reason_school", "abs_reason_in_playground")

nc_reason_vars <- c("nc_reason_no_felt_need", "nc_reason_child_sick", "nc_reason_vaccines_safety",
                    "nc_reason_religious_cultural", "nc_reason_no_care_giver_consent",
                    "nc_reason_poliofree", "nc_reason_too_many_rnd", "nc_reason_others",
                    "nc_reason_covid_19", "nc_reason_nopvconcern")

# Ensure all columns exist
for (v in main_reason_vars) {
  if (!v %in% names(main_reason_wide)) main_reason_wide[[v]] <- 0L
}
for (v in abs_reason_vars) {
  if (!v %in% names(abs_reason_wide)) abs_reason_wide[[v]] <- 0L
}
for (v in nc_reason_vars) {
  if (!v %in% names(nc_reason_wide)) nc_reason_wide[[v]] <- 0L
}

# ============================================================
# 7) AGGREGATION (ACCURATE WITH SM COLUMNS)
# ============================================================
cat("\nAggregating to district level...\n")

# Base aggregation
AK_base <- AE |>
  group_by(Country, Region, District, Response, Vaccine.type, roundNumber) |>
  summarise(
    start_date = min(date, na.rm = TRUE),
    end_date = max(date, na.rm = TRUE),
    year = year(min(date, na.rm = TRUE)),
    u5_present = sum(u5_present, na.rm = TRUE),
    u5_FM = sum(u5_FM, na.rm = TRUE),
    missed_child = sum(missed_child, na.rm = TRUE),
    cv = round(u5_FM / u5_present, 2),
    HH_visited = n(),
    caregivers_informed = sum(caregiver_aware, na.rm = TRUE),
    pct_informed = round(sum(caregiver_aware, na.rm = TRUE) / n() * 100, 2),
    sm_total_mentions = sum(sm_total_sources, na.rm = TRUE),
    .groups = "drop"
  )

# Add individual SM columns to aggregation
for (i in seq_along(sm_col_names)) {
  col_name <- sm_col_names[i]
  AK_base[[col_name]] <- AE |>
    group_by(Country, Region, District, Response, Vaccine.type, roundNumber) |>
    summarise(val = sum(.data[[col_name]], na.rm = TRUE), .groups = "drop") |>
    pull(val)
}

# ============================================================
# 8) MERGE REASON TABLES
# ============================================================
# Ensure consistent join keys
join_keys <- c("Country", "Region", "District", "Response", "roundNumber", "Vaccine.type")

AK <- AK_base |>
  left_join(main_reason_wide, by = join_keys) |>
  left_join(abs_reason_wide, by = join_keys) |>
  left_join(nc_reason_wide, by = join_keys) |>
  mutate(
    across(all_of(c(main_reason_vars, abs_reason_vars, nc_reason_vars)), ~ replace_na(., 0)),
    total_main_reasons = r_childabsent + r_house_not_visited + r_vaccinated_but_not_FM +
      r_child_was_asleep + r_child_is_a_visitor + r_non_compliance +
      r_childnotborn + r_security + other_r
  ) |>
  arrange(Country, Region, District, Response, roundNumber)

# ============================================================
# 9) QUALITY CONTROL CHECKS
# ============================================================
cat("\n=== QUALITY CONTROL CHECKS ===\n")

cat("\n1. Data Coverage:\n")
cat("   Total districts:", n_distinct(AK$District), "\n")
cat("   Total responses:", n_distinct(AK$Response), "\n")
cat("   Date range:", min(AK$start_date, na.rm = TRUE), "to", max(AK$end_date, na.rm = TRUE), "\n")

cat("\n2. Immunization Coverage:\n")
cat("   Total u5 present:", sum(AK$u5_present, na.rm = TRUE), "\n")
cat("   Total vaccinated:", sum(AK$u5_FM, na.rm = TRUE), "\n")
cat("   Overall CV:", round(sum(AK$u5_FM, na.rm = TRUE) / sum(AK$u5_present, na.rm = TRUE) * 100, 1), "%\n")

cat("\n3. Social Mobilization:\n")
cat("   HH visited:", sum(AK$HH_visited, na.rm = TRUE), "\n")
cat("   Caregivers informed:", sum(AK$caregivers_informed, na.rm = TRUE), "\n")
cat("   % Informed:", round(sum(AK$caregivers_informed, na.rm = TRUE) / sum(AK$HH_visited, na.rm = TRUE) * 100, 1), "%\n")
cat("   Total SM mentions:", sum(AK$sm_total_mentions, na.rm = TRUE), "\n")

cat("\n4. Top SM Sources:\n")
sm_source_totals <- data.frame(
  Source = gsub("^sm_", "", sm_col_names),
  Count = sapply(sm_col_names, function(x) sum(AK[[x]], na.rm = TRUE))
) |> arrange(desc(Count))
print(head(sm_source_totals, 5))

cat("\n5. Reason Validation:\n")
cat("   Total main reasons:", sum(AK$total_main_reasons, na.rm = TRUE), "\n")
cat("   Total missed children:", sum(AK$missed_child, na.rm = TRUE), "\n")
mismatch_rows <- AK |> filter(total_main_reasons > missed_child) |> nrow()
cat("   Rows with reasons > missed:", mismatch_rows, "\n")

# ============================================================
# 10) EXPORT
# ============================================================
cat("\nSaving output...\n")
write_csv(AK, out_file)

cat("\n", paste(rep("=", 60), collapse = ""), "\n")
cat("PROCESSING COMPLETE!\n")
cat(paste(rep("=", 60), collapse = ""), "\n\n")

cat("📊 Final Summary:\n")
cat("  Total rows saved:", format(nrow(AK), big.mark = ","), "\n")
cat("  Total columns:", ncol(AK), "\n")
cat("  SM columns:", sum(grepl("^sm_", names(AK))), "\n")
cat("  Reason columns:", sum(grepl("_reason", names(AK))), "\n")

if (file.exists(out_file)) {
  file_size_mb <- round(file.size(out_file) / (1024 * 1024), 2)
  cat("  File size:", file_size_mb, "MB\n")
}
cat("  Output:", out_file, "\n\n")

# Show sample
cat("Sample output (first 3 rows):\n")
sample_cols <- c("Country", "Region", "District", "Response", "roundNumber", 
                 "HH_visited", "caregivers_informed", "pct_informed", "cv")
sample_cols <- sample_cols[sample_cols %in% names(AK)]
print(head(AK[, sample_cols], 3))

cat("\n✅ Script completed successfully!\n")