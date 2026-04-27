# ============================================================
# NIGERIA IM REPOSITORY BUILDER - OPTIMIZED FAST VERSION
# Reasons + Correct Nigeria Social Mobilization Codebook
# ============================================================

suppressPackageStartupMessages({
  library(tidyverse)
  library(data.table)
  library(stringr)
  library(lubridate)
  library(qs)
})

# ============================================================
# 0) INPUT / OUTPUT
# ============================================================

rds_file <- "C:/Users/TOURE/Documents/PADACORD/IM/7178.rds"
out_file <- "C:/Users/TOURE/Mes documents/REPOSITORIES/IM_raw_data/IM_level/NIE_IM_JAN_2025.csv"

# ============================================================
# 1) READ RAW DATA
# ============================================================

cat("\nReading raw data...\n")
t0 <- Sys.time()

AB <- qread(rds_file)
setDT(AB)

AB[, Country := "NIE"]
AB[, states := trimws(as.character(states))]
AB[states %in% c("", "NA", "null", "nan"), states := NA_character_]
AB <- AB[!is.na(states)]

cat("Read completed in:", round(difftime(Sys.time(), t0, units = "secs"), 1), "seconds\n")

# ============================================================
# 2) HELPERS
# ============================================================

normalize_reason_text <- function(x) {
  x <- tolower(trimws(as.character(x)))
  x[x %in% c("", "na", "nan", "null")] <- NA_character_
  x <- iconv(x, from = "", to = "ASCII//TRANSLIT", sub = "")
  x <- gsub("[\r\n\t]+", " ", x)
  x <- gsub("[[:punct:]]+", " ", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

detect_main_reason <- function(x) {
  x0 <- normalize_reason_text(x)
  
  dplyr::case_when(
    is.na(x0) | x0 == "" ~ NA_character_,
    
    str_detect(x0, "\\bnew born\\b|\\bnewborn\\b|\\bnew birth\\b|\\bnewly born\\b|\\ba day child\\b|\\ba day old\\b|\\bthree days old\\b|\\bjust gave birth\\b|\\bgiven birth\\b|\\bwas born yesterday\\b|\\bchild was born yesterday\\b|\\bdelivered on\\b|\\bbaby has four days\\b|\\bbaby was just born\\b|\\bzero dose\\b|\\bnew born baby\\b|\\bnew born babies\\b|\\bnew born child\\b|\\bnew born bby\\b") ~ "r_childnotborn",
    
    str_detect(x0, "\\bsecurity\\b|security related issues") ~ "r_security",
    
    str_detect(x0, "finger mark|finger marked|finger marking|not finger marked|no mark on left finger|fingers not mark|mark was seen|mark was not seen|cleaned off|wrongly finger marked|vaccinated but was not marked|immunized but no mark|vaccinated but no mark|finger marking erased|finger marking has cleaned off|child was immunized|the child was immunized|immunized at different occasions|passed the immunization|received routine opv|received vaccine last month") ~ "r_vaccinated_but_not_FM",
    
    str_detect(x0, "team not visited|team not visit|team did not visit|team didn t visit|household not visited|household not visit|house was not visit|house not visited|not revisited|never revisited|no revisit|revisit household|team omitted|did not make effort|didn t make effort|team failed to check|questions were not asked|didn t ask|did not ask|team didn t see the children|team didn t immunise|team didn t immunize|team visited but.*not.*effort|team got to the house but.*not.*effort|missed by the team|team do not ask five key question|teams unable to ask questions|team wrote revisit but never revisited|team wrote revisited but never revisited|team didn t reached out|team failed to check for children|house was not visit by the team") ~ "r_house_not_visited",
    
    str_detect(x0, "\\basleep\\b|\\bsleeping\\b|\\bwas sleeping\\b|\\bchild sleeping\\b|\\bchild was sleep\\b|\\bchild was asleep\\b|\\bwas slept\\b|\\bat sleep\\b|\\bchild was sleeping\\b|\\bchild is sleeping\\b|\\bshe was sleeping\\b|\\bmother was sleeping\\b") ~ "r_child_was_asleep",
    
    str_detect(x0, "\\bvisitor\\b|\\bvisitors\\b|came for visiting|came for a visit|came visiting|came on visit|visiting child|visiting parent|just arrived|just came|just came back|moved in|from outside the settlement|from another state|from other state|not from the settlement|came for holiday|just came for holiday|holiday|sallah festival|omugwo|from village|from ibadan|from kano|from lagos|visit child from other lga|newly located|packed in|visitor from village|came from ibadan|came from kano|came from lagos|from nassarawa state|child just came visiting|child was visiting from another state|he is a visitor|the child is a visitor|he s on a visit to the house") ~ "r_child_is_a_visitor",
    
    str_detect(x0, "non compliance|noncompliance|refusal|refused|rejection|does not want|do not allow|did not allow|father refused|father did not allow|father do not allow|mother requested not to give|not intrested|not interested|religious|traditional|cultural|no felt need|polio can be cured|polio has been eradicated|too many rounds|too many round|too many rnd|no caregiver consent|no care giver consent|no parental consent|caregiver refusal|scared|afraid|vaccine.*safe|vaccines.*safe|negative way|not been educated|ignorance|the say no|religious beliefs|announcement from mosque") ~ "r_non_compliance",
    
    str_detect(x0, "absent|abcent|absant|absend|abcend|abset|absence|absences|not around|not arround|not arrnd|not a round|not arond|not sround|not araun|not home|not at home|not as home|note at home|no at home|not present|not found at home|not in the house|not in house|not in area|not available|not seen|not met|wasn t present|wasnt present|wasn t around|wasnt around|wasn t home|wasnt home|away during|away with|went out|out of house|market|farm|school|shool|sch|islamiya|modiraza|qur an school|play ground|playground|playing ground|play graunt|play grouwn|play groud|play grand|social event|social events|socialevert|socialevent|social evert|social getthering|event center|church|travel|travelled|travelling|traveling|journey|transit|errand|river|work|office|wedding|ceremony|burial|meeting") ~ "r_childabsent",
    
    TRUE ~ "other_r"
  )
}

detect_abs_reason <- function(x) {
  x0 <- normalize_reason_text(x)
  
  dplyr::case_when(
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
  
  dplyr::case_when(
    is.na(x0) | x0 == "" ~ NA_character_,
    str_detect(x0, "religious|cultural|traditional|announcement from mosque|religious beliefs") ~ "nc_reason_religious_cultural",
    str_detect(x0, "polio can be cured|polio free|polio has been eradicated|poliofree") ~ "nc_reason_poliofree",
    str_detect(x0, "vaccine.*safe|vaccines.*safe|afraid|scared|negative way|reacted on child|interaction|safety") ~ "nc_reason_vaccines_safety",
    str_detect(x0, "no felt need|no need|no perceived need") ~ "nc_reason_no_felt_need",
    str_detect(x0, "too many rounds|too many round|too many rnd") ~ "nc_reason_too_many_rnd",
    str_detect(x0, "father refused|father did not allow|father do not allow|does not want|do not allow|did not allow|no caregiver consent|no care giver consent|no parental consent|caregiver refusal|mother requested not to give|refusal|refused|rejection|the say no") ~ "nc_reason_no_care_giver_consent",
    str_detect(x0, "child is sick|child was sick|child sick|child seek|child was ill|not healthy|unwell|hospital|admitted|not well|no medicine|physically fit|seriously sick|child are sick") ~ "nc_reason_child_sick",
    str_detect(x0, "covid") ~ "nc_reason_covid_19",
    str_detect(x0, "nopv|n opv") ~ "nc_reason_nopvconcern",
    TRUE ~ "nc_reason_others"
  )
}

# ============================================================
# 3) EXPECTED OUTPUT VARIABLES
# ============================================================

main_reason_vars <- c(
  "r_childabsent",
  "r_house_not_visited",
  "r_vaccinated_but_not_FM",
  "r_child_was_asleep",
  "r_child_is_a_visitor",
  "r_non_compliance",
  "r_childnotborn",
  "r_security",
  "other_r"
)

abs_reason_vars <- c(
  "abs_reason_other",
  "abs_reason_travelled",
  "abs_reason_farm",
  "abs_reason_market",
  "abs_reason_school",
  "abs_reason_in_playground"
)

nc_reason_vars <- c(
  "nc_reason_no_felt_need",
  "nc_reason_child_sick",
  "nc_reason_vaccines_safety",
  "nc_reason_religious_cultural",
  "nc_reason_no_care_giver_consent",
  "nc_reason_poliofree",
  "nc_reason_too_many_rnd",
  "nc_reason_others",
  "nc_reason_covid_19",
  "nc_reason_nopvconcern"
)

# Correct Nigeria SourceInfo codebook
sm_map_dt <- data.table(
  sm_code = as.character(1:13),
  sm_type = c(
    "sm_traditional_leader",             # 1
    "sm_town_announcer",                 # 2
    "sm_mosque_announcement",            # 3
    "sm_radio",                          # 4
    "sm_newspaper",                      # 5
    "sm_poster_leaflets",                # 6
    "sm_banner_hoarding",                # 7
    "sm_relative_neighbour_friend",      # 8
    "sm_health_worker",                  # 9
    "sm_vcm_unicef",                     # 10
    "sm_school_children_rally_visit",    # 11
    "sm_not_aware",                      # 12
    "sm_other"                           # 13
  )
)

sm_vars <- sm_map_dt$sm_type
sm_aware_vars <- setdiff(sm_vars, "sm_not_aware")

# ============================================================
# 4) BASE REPOSITORY PREP
# ============================================================

cat("\nPreparing base data...\n")
t0 <- Sys.time()

AC <- copy(AB)

AC <- AC[!is.na(today) & !is.na(states)]

AC[, today := as.Date(today)]
AC[, DateMonitor := as.Date(DateMonitor)]
AC[, year := as.numeric(lubridate::year(today))]

imm_cols <- grep("^Imm_Seen_house", names(AC), value = TRUE)
unimm_cols <- grep("^unimm_h", names(AC), value = TRUE)

for (cc in c(imm_cols, unimm_cols)) {
  set(AC, j = cc, value = suppressWarnings(as.numeric(AC[[cc]])))
}

AC <- AC[year > 2019]

setnames(
  AC,
  old = c("states", "lgas", "today"),
  new = c("Region", "District", "date"),
  skip_absent = TRUE
)

AC[, u5_FM := rowSums(.SD, na.rm = TRUE), .SDcols = imm_cols]
AC[, missed_child := rowSums(.SD, na.rm = TRUE), .SDcols = unimm_cols]
AC[, u5_present := u5_FM + missed_child]
AC[, month := lubridate::month(date)]

AC[vactype_other == "b0pv", vactype_other := "bOPV"]
AC[vactype_other == "cmopv2", vactype_other := "mOPV"]
AC[vactype_other %in% c("fiPv", "fipv plus", "fIPV plus", "Fipv plus nopv2", "Fipv+nopv2"), vactype_other := "FIPV+nOPV2"]
AC[vactype_other == "FIPV+NOPV2", vactype_other := "bOPV"]
AC[vactype_other == "Hpv", vactype_other := "HPV"]
AC[vactype_other %in% c("N opv", "N opv3"), vactype_other := "nOPV2"]

AC[, roundNumber := paste0("Rnd", month)]

AC[year == 2025 & month == 1, roundNumber := "Rnd1"]
AC[year == 2024 & month == 12, roundNumber := "Rnd5"]
AC[year == 2024 & month == 11, roundNumber := "Rnd4"]
AC[year == 2024 & month %in% c(9, 10), roundNumber := "Rnd3"]
AC[year == 2024 & month %in% c(2, 3), roundNumber := "Rnd1"]
AC[year == 2024 & month %in% c(4, 5, 6), roundNumber := "Rnd2"]
AC[year == 2023 & month %in% c(1, 5), roundNumber := "Rnd1"]
AC[year == 2023 & month %in% c(6, 7, 8), roundNumber := "Rnd2"]
AC[year == 2023 & month %in% c(9, 10), roundNumber := "Rnd3"]
AC[year == 2023 & month == 11, roundNumber := "Rnd4"]
AC[year == 2023 & month == 12, roundNumber := "Rnd5"]

AC[, Vaccine.type := ""]

AC[year == 2025 & month == 1, Vaccine.type := "nOPV2"]
AC[year == 2024 & month %in% c(2, 3, 4, 9, 10, 11, 12), Vaccine.type := "nOPV2"]
AC[year == 2023 & month == 1, Vaccine.type := "nOPV2"]
AC[year == 2023 & month %in% c(5, 7, 9), Vaccine.type := "fIPV+nOPV2"]
AC[year == 2023 & month %in% c(8, 10, 11, 12), Vaccine.type := "nOPV2"]
AC[year == 2022 & month == 7, Vaccine.type := "nOPV2"]
AC[year == 2021 & month %in% 3:10, Vaccine.type := "nOPV2"]
AC[year == 2020 & month %in% c(1, 3), Vaccine.type := "nOPV2"]
AC[year == 2021 & month %in% c(11, 12), Vaccine.type := "bOPV"]
AC[year == 2022 & month %in% c(1, 2, 3, 4, 5, 8, 9, 10, 11, 12), Vaccine.type := "bOPV"]

AC[Vaccine.type == "other", Vaccine.type := vactype_other]

AC[, Response := siatype]

AC[year == 2025 & month == 1, Response := "OBR1"]
AC[year == 2024 & month %in% c(2, 3, 4, 8, 9, 10, 11, 12), Response := "NIE-2024-nOPV2"]
AC[year == 2020 & month %in% c(1, 2, 3), Response := "NGA-20DS-01-2020"]
AC[year == 2020 & month == 12, Response := "NGA-5DS-10-2020"]
AC[year == 2021 & month == 1, Response := "NGA-5DS-10-2020"]
AC[year == 2021 & month == 3, Response := "NGA-2021-013-1"]
AC[year == 2021 & month %in% c(4, 5), Response := "NGA-2021-011-1"]
AC[year == 2021 & month %in% c(6, 7), Response := "NGA-2021-016-1"]
AC[year == 2021 & month == 8, Response := "NGA-2021-019"]
AC[year == 2021 & month == 9, Response := "NGA-2021-020-4"]
AC[year == 2021 & month == 10, Response := "NGA-2021-020-2"]
AC[year == 2021 & month == 11, Response := "NGA-2021-020-3"]
AC[year == 2022 & month %in% c(7, 8), Response := "Kwara Response"]
AC[year == 2023 & month %in% c(5, 6), Response := "NIE-2023-04-02_nOPV"]
AC[year == 2023 & month %in% c(7, 10, 11), Response := "NIE-2023-07-03_nOPV"]
AC[year == 2023 & month == 12, Response := "NIE-2023-07-03_nOPV2"]

AC[str_detect(Response, "nOPV"), Vaccine.type := "nOPV2"]
AC[str_detect(Response, "bOPV"), Vaccine.type := "bOPV"]
AC[!(str_detect(Response, "nOPV") | str_detect(Response, "bOPV")), Vaccine.type := vactype]

AE <- AC
AE[, row_id___ := .I]

cat("Base prep completed in:", round(difftime(Sys.time(), t0, units = "secs"), 1), "seconds\n")

# ============================================================
# 5) OPTIMIZED REASON LONG TABLE
# ============================================================

cat("\nProcessing missed-child reasons...\n")
t0 <- Sys.time()

reason_cols_main <- grep("^NOimmReas_Child.*(?<!_other)$", names(AE), value = TRUE, perl = TRUE)
reason_cols_other <- grep("^NOimmReas_Child.*_other$", names(AE), value = TRUE)

id_cols_reason <- c(
  "row_id___",
  "Country",
  "Region",
  "District",
  "Response",
  "roundNumber",
  "Vaccine.type"
)

reason_long_main <- melt(
  AE,
  id.vars = id_cols_reason,
  measure.vars = reason_cols_main,
  variable.name = "reason_source_col",
  value.name = "reason_raw",
  variable.factor = FALSE
)

reason_long_main[, reason_raw := normalize_reason_text(reason_raw)]
reason_long_main <- reason_long_main[!is.na(reason_raw) & reason_raw != ""]

reason_long_other <- melt(
  AE,
  id.vars = "row_id___",
  measure.vars = reason_cols_other,
  variable.name = "reason_other_source_col",
  value.name = "reason_other_raw",
  variable.factor = FALSE
)

reason_long_other[, reason_other_raw := normalize_reason_text(reason_other_raw)]
reason_long_other <- reason_long_other[!is.na(reason_other_raw) & reason_other_raw != ""]
reason_long_other[, reason_source_col := str_remove(reason_other_source_col, "_other$")]

reason_long <- merge(
  reason_long_main,
  reason_long_other[, .(row_id___, reason_source_col, reason_other_raw)],
  by = c("row_id___", "reason_source_col"),
  all.x = TRUE
)

reason_long[, reason_final := fifelse(
  !is.na(reason_other_raw) & reason_other_raw != "",
  reason_other_raw,
  reason_raw
)]

reason_long <- reason_long[!is.na(reason_final) & reason_final != ""]

cat("Reason long table rows:", nrow(reason_long), "\n")
cat("Reason long processing completed in:", round(difftime(Sys.time(), t0, units = "secs"), 1), "seconds\n")

# ============================================================
# 6) CLASSIFY REASONS
# ============================================================

cat("\nClassifying reasons...\n")
t0 <- Sys.time()

reason_long[, main_reason := detect_main_reason(reason_final)]
reason_long[, abs_reason := NA_character_]
reason_long[, nc_reason := NA_character_]

reason_long[main_reason == "r_childabsent", abs_reason := detect_abs_reason(reason_final)]
reason_long[main_reason == "r_non_compliance", nc_reason := detect_nc_reason(reason_final)]

cat("Reason classification completed in:", round(difftime(Sys.time(), t0, units = "secs"), 1), "seconds\n")

# ============================================================
# 7) WIDE REASON TABLES
# ============================================================

cat("\nBuilding reason wide tables...\n")
t0 <- Sys.time()

reason_group_cols <- c(
  "Country",
  "Region",
  "District",
  "Response",
  "roundNumber",
  "Vaccine.type"
)

main_reason_wide <- dcast(
  reason_long[!is.na(main_reason) & main_reason != ""],
  Country + Region + District + Response + roundNumber + Vaccine.type ~ main_reason,
  fun.aggregate = length,
  value.var = "main_reason"
)

abs_reason_wide <- dcast(
  reason_long[!is.na(abs_reason) & abs_reason != ""],
  Country + Region + District + Response + roundNumber + Vaccine.type ~ abs_reason,
  fun.aggregate = length,
  value.var = "abs_reason"
)

nc_reason_wide <- dcast(
  reason_long[!is.na(nc_reason) & nc_reason != ""],
  Country + Region + District + Response + roundNumber + Vaccine.type ~ nc_reason,
  fun.aggregate = length,
  value.var = "nc_reason"
)

for (v in setdiff(main_reason_vars, names(main_reason_wide))) main_reason_wide[, (v) := 0L]
for (v in setdiff(abs_reason_vars, names(abs_reason_wide))) abs_reason_wide[, (v) := 0L]
for (v in setdiff(nc_reason_vars, names(nc_reason_wide))) nc_reason_wide[, (v) := 0L]

cat("Reason wide tables completed in:", round(difftime(Sys.time(), t0, units = "secs"), 1), "seconds\n")

# ============================================================
# 8) SOCIAL MOBILIZATION - CORRECT NIGERIA CODEBOOK
# ============================================================

cat("\nProcessing social mobilization...\n")
t0 <- Sys.time()

sm_cols <- grep("^SourceInfo_house", names(AE), value = TRUE, ignore.case = TRUE)

if (length(sm_cols) > 0) {
  
  sm_long <- melt(
    AE,
    id.vars = c(
      "row_id___",
      "Country",
      "Region",
      "District",
      "Response",
      "roundNumber",
      "Vaccine.type"
    ),
    measure.vars = sm_cols,
    variable.name = "sm_col",
    value.name = "sm_raw",
    variable.factor = FALSE
  )
  
  sm_long[, sm_raw := trimws(tolower(as.character(sm_raw)))]
  sm_long[sm_raw %in% c("", "na", "nan", "null"), sm_raw := NA_character_]
  sm_long <- sm_long[!is.na(sm_raw)]
  
  # Split multi-code entries such as "1 2 9"
  sm_long <- sm_long[
    ,
    .(sm_code = unlist(strsplit(sm_raw, "\\s+"))),
    by = .(
      row_id___,
      Country,
      Region,
      District,
      Response,
      roundNumber,
      Vaccine.type,
      sm_col
    )
  ]
  
  sm_long[, sm_code := trimws(sm_code)]
  sm_long <- sm_long[sm_code != ""]
  
  sm_unknown_codes <- setdiff(unique(sm_long$sm_code), sm_map_dt$sm_code)
  
  if (length(sm_unknown_codes) > 0) {
    cat("\nWARNING: Unknown SourceInfo codes detected:\n")
    print(sm_unknown_codes)
  }
  
  sm_long <- merge(
    sm_long,
    sm_map_dt,
    by = "sm_code",
    all.x = FALSE,
    all.y = FALSE
  )
  
  sm_wide <- dcast(
    sm_long,
    Country + Region + District + Response + roundNumber + Vaccine.type ~ sm_type,
    fun.aggregate = length,
    value.var = "sm_type"
  )
  
  for (v in setdiff(sm_vars, names(sm_wide))) sm_wide[, (v) := 0L]
  
  sm_wide[, sm_total_sources := rowSums(.SD, na.rm = TRUE), .SDcols = sm_vars]
  sm_wide[, sm_total_awareness_sources := rowSums(.SD, na.rm = TRUE), .SDcols = sm_aware_vars]
  
} else {
  
  sm_wide <- unique(AE[, .(
    Country,
    Region,
    District,
    Response,
    roundNumber,
    Vaccine.type
  )])
  
  for (v in sm_vars) sm_wide[, (v) := 0L]
  
  sm_wide[, sm_total_sources := 0L]
  sm_wide[, sm_total_awareness_sources := 0L]
}

cat("SM processing completed in:", round(difftime(Sys.time(), t0, units = "secs"), 1), "seconds\n")

# ============================================================
# 9) BASE REPOSITORY AGGREGATION
# ============================================================

cat("\nAggregating base repository...\n")
t0 <- Sys.time()

AK_base <- AE[
  ,
  .(
    start_date = min(date, na.rm = TRUE),
    end_date = max(date, na.rm = TRUE),
    u5_present = sum(u5_present, na.rm = TRUE),
    u5_FM = sum(u5_FM, na.rm = TRUE),
    missed_child = sum(missed_child, na.rm = TRUE)
  ),
  by = .(
    Country,
    Region,
    District,
    Response,
    Vaccine.type,
    roundNumber
  )
]

AK_base[, year := lubridate::year(start_date)]
AK_base[, cv := round(u5_FM / u5_present, 2)]
AK_base[is.nan(cv) | is.infinite(cv), cv := NA_real_]

cat("Base aggregation completed in:", round(difftime(Sys.time(), t0, units = "secs"), 1), "seconds\n")

# ============================================================
# 10) MERGE FINAL REPOSITORY
# ============================================================

cat("\nMerging final repository...\n")
t0 <- Sys.time()

setkeyv(AK_base, reason_group_cols)
setkeyv(main_reason_wide, reason_group_cols)
setkeyv(abs_reason_wide, reason_group_cols)
setkeyv(nc_reason_wide, reason_group_cols)
setkeyv(sm_wide, reason_group_cols)

AK <- merge(
  AK_base,
  main_reason_wide[, c(reason_group_cols, main_reason_vars), with = FALSE],
  by = reason_group_cols,
  all.x = TRUE
)

AK <- merge(
  AK,
  abs_reason_wide[, c(reason_group_cols, abs_reason_vars), with = FALSE],
  by = reason_group_cols,
  all.x = TRUE
)

AK <- merge(
  AK,
  nc_reason_wide[, c(reason_group_cols, nc_reason_vars), with = FALSE],
  by = reason_group_cols,
  all.x = TRUE
)

AK <- merge(
  AK,
  sm_wide[, c(
    reason_group_cols,
    sm_vars,
    "sm_total_sources",
    "sm_total_awareness_sources"
  ), with = FALSE],
  by = reason_group_cols,
  all.x = TRUE
)

count_cols <- c(
  main_reason_vars,
  abs_reason_vars,
  nc_reason_vars,
  sm_vars,
  "sm_total_sources",
  "sm_total_awareness_sources"
)

for (cc in count_cols) {
  set(AK, i = which(is.na(AK[[cc]])), j = cc, value = 0L)
}

AK[
  ,
  total_main_reasons :=
    r_childabsent +
    r_house_not_visited +
    r_vaccinated_but_not_FM +
    r_child_was_asleep +
    r_child_is_a_visitor +
    r_non_compliance +
    r_childnotborn +
    r_security +
    other_r
]

setcolorder(
  AK,
  c(
    "Country",
    "Region",
    "District",
    "Response",
    "Vaccine.type",
    "roundNumber",
    "start_date",
    "end_date",
    "year",
    "u5_present",
    "u5_FM",
    "missed_child",
    "cv",
    main_reason_vars,
    abs_reason_vars,
    nc_reason_vars,
    sm_vars,
    "sm_total_sources",
    "sm_total_awareness_sources",
    "total_main_reasons"
  )
)

cat("Final merge completed in:", round(difftime(Sys.time(), t0, units = "secs"), 1), "seconds\n")

# ============================================================
# 11) QC
# ============================================================

cat("\n================ QC SUMMARY ================\n")

cat("\nMain reason totals:\n")
print(colSums(as.data.frame(AK[, ..main_reason_vars]), na.rm = TRUE))

cat("\nAbsence reason totals:\n")
print(colSums(as.data.frame(AK[, ..abs_reason_vars]), na.rm = TRUE))

cat("\nNon-compliance reason totals:\n")
print(colSums(as.data.frame(AK[, ..nc_reason_vars]), na.rm = TRUE))

cat("\nSocial mobilization totals:\n")
print(colSums(as.data.frame(AK[, ..sm_vars]), na.rm = TRUE))

cat("\nSM total sources:\n")
print(sum(AK$sm_total_sources, na.rm = TRUE))

cat("\nSM total awareness sources excluding Not Aware:\n")
print(sum(AK$sm_total_awareness_sources, na.rm = TRUE))

cat("\nRows where total_main_reasons > missed_child:\n")
print(
  AK[
    total_main_reasons > missed_child,
    .(
      Country,
      Region,
      District,
      Response,
      roundNumber,
      missed_child,
      total_main_reasons
    )
  ][1:20]
)

cat("\nRows where u5_present is 0 or missing:\n")
print(
  AK[
    is.na(u5_present) | u5_present == 0,
    .(
      Country,
      Region,
      District,
      Response,
      roundNumber,
      u5_present,
      u5_FM,
      missed_child
    )
  ][1:20]
)

cat("\n============================================\n")

# ============================================================
# 12) EXPORT
# ============================================================

cat("\nWriting output...\n")
t0 <- Sys.time()

fwrite(AK, out_file)

cat("Export completed in:", round(difftime(Sys.time(), t0, units = "secs"), 1), "seconds\n")
cat("\nNigeria IM repository successfully written to:\n", out_file, "\n")