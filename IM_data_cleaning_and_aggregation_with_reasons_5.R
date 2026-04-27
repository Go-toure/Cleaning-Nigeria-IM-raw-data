# ============================================================
# NIGERIA IM REPOSITORY BUILDER - FAST LONG/WIDE VERSION
# WITH REASONS + SOCIAL MOBILIZATION
# ============================================================

library(tidyverse)
library(data.table)
library(stringr)
library(lubridate)
library(qs)

# ============================================================
# 0) INPUT / OUTPUT
# ============================================================

rds_file <- "C:/Users/TOURE/Documents/PADACORD/IM/7178.rds"
out_file <- "C:/Users/TOURE/Mes documents/REPOSITORIES/IM_raw_data/IM_level/NIE_IM_JAN_2025.csv"

# ============================================================
# 1) READ RAW DATA
# ============================================================

AB <- qread(rds_file) |>
  mutate(
    Country = "NIE",
    states = trimws(as.character(states)),
    states = na_if(states, ""),
    states = na_if(states, "NA"),
    states = na_if(states, "null")
  ) |>
  filter(!is.na(states))

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

make_reason_slug <- function(x) {
  x %>%
    str_replace_all("[^A-Za-z0-9]+", "_") %>%
    str_replace_all("_+", "_") %>%
    str_replace_all("^_|_$", "") %>%
    str_to_lower()
}

detect_main_reason <- function(x) {
  x0 <- normalize_reason_text(x)
  
  case_when(
    is.na(x0) | x0 == "" ~ NA_character_,
    
    str_detect(
      x0,
      "\\bnew born\\b|\\bnewborn\\b|\\bnew birth\\b|\\bnewly born\\b|\\ba day child\\b|\\ba day old\\b|\\bthree days old\\b|\\bjust gave birth\\b|\\bgiven birth\\b|\\bwas born yesterday\\b|\\bchild was born yesterday\\b|\\bdelivered on\\b|\\bbaby has four days\\b|\\bbaby was just born\\b|\\bzero dose\\b|\\bnew born baby\\b|\\bnew born babies\\b|\\bnew born child\\b|\\bnew born bby\\b"
    ) ~ "r_childnotborn",
    
    str_detect(x0, "\\bsecurity\\b|security related issues") ~ "r_security",
    
    str_detect(
      x0,
      "finger mark|finger marked|finger marking|not finger marked|no mark on left finger|fingers not mark|mark was seen|mark was not seen|cleaned off|wrongly finger marked|vaccinated but was not marked|immunized but no mark|vaccinated but no mark|finger marking erased|finger marking has cleaned off|child was immunized|the child was immunized|immunized at different occasions|passed the immunization|received routine opv|received vaccine last month"
    ) ~ "r_vaccinated_but_not_FM",
    
    str_detect(
      x0,
      "team not visited|team not visit|team did not visit|team didn t visit|household not visited|household not visit|house was not visit|house not visited|not revisited|never revisited|no revisit|revisit household|team omitted|did not make effort|didn t make effort|team failed to check|questions were not asked|didn t ask|did not ask|team didn t see the children|team didn t immunise|team didn t immunize|team visited but.*not.*effort|team got to the house but.*not.*effort|missed by the team|team do not ask five key question|teams unable to ask questions|team wrote revisit but never revisited|team wrote revisited but never revisited|team didn t reached out|team failed to check for children|house was not visit by the team"
    ) ~ "r_house_not_visited",
    
    str_detect(
      x0,
      "\\basleep\\b|\\bsleeping\\b|\\bwas sleeping\\b|\\bchild sleeping\\b|\\bchild was sleep\\b|\\bchild was asleep\\b|\\bwas slept\\b|\\bat sleep\\b|\\bchild was sleeping\\b|\\bchild is sleeping\\b|\\bshe was sleeping\\b|\\bmother was sleeping\\b"
    ) ~ "r_child_was_asleep",
    
    str_detect(
      x0,
      "\\bvisitor\\b|\\bvisitors\\b|came for visiting|came for a visit|came visiting|came on visit|visiting child|visiting parent|just arrived|just came|just came back|moved in|from outside the settlement|from another state|from other state|not from the settlement|came for holiday|just came for holiday|holiday|sallah festival|omugwo|from village|from ibadan|from kano|from lagos|visit child from other lga|newly located|packed in|visitor from village|came from ibadan|came from kano|came from lagos|from nassarawa state|child just came visiting|child was visiting from another state|he is a visitor|the child is a visitor|he s on a visit to the house"
    ) ~ "r_child_is_a_visitor",
    
    str_detect(
      x0,
      "non compliance|noncompliance|refusal|refused|rejection|does not want|do not allow|did not allow|father refused|father did not allow|father do not allow|mother requested not to give|not intrested|not interested|religious|traditional|cultural|no felt need|polio can be cured|polio has been eradicated|too many rounds|too many round|too many rnd|no caregiver consent|no care giver consent|no parental consent|caregiver refusal|scared|afraid|vaccine.*safe|vaccines.*safe|negative way|not been educated|ignorance|the say no|religious beliefs|announcement from mosque"
    ) ~ "r_non_compliance",
    
    str_detect(
      x0,
      "absent|abcent|absant|absend|abcend|abset|absence|absences|not around|not arround|not arrnd|not a round|not arond|not sround|not araun|not home|not at home|not as home|note at home|no at home|not present|not found at home|not in the house|not in house|not in area|not available|not seen|not met|wasn t present|wasnt present|wasn t around|wasnt around|wasn t home|wasnt home|away during|away with|went out|out of house|market|farm|school|shool|sch|islamiya|modiraza|qur an school|play ground|playground|playing ground|play graunt|play grouwn|play groud|play grand|social event|social events|socialevert|socialevent|social evert|social getthering|event center|church|travel|travelled|travelling|traveling|journey|transit|errand|river|work|office|wedding|ceremony|burial|meeting"
    ) ~ "r_childabsent",
    
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
    str_detect(x0, "father refused|father did not allow|father do not allow|does not want|do not allow|did not allow|no caregiver consent|no care giver consent|no parental consent|caregiver refusal|mother requested not to give|refusal|refused|rejection|the say no") ~ "nc_reason_no_care_giver_consent",
    str_detect(x0, "child is sick|child was sick|child sick|child seek|child was ill|not healthy|unwell|hospital|admitted|not well|no medicine|physically fit|seriously sick|child are sick") ~ "nc_reason_child_sick",
    str_detect(x0, "covid") ~ "nc_reason_covid_19",
    str_detect(x0, "nopv|n opv") ~ "nc_reason_nopvconcern",
    TRUE ~ "nc_reason_others"
  )
}

build_reason_wide_std <- function(data, reason_col) {
  data %>%
    filter(!is.na(.data[[reason_col]]), .data[[reason_col]] != "") %>%
    count(Country, Region, District, Response, roundNumber, Vaccine.type, .data[[reason_col]], name = "n") %>%
    pivot_wider(
      names_from = all_of(reason_col),
      values_from = n,
      values_fill = 0
    )
}

# ============================================================
# 3) BASE REPOSITORY PREP
# ============================================================

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
    everything()
  )

AD <- AC |>
  mutate(
    u5_FM = rowSums(across(starts_with("Imm_Seen_house")), na.rm = TRUE),
    missed_child = rowSums(across(starts_with("unimm_h")), na.rm = TRUE),
    u5_present = u5_FM + missed_child,
    month = month(date)
  )

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
      year == 2024 & month %in% c(2, 3, 4, 9, 10, 11, 12) ~ "nOPV2",
      year == 2023 & month == 1 ~ "nOPV2",
      year == 2023 & month %in% c(5, 7, 9) ~ "fIPV+nOPV2",
      year == 2023 & month %in% c(8, 10, 11, 12) ~ "nOPV2",
      year == 2022 & month == 7 ~ "nOPV2",
      year == 2021 & month %in% 3:10 ~ "nOPV2",
      year == 2020 & month %in% c(1, 3) ~ "nOPV2",
      year == 2021 & month %in% c(11, 12) ~ "bOPV",
      year == 2022 & month %in% c(1, 2, 3, 4, 5, 8, 9, 10, 11, 12) ~ "bOPV",
      TRUE ~ ""
    ),
    Vaccine.type = case_when(
      Vaccine.type == "other" ~ vactype_other,
      TRUE ~ Vaccine.type
    ),
    Response = case_when(
      year == 2025 & month == 1 ~ "OBR1",
      year == 2024 & month %in% c(2, 3, 4, 8, 9, 10, 11, 12) ~ "NIE-2024-nOPV2",
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
      year == 2023 & month %in% c(7, 10, 11) ~ "NIE-2023-07-03_nOPV",
      year == 2023 & month == 12 ~ "NIE-2023-07-03_nOPV2",
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
# 4) FAST LONG REASON TABLE
# ============================================================

reason_cols_main  <- grep("^NOimmReas_Child.*(?<!_other)$", names(AE), value = TRUE, perl = TRUE)
reason_cols_other <- grep("^NOimmReas_Child.*_other$", names(AE), value = TRUE)

AE_reasons <- AE |>
  mutate(row_id___ = row_number())

reason_long_main <- AE_reasons |>
  select(
    row_id___, Country, Region, District, Response, roundNumber, Vaccine.type,
    all_of(reason_cols_main)
  ) |>
  pivot_longer(
    cols = all_of(reason_cols_main),
    names_to = "reason_source_col",
    values_to = "reason_raw"
  ) |>
  mutate(reason_raw = normalize_reason_text(reason_raw)) |>
  filter(!is.na(reason_raw), reason_raw != "")

reason_long_other <- AE_reasons |>
  select(row_id___, all_of(reason_cols_other)) |>
  pivot_longer(
    cols = all_of(reason_cols_other),
    names_to = "reason_other_source_col",
    values_to = "reason_other_raw"
  ) |>
  mutate(
    reason_other_raw = normalize_reason_text(reason_other_raw),
    reason_source_col = str_remove(reason_other_source_col, "_other$")
  ) |>
  filter(!is.na(reason_other_raw), reason_other_raw != "")

reason_long <- reason_long_main |>
  left_join(
    reason_long_other |> select(row_id___, reason_source_col, reason_other_raw),
    by = c("row_id___", "reason_source_col")
  ) |>
  mutate(
    reason_final = case_when(
      !is.na(reason_other_raw) & reason_other_raw != "" ~ reason_other_raw,
      TRUE ~ reason_raw
    )
  ) |>
  filter(!is.na(reason_final), reason_final != "")

# ============================================================
# 5) CLASSIFY EACH CHILD REASON ONCE
# ============================================================

reason_long <- reason_long |>
  mutate(
    main_reason = detect_main_reason(reason_final),
    abs_reason = if_else(main_reason == "r_childabsent", detect_abs_reason(reason_final), NA_character_),
    nc_reason  = if_else(main_reason == "r_non_compliance", detect_nc_reason(reason_final), NA_character_)
  )

# ============================================================
# 6) BUILD WIDE TABLES OF STANDARDIZED REASONS
# ============================================================

main_reason_wide <- build_reason_wide_std(reason_long, "main_reason")
abs_reason_wide  <- build_reason_wide_std(reason_long, "abs_reason")
nc_reason_wide   <- build_reason_wide_std(reason_long, "nc_reason")

main_reason_vars <- c(
  "r_childabsent", "r_house_not_visited", "r_vaccinated_but_not_FM",
  "r_child_was_asleep", "r_child_is_a_visitor", "r_non_compliance",
  "r_childnotborn", "r_security", "other_r"
)

abs_reason_vars <- c(
  "abs_reason_other", "abs_reason_travelled", "abs_reason_farm",
  "abs_reason_market", "abs_reason_school", "abs_reason_in_playground"
)

nc_reason_vars <- c(
  "nc_reason_no_felt_need", "nc_reason_child_sick",
  "nc_reason_vaccines_safety", "nc_reason_religious_cultural",
  "nc_reason_no_care_giver_consent", "nc_reason_poliofree",
  "nc_reason_too_many_rnd", "nc_reason_others",
  "nc_reason_covid_19", "nc_reason_nopvconcern"
)

for (v in setdiff(main_reason_vars, names(main_reason_wide))) main_reason_wide[[v]] <- 0L
for (v in setdiff(abs_reason_vars, names(abs_reason_wide))) abs_reason_wide[[v]] <- 0L
for (v in setdiff(nc_reason_vars, names(nc_reason_wide))) nc_reason_wide[[v]] <- 0L

# ============================================================
# 6B) SOCIAL MOBILIZATION PARSING + AGGREGATION
# ============================================================

sm_cols <- grep("^SourceInfo_house", names(AE), value = TRUE, ignore.case = TRUE)

sm_map <- c(
  "1"  = "sm_radio",
  "2"  = "sm_tv",
  "3"  = "sm_health_worker",
  "4"  = "sm_community_leader",
  "5"  = "sm_religious_leader",
  "6"  = "sm_town_crier",
  "7"  = "sm_mobile_announcement",
  "8"  = "sm_house_to_house",
  "9"  = "sm_family_friends",
  "10" = "sm_school",
  "11" = "sm_market",
  "12" = "sm_social_media",
  "13" = "sm_other"
)

sm_vars <- unique(unname(sm_map))

if (length(sm_cols) > 0) {
  
  sm_long <- AE |>
    mutate(row_id___ = row_number()) |>
    select(
      row_id___, Country, Region, District, Response, roundNumber, Vaccine.type,
      all_of(sm_cols)
    ) |>
    pivot_longer(
      cols = all_of(sm_cols),
      names_to = "sm_col",
      values_to = "sm_raw"
    ) |>
    mutate(
      sm_raw = trimws(tolower(as.character(sm_raw))),
      sm_raw = na_if(sm_raw, ""),
      sm_raw = na_if(sm_raw, "na"),
      sm_raw = na_if(sm_raw, "nan"),
      sm_raw = na_if(sm_raw, "null")
    ) |>
    filter(!is.na(sm_raw)) |>
    separate_rows(sm_raw, sep = "\\s+") |>
    mutate(
      sm_code = str_trim(sm_raw),
      sm_type = unname(sm_map[sm_code])
    ) |>
    filter(!is.na(sm_type), sm_type != "")
  
  sm_wide <- sm_long |>
    count(Country, Region, District, Response, roundNumber, Vaccine.type, sm_type, name = "n") |>
    pivot_wider(
      names_from = sm_type,
      values_from = n,
      values_fill = 0
    )
  
  for (v in setdiff(sm_vars, names(sm_wide))) sm_wide[[v]] <- 0L
  
  sm_wide <- sm_wide |>
    mutate(
      sm_total_sources = rowSums(across(all_of(sm_vars)), na.rm = TRUE)
    )
  
} else {
  
  sm_wide <- AE |>
    distinct(Country, Region, District, Response, roundNumber, Vaccine.type)
  
  for (v in sm_vars) sm_wide[[v]] <- 0L
  
  sm_wide <- sm_wide |>
    mutate(sm_total_sources = 0L)
}

# ============================================================
# 7) BASE REPOSITORY AGGREGATION
# ============================================================

AK_base <- AE |>
  select(
    Country, Region, District, date, Response, Vaccine.type, roundNumber,
    month, year, u5_present, u5_FM, missed_child
  ) |>
  group_by(Country, Region, District, Response, Vaccine.type, roundNumber) |>
  summarise(
    start_date = min(date, na.rm = TRUE),
    end_date   = max(date, na.rm = TRUE),
    year       = year(start_date),
    u5_present = sum(u5_present, na.rm = TRUE),
    u5_FM      = sum(u5_FM, na.rm = TRUE),
    missed_child = sum(missed_child, na.rm = TRUE),
    cv = round(u5_FM / u5_present, 2),
    .groups = "drop"
  )

# ============================================================
# 8) MERGE REASONS + SM INTO FINAL REPOSITORY
# ============================================================

AK <- AK_base |>
  left_join(
    main_reason_wide |>
      select(Country, Region, District, Response, roundNumber, Vaccine.type, all_of(main_reason_vars)),
    by = c("Country", "Region", "District", "Response", "roundNumber", "Vaccine.type")
  ) |>
  left_join(
    abs_reason_wide |>
      select(Country, Region, District, Response, roundNumber, Vaccine.type, all_of(abs_reason_vars)),
    by = c("Country", "Region", "District", "Response", "roundNumber", "Vaccine.type")
  ) |>
  left_join(
    nc_reason_wide |>
      select(Country, Region, District, Response, roundNumber, Vaccine.type, all_of(nc_reason_vars)),
    by = c("Country", "Region", "District", "Response", "roundNumber", "Vaccine.type")
  ) |>
  left_join(
    sm_wide |>
      select(Country, Region, District, Response, roundNumber, Vaccine.type, all_of(sm_vars), sm_total_sources),
    by = c("Country", "Region", "District", "Response", "roundNumber", "Vaccine.type")
  ) |>
  mutate(
    across(all_of(c(main_reason_vars, abs_reason_vars, nc_reason_vars, sm_vars)), ~ replace_na(., 0L)),
    sm_total_sources = replace_na(sm_total_sources, 0L),
    total_main_reasons =
      r_childabsent + r_house_not_visited + r_vaccinated_but_not_FM +
      r_child_was_asleep + r_child_is_a_visitor + r_non_compliance +
      r_childnotborn + r_security + other_r
  )

# ============================================================
# 9) QC
# ============================================================

cat("\nMain reason totals:\n")
print(colSums(AK |> select(all_of(main_reason_vars)) |> as.data.frame(), na.rm = TRUE))

cat("\nAbsence reason totals:\n")
print(colSums(AK |> select(all_of(abs_reason_vars)) |> as.data.frame(), na.rm = TRUE))

cat("\nNon-compliance reason totals:\n")
print(colSums(AK |> select(all_of(nc_reason_vars)) |> as.data.frame(), na.rm = TRUE))

cat("\nSocial mobilization totals:\n")
print(colSums(AK |> select(all_of(sm_vars)) |> as.data.frame(), na.rm = TRUE))

cat("\nRows where total_main_reasons > missed_child:\n")
print(
  AK |>
    filter(total_main_reasons > missed_child) |>
    select(Country, Region, District, Response, roundNumber, missed_child, total_main_reasons) |>
    head(20)
)

# ============================================================
# 10) EXPORT
# ============================================================

write_csv(AK, out_file)

cat("\nNigeria IM repository successfully written to:\n", out_file, "\n")