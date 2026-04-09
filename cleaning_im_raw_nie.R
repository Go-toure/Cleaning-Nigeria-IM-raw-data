
library(data.table)
library(stringr)
library(qs)
rds_file <- "C:/Users/TOURE/Documents/PADACORD/IM/7178.rds"

file.info(rds_file)$size

data <- qread(rds_file)

dt <- as.data.table(data)

# #columns starting with Reason
reason_cols <- grep("^NOimmReas", names(dt), value = TRUE, ignore.case = TRUE)

reason_cols

# distinct values
all_values_clean <- unique(
  trimws(tolower(unlist(dt[, ..reason_cols])))
)

all_values_clean <- all_values_clean[!is.na(all_values_clean) & all_values_clean != ""]

all_values_clean


#Clean step-by-step
clean_values <- all_values_clean[!grepl("^\\d+$", all_values_clean)]
clean_values <- clean_values[!grepl("^[a-zA-Z]$", clean_values)]
clean_values <- clean_values[nchar(clean_values) > 2]
clean_values

#classification

# ============================================================
# 1) Normalize raw free-text reason
# ============================================================
normalize_reason_text <- function(x) {
  x <- tolower(trimws(x))
  x <- iconv(x, from = "", to = "ASCII//TRANSLIT", sub = "")
  x <- gsub("[\r\n\t]+", " ", x)
  x <- gsub("[[:punct:]]+", " ", x)
  x <- gsub("\\s+", " ", x)
  x <- trimws(x)
  x
}

# ============================================================
# 2) Detect main AFRO reason
#    Returns one of:
#    r_childabsent, r_house_not_visited, r_vaccinated_but_not_FM,
#    r_child_was_asleep, r_child_is_a_visitor, r_non_compliance,
#    r_childnotborn, r_security, other_r
# ============================================================
detect_main_reason <- function(x) {
  x0 <- normalize_reason_text(x)
  
  if (is.na(x0) || x0 == "") return(NA_character_)
  
  # ---- child not born / newborn ----
  if (str_detect(x0,
                 "\\bnew born\\b|\\bnewborn\\b|\\bnew birth\\b|\\bnewly born\\b|\\ba day old\\b|\\bthree days old\\b|\\bjust gave birth\\b|\\bgiven birth\\b|\\bwas born yesterday\\b|\\bdelivered on\\b|\\bbaby has four days\\b|\\bbaby was just born\\b|\\bzero dose\\b"
  )) return("r_childnotborn")
  
  # ---- security ----
  if (str_detect(x0,
                 "\\bsecurity\\b|security related issues"
  )) return("r_security")
  
  # ---- vaccinated but not finger marked ----
  if (str_detect(x0,
                 "finger mark|finger marked|finger marking|not finger marked|no mark on left finger|fingers not mark|mark was seen|mark was not seen|cleaned off|wrongly finger marked|vaccinated but was not marked|immunized but no mark|immunized at different occasions|received routine opv|received vaccine last month|vaccinated but no mark"
  )) return("r_vaccinated_but_not_FM")
  
  # ---- house not visited ----
  if (str_detect(x0,
                 "team not visited|team not visit|team did not visit|team didn t visit|household not visited|house was not visit|house not visited|not revisited|never revisited|no revisit|revisit household|team omitted|did not make effort|didn t make effort|team failed to check|questions were not asked|didn t ask|did not ask|team didn t see the children|team didn t immunise|team visited but.*not.*effort|team got to the house but.*not.*effort|missed by the team"
  )) return("r_house_not_visited")
  
  # ---- child asleep ----
  if (str_detect(x0,
                 "\\basleep\\b|\\bsleeping\\b|\\bwas sleeping\\b|\\bchild sleeping\\b|\\bchild was sleep\\b|\\bchild was asleep\\b|\\bwas slept\\b|\\bat sleep\\b"
  )) return("r_child_was_asleep")
  
  # ---- visitor ----
  if (str_detect(x0,
                 "\\bvisitor\\b|\\bvisitors\\b|came for visiting|came for a visit|came visiting|came on visit|visiting child|visiting parent|just arrived|just came|just came back|moved in|from outside the settlement|from another state|from other state|not from the settlement|came for holiday|holiday|sallah festival|omugwo|from village|from ibadan|from kano|from lagos|visit child from other lga|newly located|packed in"
  )) return("r_child_is_a_visitor")
  
  # ---- non-compliance / refusal ----
  if (str_detect(x0,
                 "non compliance|noncompliance|refusal|refused|rejection|does not want|do not allow|did not allow|father refused|father did not allow|father do not allow|mother requested not to give|not intrested|not interested|religious|traditional|cultural|no felt need|polio can be cured|too many rounds|too many round|too many rnd|no caregiver consent|no care giver consent|no parental consent|caregiver refusal|scared|afraid|vaccine.*safe|vaccines.*safe|negative way|not been educated|ignorance"
  )) return("r_non_compliance")
  
  # ---- child absent ----
  # This is intentionally broad for Nigeria IM
  if (str_detect(x0,
                 "absent|abcent|absant|absend|abcend|abset|absence|absences|not around|not arround|not arrnd|not a round|not at home|not home|not present|not found at home|not in the house|not in house|not in area|not available|not seen|not met|wasn t present|wasnt present|wasn t around|wasnt around|wasn t home|wasnt home|away during|away with|went out|out of house|market|farm|school|play ground|playground|playing ground|social event|event center|church|travel|travelled|travelling|traveling|journey|transit|errand|river|work|office|wedding|ceremony|burial|meeting"
  )) return("r_childabsent")
  
  # ---- fallback ----
  "other_r"
}

# ============================================================
# 3) Detailed ABS reason
#    Returns:
#    abs_reason_other, abs_reason_travelled, abs_reason_farm,
#    abs_reason_market, abs_reason_school, abs_reason_in_playground
# ============================================================
detect_abs_reason <- function(x) {
  x0 <- normalize_reason_text(x)
  
  if (is.na(x0) || x0 == "") return(NA_character_)
  
  if (str_detect(x0, "travel|travelled|travelling|traveling|journey|transit|out of town|other state|outside the lga|trip")) {
    return("abs_reason_travelled")
  }
  
  if (str_detect(x0, "farm|farming|bush|firewood")) {
    return("abs_reason_farm")
  }
  
  if (str_detect(x0, "market|shop")) {
    return("abs_reason_market")
  }
  
  if (str_detect(x0, "school|schools|shool|sch|islamiya|modiraza|qur an school")) {
    return("abs_reason_school")
  }
  
  if (str_detect(x0, "play ground|playground|playing ground|play graunt|play grouwn|play grand|play groud|went to play|he was playing")) {
    return("abs_reason_in_playground")
  }
  
  return("abs_reason_other")
}

# ============================================================
# 4) Detailed NC reason
#    Returns:
#    nc_reason_no_felt_need, nc_reason_child_sick,
#    nc_reason_vaccines_safety, nc_reason_religious_cultural,
#    nc_reason_no_care_giver_consent, nc_reason_poliofree,
#    nc_reason_too_many_rnd, nc_reason_others,
#    nc_reason_covid_19, nc_reason_nopvconcern
# ============================================================
detect_nc_reason <- function(x) {
  x0 <- normalize_reason_text(x)
  
  if (is.na(x0) || x0 == "") return(NA_character_)
  
  if (str_detect(x0, "religious|cultural|traditional|announcement from mosque")) {
    return("nc_reason_religious_cultural")
  }
  
  if (str_detect(x0, "polio can be cured|polio free|polio has been eradicated|poliofree")) {
    return("nc_reason_poliofree")
  }
  
  if (str_detect(x0, "vaccine.*safe|vaccines.*safe|afraid|scared|negative way|reacted on child|interaction|safety")) {
    return("nc_reason_vaccines_safety")
  }
  
  if (str_detect(x0, "no felt need|no need|no perceived need")) {
    return("nc_reason_no_felt_need")
  }
  
  if (str_detect(x0, "too many rounds|too many round|too many rnd")) {
    return("nc_reason_too_many_rnd")
  }
  
  if (str_detect(x0, "father refused|father did not allow|father do not allow|does not want|do not allow|did not allow|no caregiver consent|no care giver consent|no parental consent|caregiver refusal|mother requested not to give|refusal|refused|rejection")) {
    return("nc_reason_no_care_giver_consent")
  }
  
  if (str_detect(x0, "child is sick|child was sick|child sick|child seek|child was ill|not healthy|unwell|hospital|admitted|not well|no medicine|physically fit")) {
    return("nc_reason_child_sick")
  }
  
  if (str_detect(x0, "covid")) {
    return("nc_reason_covid_19")
  }
  
  if (str_detect(x0, "nopv|n opv")) {
    return("nc_reason_nopvconcern")
  }
  
  return("nc_reason_others")
}

# ============================================================
# 5) Apply to distinct values first
#    clean_values = your distinct cleaned reason texts
# ============================================================
reason_map <- data.table(raw_reason = clean_values)

reason_map[, main_reason := vapply(raw_reason, detect_main_reason, character(1))]
reason_map[, abs_detail  := fifelse(main_reason == "r_childabsent",
                                    vapply(raw_reason, detect_abs_reason, character(1)),
                                    NA_character_)]
reason_map[, nc_detail   := fifelse(main_reason == "r_non_compliance",
                                    vapply(raw_reason, detect_nc_reason, character(1)),
                                    NA_character_)]

# Review mapping
reason_map

# ============================================================
# 6) OPTIONAL: inspect what still falls into other_r
# ============================================================
reason_map[main_reason == "other_r"][order(raw_reason)]

# ============================================================
# 7) Apply to full data table dt
#    Replace NOimmReas columns with your actual reason columns if needed
# ============================================================
reason_cols <- grep("^NOimmReas", names(dt), value = TRUE, ignore.case = TRUE)

# Create a single harmonized free-text reason per record
# (first non-empty reason among NOimmReas columns)
dt[, raw_reason_harmonized := {
  vals <- unlist(.SD, use.names = FALSE)
  vals <- vals[!is.na(vals) & trimws(vals) != ""]
  if (length(vals) == 0) NA_character_ else vals[1]
}, by = seq_len(nrow(dt)), .SDcols = reason_cols]

# Normalize + classify
dt[, main_reason := vapply(raw_reason_harmonized, detect_main_reason, character(1))]
dt[, abs_detail  := fifelse(main_reason == "r_childabsent",
                            vapply(raw_reason_harmonized, detect_abs_reason, character(1)),
                            NA_character_)]
dt[, nc_detail   := fifelse(main_reason == "r_non_compliance",
                            vapply(raw_reason_harmonized, detect_nc_reason, character(1)),
                            NA_character_)]

# ============================================================
# 8) Create AFRO-standard binary columns
# ============================================================
main_reason_vars <- c(
  "r_childabsent", "r_house_not_visited", "r_vaccinated_but_not_FM",
  "r_child_was_asleep", "r_child_is_a_visitor", "r_non_compliance",
  "r_childnotborn", "r_security", "other_r"
)

for (v in main_reason_vars) {
  dt[, (v) := as.integer(main_reason == v)]
}

abs_reason_vars <- c(
  "abs_reason_other", "abs_reason_travelled", "abs_reason_farm",
  "abs_reason_market", "abs_reason_school", "abs_reason_in_playground"
)

for (v in abs_reason_vars) {
  dt[, (v) := as.integer(abs_detail == v)]
}

nc_reason_vars <- c(
  "nc_reason_no_felt_need", "nc_reason_child_sick",
  "nc_reason_vaccines_safety", "nc_reason_religious_cultural",
  "nc_reason_no_care_giver_consent", "nc_reason_poliofree",
  "nc_reason_too_many_rnd", "nc_reason_others",
  "nc_reason_covid_19", "nc_reason_nopvconcern"
)

for (v in nc_reason_vars) {
  dt[, (v) := as.integer(nc_detail == v)]
}

# ============================================================
# 9) Quality checks
# ============================================================
# Main reason distribution
dt[, .N, by = main_reason][order(-N)]

# Detailed ABS distribution
dt[main_reason == "r_childabsent", .N, by = abs_detail][order(-N)]

# Detailed NC distribution
dt[main_reason == "r_non_compliance", .N, by = nc_detail][order(-N)]

# Values still classified as other_r
unique(dt[main_reason == "other_r", raw_reason_harmonized])

# ============================================================
# 10) Optional export of dictionary review
# ============================================================
# fwrite(reason_map, "C:/Users/TOURE/Documents/PADACORD/IM/nigeria_im_reason_dictionary_review.csv")
