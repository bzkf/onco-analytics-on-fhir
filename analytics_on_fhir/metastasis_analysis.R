###############################################################
# Author: Brigitte Kühnel
# Date: 2026-02-12
# Description: Zweittumor - pivot-based pipeline with extensive checks
###############################################################

# ---- Setup ----
library(dplyr)
library(tidyr)
library(lubridate)
library(ggplot2)
library(gridExtra)
library(scales)
library(survival)
library(here)

HERE <- here::here()

# ----------------------------------------------------------
# PARAMETER: First metastasis of tumor 1 must be at least this many months before the diagnosis date of tumor 2
# ----------------------------------------------------------

meta_to_T2_months <- 1
#meta_to_T2_months <- 0
#meta_to_T2_months <- 6
analysis_tag <- paste0("T1meta_before_T2dx_", meta_to_T2_months, "m")


# ----------------------------------------------------------
# Paths
# ----------------------------------------------------------
base_dir <- file.path(HERE, "results", "study_protocol_d")
cat("base_dir :", base_dir, "\n")
infile   <- file.path(base_dir, "df_all_pivot.csv")

# All outputs go here
out_dir  <- file.path(base_dir, "plots_r")
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

setwd(base_dir)

# Optional: write a log (so you have all tables/heads saved)
logfile <- file.path(out_dir, paste0("run_log_checks_", analysis_tag, ".txt"))
sink(logfile, split = TRUE)
cat("Run timestamp:", as.character(Sys.time()), "\n")
cat("Working directory:", getwd(), "\n")
cat("Input file:", infile, "\n")
cat("Output dir:", out_dir, "\n\n")

# ----------------------------------------------------------
# Load pivot dataset
# ----------------------------------------------------------
dfp <- read.csv(infile, sep = ";", header = TRUE, stringsAsFactors = FALSE)

cat("==== RAW INPUT CHECKS ====\n")
cat("dim(dfp):\n"); print(dim(dfp))
cat("\ncolnames(dfp):\n"); print(colnames(dfp))
cat("\nhead(dfp):\n"); print(head(dfp, 3))

# Required columns check
required_cols <- c(
  "patient_resource_id",
  "condition_id_1", "condition_id_2",
  "asserted_date_1", "asserted_date_2",
  "icd10_code_1", "icd10_code_2",
  "metastasis_loc_1", "metastasis_loc_2",
  "metastasis_date_first_1", "metastasis_date_first_2"
)

missing_cols <- setdiff(required_cols, colnames(dfp))
cat("\nMissing required columns:\n"); print(missing_cols)
if (length(missing_cols) > 0) {
  stop(paste0("STOP: Missing required columns: ", paste(missing_cols, collapse = ", ")))
}


# Remove tumor with first tumor date = 1.1.1970
dfp <- dfp %>%
  filter(!is.na(asserted_date_1) & asserted_date_1  != "1970-01-01")
# Remove tumor with first tumor date = 1.7.1970
dfp <- dfp %>%
  filter(!is.na(asserted_date_1) & asserted_date_1  != "1970-07-01")

# Keep only patients with registered second tumor
dfp <- dfp %>%
  filter(!is.na(condition_id_2) & condition_id_2 != "")

cat("\n==== AFTER FILTER: condition_id_2 present ====\n")
cat("dim(dfp):\n"); print(dim(dfp))

# ----------------------------------------------------------
# Helper: ICD -> Entity label
# If code does not fall into an entity, return 3-character ICD (e.g., C49, C91, D09)
# ----------------------------------------------------------
get_entity_label <- function(icd) {

  if (is.na(icd) || trimws(icd) == "") return(NA_character_)

  icd <- toupper(trimws(icd))

  # Extract letter and first two digits (3-character ICD category)
  letter <- substr(icd, 1, 1)
  two_digits <- substr(icd, 2, 3)

  # If digits are not available, return NA
  if (!grepl("^[0-9]{2}$", two_digits)) return(NA_character_)

  number <- suppressWarnings(as.numeric(two_digits))

  # Default fallback: return 3-character ICD category (no dot)
  fallback_3char <- paste0(letter, two_digits)

  if (letter == "C") {
    if (!is.na(number) && number >= 0  && number <= 14) return("Lippe, Mundhöhle und Rachen")
    if (!is.na(number) && number == 15) return("Speiseröhre")
    if (!is.na(number) && number == 16) return("Magen")
    if (!is.na(number) && number >= 18 && number <= 21) return("Dickdarm und Rektum")
    if (!is.na(number) && number == 22) return("Leber")
    if (!is.na(number) && number >= 23 && number <= 24) return("Gallenblase und Gallenwege")
    if (!is.na(number) && number == 25) return("Bauchspeicheldrüse")
    if (!is.na(number) && number == 32) return("Kehlkopf")
    if (!is.na(number) && number >= 33 && number <= 34) return("Trachea, Bronchien und Lunge")
    if (!is.na(number) && number == 43) return("Malignes Melanom der Haut")
    if (!is.na(number) && number == 50) return("Brust")
    if (!is.na(number) && number == 53) return("Gebärmutterhals")
    if (!is.na(number) && number >= 54 && number <= 55) return("Gebärmutterkörper")
    if (!is.na(number) && number == 56) return("Eierstöcke")
    if (!is.na(number) && number == 61) return("Prostata")
    if (!is.na(number) && number == 62) return("Hoden")
    if (!is.na(number) && number == 64) return("Niere")
    if (!is.na(number) && number == 67) return("Harnblase")
    if (!is.na(number) && number >= 70 && number <= 72) return("Gehirn und zentrales Nervensystem")
    if (!is.na(number) && number == 73) return("Schilddrüse")
    if (!is.na(number) && number == 81) return("Morbus Hodgkin")
    if (!is.na(number) && number >= 82 && number <= 88) return("Non-Hodgkin-Lymphome")
    if (!is.na(number) && number == 90) return("Plasmozytom")
    if (!is.na(number) && number >= 91 && number <= 95) return("Leukämien")
    if (!is.na(number) && number == 96) return("Non-Hodgkin-Lymphome")

    # Not mapped -> return 3-character ICD (e.g., C49)
    return(fallback_3char)
  }

  if (letter == "D") {
    if (!is.na(number) && number == 5) return("Brust")
    if (!is.na(number) && number == 6) return("Gebärmutterhals")

    # Keep special D-codes if present, otherwise fall back to 3-char (e.g., D09)
    if (icd == "D09.0") return("Harnblase")
    if (icd == "D39.1") return("Eierstöcke")
    if (icd == "D41.4") return("Harnblase")

    # Not mapped -> return 3-character ICD (e.g., D09)
    return(fallback_3char)
  }

  # For any other letter (rare in your context): return 3-char category if possible
  return(fallback_3char)
}



# ----------------------------------------------------------
# Compact date parsing + focused tests (formats, 1970, consistency, timediff)
# ----------------------------------------------------------

# --- 1) Parser (handles ISO, ISO-datetime, YYYY-MM, YYYY, DD.MM.YYYY, slashes, numeric-like; protects "0" -> 1970) ---
parse_mixed_date <- function(x, fill = c("mid", "start")) {
  fill <- match.arg(fill)

  raw <- trimws(as.character(x))
  raw[raw == ""] <- NA_character_

  # strip ISO datetime to date (ONLY on non-NA elements to avoid recycling)
  norm <- raw
  i <- !is.na(norm)
  norm[i] <- sub("^([0-9]{4}-[0-9]{2}-[0-9]{2}).*$", "\\1", norm[i])

  # fill partial dates
  full <- norm
  is_month <- !is.na(full) & grepl("^\\d{4}-\\d{2}$", full)
  is_year  <- !is.na(full) & grepl("^\\d{4}$", full)

  if (fill == "start") {
    full[is_month] <- paste0(full[is_month], "-01")
    full[is_year]  <- paste0(full[is_year], "-01-01")
  } else {
    full[is_month] <- paste0(full[is_month], "-15")
    full[is_year]  <- paste0(full[is_year], "-07-01")
  }

  parsed <- suppressWarnings(lubridate::ymd(full))
  miss <- is.na(parsed) & !is.na(full)

  # dmy (incl. DD.MM.YYYY)
  parsed[miss] <- suppressWarnings(lubridate::dmy(full[miss]))
  miss <- is.na(parsed) & !is.na(full)

  # slash variants
  tmp <- gsub("/", "-", full[miss])
  parsed[miss] <- suppressWarnings(lubridate::ymd(tmp))
  miss <- is.na(parsed) & !is.na(full)

  tmp <- gsub("/", "-", full[miss])
  d1 <- suppressWarnings(lubridate::dmy(tmp))
  d2 <- suppressWarnings(lubridate::mdy(tmp))
  d1[is.na(d1)] <- d2[is.na(d1)]
  parsed[miss] <- d1

  # numeric-like rescue
  miss <- is.na(parsed) & !is.na(full)
  idx_num <- which(miss & grepl("^\\d+$", full))
  if (length(idx_num) > 0) {
    vals <- suppressWarnings(as.numeric(full[idx_num]))
    vals[vals == 0] <- NA_real_  # prevent epoch artifacts from 0

    out <- rep(as.Date(NA), length(vals))
    is_excel   <- !is.na(vals) & vals >= 20000 & vals <= 80000
    is_unix_s  <- !is.na(vals) & vals > 1e8 & vals <= 1e11
    is_unix_ms <- !is.na(vals) & vals > 1e11

    out[is_excel]   <- as.Date(vals[is_excel], origin = "1899-12-30")
    out[is_unix_s]  <- as.Date(as.POSIXct(vals[is_unix_s], origin = "1970-01-01", tz = "UTC"))
    out[is_unix_ms] <- as.Date(as.POSIXct(vals[is_unix_ms] / 1000, origin = "1970-01-01", tz = "UTC"))
    parsed[idx_num] <- out
  }

  precision <- rep("unknown", length(norm))
  precision[!is.na(norm) & grepl("^\\d{4}-\\d{2}-\\d{2}$", norm)] <- "day"
  precision[!is.na(norm) & grepl("^\\d{4}-\\d{2}$", norm)]        <- "month"
  precision[!is.na(norm) & grepl("^\\d{4}$", norm)]               <- "year"

  list(raw = raw, full = full, date = parsed, precision = precision)
}


# --- 2) One compact runner for 4 columns + tests ---
run_date_block <- function(dfp, fill = "mid", seed = 123, n = 30) {

  cols <- c("asserted_date_1", "asserted_date_2", "metastasis_date_first_1", "metastasis_date_first_2")
  stopifnot(all(cols %in% colnames(dfp)))

  p1 <- parse_mixed_date(dfp$asserted_date_1, fill = fill)
  p2 <- parse_mixed_date(dfp$asserted_date_2, fill = fill)
  m1 <- parse_mixed_date(dfp$metastasis_date_first_1, fill = fill)
  m2 <- parse_mixed_date(dfp$metastasis_date_first_2, fill = fill)

  dfp <- dfp %>%
    mutate(
      asserted_date_1_raw  = p1$raw, asserted_date_1_full  = p1$full, date1 = p1$date, date1_precision = p1$precision,
      asserted_date_2_raw  = p2$raw, asserted_date_2_full  = p2$full, date2 = p2$date, date2_precision = p2$precision,
      metastasis_date_first_1_raw  = m1$raw, metastasis_date_first_1_full = m1$full, meta_date1 = m1$date, meta1_precision = m1$precision,
      metastasis_date_first_2_raw  = m2$raw, metastasis_date_first_2_full = m2$full, meta_date2 = m2$date, meta2_precision = m2$precision
    )

  epoch <- as.Date("1970-01-01")

  # --- A) Overview table per column (counts, NAs, 1970, precision)
  compact_summary <- function(raw, full, parsed, precision) {
    raw0 <- trimws(as.character(raw)); raw0[raw0 == ""] <- NA_character_
    data.frame(
      n_total = length(raw0),
      n_nonempty = sum(!is.na(raw0)),
      n_parsed = sum(!is.na(parsed)),
      n_na = sum(is.na(parsed)),
      n_1970 = sum(!is.na(parsed) & parsed == epoch),
      prec_day = sum(precision == "day" & !is.na(raw0)),
      prec_month = sum(precision == "month" & !is.na(raw0)),
      prec_year = sum(precision == "year" & !is.na(raw0)),
      stringsAsFactors = FALSE
    )
  }

  cat("\n================ DATE PARSE SUMMARY (fill=", fill, ") ================\n", sep = "")
  summary_tbl <- bind_rows(
    cbind(col = "asserted_date_1", compact_summary(dfp$asserted_date_1_raw, dfp$asserted_date_1_full, dfp$date1, dfp$date1_precision)),
    cbind(col = "asserted_date_2", compact_summary(dfp$asserted_date_2_raw, dfp$asserted_date_2_full, dfp$date2, dfp$date2_precision)),
    cbind(col = "metastasis_date_first_1", compact_summary(dfp$metastasis_date_first_1_raw, dfp$metastasis_date_first_1_full, dfp$meta_date1, dfp$meta1_precision)),
    cbind(col = "metastasis_date_first_2", compact_summary(dfp$metastasis_date_first_2_raw, dfp$metastasis_date_first_2_full, dfp$meta_date2, dfp$meta2_precision))
  )
  print(summary_tbl)

  # --- B) Consistency test (this catches your "2013-08-09 -> 2023-..." issue immediately)
  consistency_check <- function(raw, full, precision, label) {
    raw0 <- trimws(as.character(raw)); raw0[raw0 == ""] <- NA_character_
    full0 <- trimws(as.character(full)); full0[full0 == ""] <- NA_character_
    bad <- which(!is.na(raw0) & grepl("^\\d{4}-\\d{2}-\\d{2}$", raw0) &
                   precision == "day" & !is.na(full0) & raw0 != full0)
    cat("\n--- CONSISTENCY:", label, "---\n")
    cat("Bad rows (precision=day AND raw ISO date but full != raw):", length(bad), "\n")
    if (length(bad) > 0) {
      i <- head(bad, 20)
      print(data.frame(raw = raw0[i], full = full0[i], precision = precision[i], stringsAsFactors = FALSE))
    }
  }

  consistency_check(dfp$metastasis_date_first_1_raw, dfp$metastasis_date_first_1_full, dfp$meta1_precision, "meta_date1")
  consistency_check(dfp$metastasis_date_first_2_raw, dfp$metastasis_date_first_2_full, dfp$meta2_precision, "meta_date2")

  # --- C) 1970 raw values table (top raw values)
  show_1970 <- function(raw, full, parsed, precision, label) {
    raw0 <- trimws(as.character(raw)); raw0[raw0 == ""] <- NA_character_
    idx <- which(!is.na(parsed) & parsed == epoch)
    cat("\n--- 1970 CASES:", label, "---\n")
    cat("Count:", length(idx), "\n")
    if (length(idx) > 0) {
      top <- head(sort(table(raw0[idx]), decreasing = TRUE), 20)
      cat("Top raw values causing 1970:\n"); print(top)
      cat("Examples (first 20):\n")
      i <- head(idx, 20)
      print(data.frame(raw = raw0[i], full = full[i], parsed = parsed[i], precision = precision[i], stringsAsFactors = FALSE))
    }
  }

  show_1970(dfp$asserted_date_1_raw, dfp$asserted_date_1_full, dfp$date1, dfp$date1_precision, "date1")
  show_1970(dfp$asserted_date_2_raw, dfp$asserted_date_2_full, dfp$date2, dfp$date2_precision, "date2")
  show_1970(dfp$metastasis_date_first_1_raw, dfp$metastasis_date_first_1_full, dfp$meta_date1, dfp$meta1_precision, "meta_date1")
  show_1970(dfp$metastasis_date_first_2_raw, dfp$metastasis_date_first_2_full, dfp$meta_date2, dfp$meta2_precision, "meta_date2")

  # --- D) Timediff checks (only if both dates exist)
  dfp_valid <- dfp %>%
    filter(!is.na(date1) & !is.na(date2)) %>%
    mutate(timediff_raw = as.numeric(date2 - date1))

  cat("\n================ TIMEDIFF CHECKS (date2 - date1) ================\n")
  cat("Valid rows:", nrow(dfp_valid), "\n")
  print(summary(dfp_valid$timediff_raw))
  cat("Neg/Zero/Pos:\n")
  print(c(
    negative = sum(dfp_valid$timediff_raw < 0, na.rm = TRUE),
    zero     = sum(dfp_valid$timediff_raw == 0, na.rm = TRUE),
    positive = sum(dfp_valid$timediff_raw > 0, na.rm = TRUE)
  ))
  if (sum(dfp_valid$timediff_raw < 0, na.rm = TRUE) > 0) {
    cat("\nExamples negative timediff (first 10):\n")
    print(head(dfp_valid %>% select(asserted_date_1_raw, asserted_date_2_raw, date1, date2, timediff_raw), 10))
  }

  # --- E) Random n (compact) for manual inspection (no NA)
  print_random <- function(raw, full, parsed, precision, label) {
    raw0 <- trimws(as.character(raw)); raw0[raw0 == ""] <- NA_character_
    idx <- which(!is.na(raw0) & !is.na(parsed) & !is.na(full))
    cat("\n==== RANDOM", n, "-", label, "(no NA) ====\n")
    if (length(idx) == 0) { cat("No rows available.\n"); return(invisible(NULL)) }
    set.seed(seed)
    take <- if (length(idx) >= n) sample(idx, n) else idx
    print(data.frame(raw = raw0[take], full = full[take], parsed = parsed[take], precision = precision[take], stringsAsFactors = FALSE))
  }

  print_random(dfp$metastasis_date_first_1_raw, dfp$metastasis_date_first_1_full, dfp$meta_date1, dfp$meta1_precision, "meta_date1")
  print_random(dfp$metastasis_date_first_2_raw, dfp$metastasis_date_first_2_full, dfp$meta_date2, dfp$meta2_precision, "meta_date2")

  list(dfp = dfp, dfp_valid = dfp_valid, summary_tbl = summary_tbl)
}

# --- Call once (replace your long block) ---
res <- run_date_block(dfp, fill = "mid", seed = 123, n = 30)
dfp <- res$dfp
dfp_valid <- res$dfp_valid

# ----------------------------------------------------------
# Entity labels for T1/T2 + checks
# ----------------------------------------------------------
dfp_valid <- dfp_valid %>%
  mutate(
    Entity_Label_T1 = vapply(icd10_code_1, get_entity_label, character(1)),
    Entity_Label_T2 = vapply(icd10_code_2, get_entity_label, character(1))
  )

cat("\n==== ENTITY LABEL CHECKS ====\n")
cat("NA Entity labels:\n")
print(c(
  na_T1 = sum(is.na(dfp_valid$Entity_Label_T1)),
  na_T2 = sum(is.na(dfp_valid$Entity_Label_T2))
))

cat("\nTop Entity_Label_T1:\n"); print(sort(table(dfp_valid$Entity_Label_T1), decreasing = TRUE))
cat("\nTop Entity_Label_T2:\n"); print(sort(table(dfp_valid$Entity_Label_T2), decreasing = TRUE))

# Export ICD/entity combos
ent <- dfp_valid %>%
  select(icd10_code_1, Entity_Label_T1, icd10_code_2, Entity_Label_T2) %>%
  distinct()

write.csv(ent,
          file = file.path(out_dir, paste0("Entity_Label_ICD10_combinations_pivot_", analysis_tag, ".csv")),
          row.names = FALSE)

# ----------------------------------------------------------
# Metastasis timing flags (reference = T2 diagnosis)
# - meta_at_first: T1 metastasis location present (legacy, kept for QA only)
# - meta_1_before_T2_interval: first T1 metastasis occurs at least meta_to_T2_months before T2 diagnosis (1/0)
# - meta_before_dx_T2: first T2 metastasis occurs before T2 diagnosis (1/0)
# - meta_before_dx_T1_interval_yes: yes/no label of meta_1_before_T2_interval (USE THIS FOR ALL DOWNSTREAM ANALYSES)
# ----------------------------------------------------------
dfp_valid <- dfp_valid %>%
  mutate(
    metastasis_loc_1 = ifelse(is.na(metastasis_loc_1), "", metastasis_loc_1),
    metastasis_loc_2 = ifelse(is.na(metastasis_loc_2), "", metastasis_loc_2),

    # Legacy definition (location-based). Kept only for plausibility checks.
    meta_at_first = ifelse(trimws(metastasis_loc_1) != "", "yes", "no"),

    # Metastasis of T1 at least X months before diagnosis of T2 (calendar-month logic)
    meta_1_before_T2_interval =
      ifelse(
        !is.na(meta_date1) & !is.na(date2) &
          meta_date1 <= (date2 %m-% months(meta_to_T2_months)),
        1L, 0L
      ),

    # Metastasis of T2 before diagnosis of T2
    meta_before_dx_T2 = ifelse(!is.na(meta_date2) & !is.na(date2) & meta_date2 < date2, 1L, 0L),

    # yes/no label used for all downstream analyses and plots
    meta_before_dx_T1_interval_yes = ifelse(meta_1_before_T2_interval == 1L, "yes", "no")
  )

cat("
==== META FLAG CHECKS ====
")
cat("table(meta_before_dx_T1_interval_yes):
"); print(table(dfp_valid$meta_before_dx_T1_interval_yes, useNA = "ifany"))
cat("
Counts (interval / T2-before-dx):
")
print(c(
  meta_before_dx_T1_interval_yes = sum(dfp_valid$meta_1_before_T2_interval == 1, na.rm = TRUE),
  meta_before_dx_T2_yes = sum(dfp_valid$meta_before_dx_T2 == 1, na.rm = TRUE)
))
cat("
Examples where meta_1_before_T2_interval == 1 (first 10):
")
print(head(dfp_valid %>% filter(meta_1_before_T2_interval == 1) %>%
             select(patient_resource_id, asserted_date_2, metastasis_date_first_1, date2, meta_date1), 10))
cat("
Examples where meta_before_dx_T2 == 1 (first 10):
")
print(head(dfp_valid %>% filter(meta_before_dx_T2 == 1) %>%
             select(patient_resource_id, asserted_date_2, metastasis_date_first_2, date2, meta_date2), 10))
# ----------------------------------------------------------
# BARPLOTS (using meta_before_dx_T1_interval_yes) + check tables
# ----------------------------------------------------------
cat("
==== BARPLOT INPUT TABLES ====
")
cat("Cross-tab Entity_Label_T2 x meta_before_dx_T1_interval_yes:
")
print(table(dfp_valid$Entity_Label_T2, dfp_valid$meta_before_dx_T1_interval_yes, useNA = "ifany"))

b <- ggplot(dfp_valid, aes(x = Entity_Label_T2, fill = meta_before_dx_T1_interval_yes)) +
  geom_bar(position = "stack") +
  theme_minimal() +
  labs(
    title = "Stacked Barplot: T1 metastasis >= X months before T2 diagnosis by Entity_Label_T2",
    x = "Entity_Label_T2 (Second tumor)",
    y = "Count",
    fill = "T1 meta >= X months before T2"
  ) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

ggsave(file.path(out_dir, paste0("Bar_MetaBeforeDxT1Interval_by_EntityLabel_T2_", analysis_tag, ".png")),
       plot = b, width = 12, height = 8, dpi = 300)

plot_data_pct <- dfp_valid %>%
  group_by(Entity_Label_T2, meta_before_dx_T1_interval_yes) %>%
  summarise(n = n(), .groups = "drop") %>%
  group_by(Entity_Label_T2) %>%
  mutate(percentage = n / sum(n) * 100)

cat("
Head(plot_data_pct):
"); print(head(plot_data_pct, 10))

pr <- ggplot(plot_data_pct, aes(x = Entity_Label_T2, y = percentage, fill = meta_before_dx_T1_interval_yes)) +
  geom_bar(stat = "identity", position = "fill") +
  scale_y_continuous(labels = percent_format(scale = 1)) +
  theme_minimal() +
  labs(
    title = "Percentage: T1 metastasis >= X months before T2 diagnosis by Entity_Label_T2",
    x = "Entity_Label_T2 (Second tumor)",
    y = "Percentage",
    fill = "T1 meta >= X months before T2"
  ) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

ggsave(file.path(out_dir, paste0("StackedBar_MetaBeforeDxT1Interval_by_EntityLabel_T2_", analysis_tag, ".png")),
       plot = pr, width = 12, height = 8, dpi = 300)

# ----------------------------------------------------------
# BUBBLE INPUT (explode metastasis_loc_1) restricted to meta_before_dx_T1_interval_yes == "yes" + extensive checks
# ----------------------------------------------------------
df_bubble <- dfp_valid %>%
  filter(meta_before_dx_T1_interval_yes == "yes") %>%
  transmute(
    ID = patient_resource_id,
    MetastaseLoc_T1 = trimws(metastasis_loc_1),
    Entity_Label_T1 = Entity_Label_T1,
    Entity_Label_T2 = Entity_Label_T2
  )

cat("\n==== BUBBLE PRECHECKS (before splitting) ====\n")
cat("dim(df_bubble):\n"); print(dim(df_bubble))
cat("\nHead(df_bubble):\n"); print(head(df_bubble, 10))
cat("\nEmpty MetastaseLoc_T1 count:\n")
print(sum(is.na(df_bubble$MetastaseLoc_T1) | df_bubble$MetastaseLoc_T1 == ""))

# How many have multiple metastasis locations in one cell?
multi_loc_n <- sum(grepl(",", df_bubble$MetastaseLoc_T1, fixed = TRUE) & df_bubble$MetastaseLoc_T1 != "", na.rm = TRUE)
cat("\nRows with comma-separated multiple MetastaseLoc_T1:", multi_loc_n, "\n")

# Explode to one metastasis location per row
df_bubble_long <- df_bubble %>%
  filter(!is.na(MetastaseLoc_T1) & MetastaseLoc_T1 != "",
         !is.na(Entity_Label_T2) & Entity_Label_T2 != "") %>%
  separate_rows(MetastaseLoc_T1, sep = "\\s*,\\s*") %>%
  mutate(MetastaseLoc_T1 = trimws(MetastaseLoc_T1)) %>%
  filter(MetastaseLoc_T1 != "")

cat("\n==== BUBBLE CHECKS (after splitting) ====\n")
cat("dim(df_bubble_long):\n"); print(dim(df_bubble_long))
cat("\nHead(df_bubble_long):\n"); print(head(df_bubble_long, 15))
cat("\nTop MetastaseLoc_T1 values:\n"); print(sort(table(df_bubble_long$MetastaseLoc_T1), decreasing = TRUE))
cat("\nTop Entity_Label_T1 values (bubble base):\n"); print(sort(table(df_bubble_long$Entity_Label_T1), decreasing = TRUE))

# Export combinations
write.csv(
  df_bubble_long %>% distinct(ID, MetastaseLoc_T1, Entity_Label_T1, Entity_Label_T2),
  file = file.path(out_dir, paste0("MetastaseLoc_T1_EntityLabel_T2_combinations_pivot_", analysis_tag, ".csv")),
  row.names = FALSE
)

# Overall bubble plot
plot_data_bub <- df_bubble_long %>%
  group_by(MetastaseLoc_T1, Entity_Label_T2) %>%
  summarise(n = n(), .groups = "drop")

cat("\nHead(plot_data_bub overall):\n"); print(head(plot_data_bub, 15))

p_bub <- ggplot(plot_data_bub, aes(x = MetastaseLoc_T1, y = Entity_Label_T2, size = n, color = n)) +
  geom_point(alpha = 0.7) +
  scale_size(range = c(3, 20)) +
  theme_minimal() +
  labs(
    title = paste0("Bubble Plot: MetastaseLoc_T1 vs. Entity_Label_T2 (", meta_to_T2_months, "m interval before T2 diagnosis)"),
    x = "MetastaseLoc_T1 (tumor 1)",
    y = "Entity_Label_T2 (tumor 2)",
    size = "Count",
    color = "Count"
  ) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

ggsave(file.path(out_dir, paste0("Bubble_MetastaseLoc_T1_EntityLabel_T2_", analysis_tag, ".png")),
       plot = p_bub, width = 12, height = 8, dpi = 300)

# ----------------------------------------------------------
# Bubble plots per Entity_Label_T1 (explicit, with thresholds + tables)
# ----------------------------------------------------------
bubble_min_n <- 10  # you can change (e.g., 20)
tbl_T1 <- sort(table(df_bubble_long$Entity_Label_T1), decreasing = TRUE)

cat("\n==== BUBBLE PER ENTITY: frequency table Entity_Label_T1 ====\n")
print(tbl_T1)
entities_T1 <- names(tbl_T1[tbl_T1 > bubble_min_n])

cat("\nEntities used for per-entity bubble plots (n >", bubble_min_n, "):\n")
print(entities_T1)

for (ent_name in entities_T1) {

  plot_data_ent <- df_bubble_long %>%
    filter(Entity_Label_T1 == ent_name) %>%
    group_by(MetastaseLoc_T1, Entity_Label_T2) %>%
    summarise(n = n(), .groups = "drop")

  cat("\n--- Entity_Label_T1:", ent_name, "---\n")
  cat("nrows(plot_data_ent):", nrow(plot_data_ent), "\n")
  cat("Head(plot_data_ent):\n"); print(head(plot_data_ent, 10))

  if (nrow(plot_data_ent) == 0) next

  p_ent <- ggplot(plot_data_ent, aes(x = MetastaseLoc_T1, y = Entity_Label_T2, size = n, color = n)) +
    geom_point(alpha = 0.7) +
    scale_size(range = c(3, 20)) +
    theme_minimal() +
    labs(
      title = paste("Bubble Plot for Entity_Label_T1:", ent_name),
      x = "MetastaseLoc_T1 (tumor 1)",
      y = "Entity_Label_T2 (tumor 2)",
      size = "Count",
      color = "Count"
    ) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))

  filename <- paste0("Bubble_MetastaseLoc_T1_EntityLabel_T2_for_",
                     gsub("[^A-Za-z0-9]", "_", ent_name),
                     "_", analysis_tag, ".png")

  ggsave(file.path(out_dir, filename),
         plot = p_ent, width = 12, height = 8, dpi = 300)
}

# ----------------------------------------------------------
# Time difference plots (cumulative) by meta_before_dx_T1_interval_yes + tables
# ----------------------------------------------------------
df_time <- dfp_valid %>%
  transmute(
    ID = patient_resource_id,
    timediff = timediff_raw,
    meta_before_dx_T1_interval_yes = meta_before_dx_T1_interval_yes,
    Entity_Label_T1 = Entity_Label_T1,
    Entity_Label_T2 = Entity_Label_T2
  ) %>%
  distinct()



cat("\n==== TIME TABLES ====\n")
cat("dim(df_time):\n"); print(dim(df_time))
cat("head(df_time):\n"); print(head(df_time, 10))
cat("summary(timediff):\n"); print(summary(df_time$timediff))
cat("quantiles(timediff):\n"); print(quantile(df_time$timediff, probs = c(0, 0.01, 0.05, 0.5, 0.95, 0.99, 1), na.rm = TRUE))
cat("table(meta_before_dx_T1_interval_yes):\n"); print(table(df_time$meta_before_dx_T1_interval_yes, useNA = "ifany"))

df_t_all <- df_time %>%
  arrange(timediff) %>%
  mutate(cumulative = row_number())

p_all <- ggplot(df_t_all, aes(x = timediff, y = cumulative)) +
  geom_step(size = 1.2) +
  geom_point() +
  theme_minimal() +
  labs(
    title = "Cumulative Curve of Time Difference (All Cases)",
    x = "Time difference (days)",
    y = "Cumulative number of cases"
  )

ggsave(file.path(out_dir, paste0("Cumulative_Timediff_All_", analysis_tag, ".png")),
       plot = p_all, width = 10, height = 6, dpi = 300)

df_t_yes <- df_time %>%
  filter(meta_before_dx_T1_interval_yes == "yes") %>%
  arrange(timediff) %>%
  mutate(cumulative = row_number())

df_t_no <- df_time %>%
  filter(meta_before_dx_T1_interval_yes == "no") %>%
  arrange(timediff) %>%
  mutate(cumulative = row_number())

cat("\nCounts YES/NO for cumulative curves:\n")
print(c(n_yes = nrow(df_t_yes), n_no = nrow(df_t_no)))

p_yes <- ggplot(df_t_yes, aes(x = timediff, y = cumulative)) +
  geom_step(size = 1.2) +
  geom_point() +
  theme_minimal() +
  labs(
    title = "Cumulative Curve (T1 meta >= X months before T2 = YES)",
    x = "Time difference (days)",
    y = "Cumulative number of cases"
  )

p_no <- ggplot(df_t_no, aes(x = timediff, y = cumulative)) +
  geom_step(size = 1.2) +
  geom_point() +
  theme_minimal() +
  labs(
    title = "Cumulative Curve (T1 meta >= X months before T2 = NO)",
    x = "Time difference (days)",
    y = "Cumulative number of cases"
  )

combined_plot <- grid.arrange(p_yes, p_no, ncol = 1)

ggsave(file.path(out_dir, paste0("Cumulative_Timediff_By_MetaBeforeDxT1Interval_", analysis_tag, ".png")),
       plot = combined_plot, width = 10, height = 12, dpi = 300)

# ----------------------------------------------------------
# Kaplan-Meier (overall + by entity T1) by meta_before_dx_T1_interval_yes + checks
# ----------------------------------------------------------
df_surv <- df_time %>%
  mutate(event = 1L)

cat("\n==== KM INPUT CHECKS ====\n")
cat("Any NA in timediff? ", any(is.na(df_surv$timediff)), "\n")
cat("Any negative timediff? ", any(df_surv$timediff < 0, na.rm = TRUE), "\n")
cat("table(meta_before_dx_T1_interval_yes):\n"); print(table(df_surv$meta_before_dx_T1_interval_yes, useNA = "ifany"))

km_fit <- survfit(Surv(timediff, event) ~ meta_before_dx_T1_interval_yes, data = df_surv)

png(file.path(out_dir, paste0("KM_MetaBeforeDxT1Interval_", analysis_tag, ".png")), width = 1200, height = 800, res = 150)
plot(km_fit,
     col = c("darkgreen", "darkred"),
     lwd = 2,
     xlab = "Time to second tumor (days)",
     ylab = "Survival probability",
     main = "Kaplan-Meier Curve by T1 meta >= X months before T2")
legend("topright",
       legend = c("yes", "no"),
       col = c("darkgreen", "darkred"),
       lwd = 2)
dev.off()

# KM per Entity_Label_T1 with threshold
km_min_n <- 50
tbl_km <- sort(table(df_surv$Entity_Label_T1), decreasing = TRUE)

cat("\n==== KM PER ENTITY: frequency table Entity_Label_T1 ====\n")
print(tbl_km)

entities_km <- names(tbl_km[tbl_km > km_min_n])
cat("\nEntities used for KM plots (n >", km_min_n, "):\n")
print(entities_km)

for (ent_name in entities_km) {

  df_ent <- df_surv %>% filter(Entity_Label_T1 == ent_name)

  groups <- unique(df_ent$meta_before_dx_T1_interval_yes)
  if (length(groups) < 2) {
    cat("Skipping", ent_name, "- only one group present:", paste(groups, collapse = ","), "\n")
    next
  }

  fit_ent <- survfit(Surv(timediff, event) ~ meta_before_dx_T1_interval_yes, data = df_ent)

  filename <- paste0("KM_curve_Entity_",
                     gsub("[^A-Za-z0-9]", "_", ent_name),
                     ".png")

  png(file.path(out_dir, filename), width = 1000, height = 800)
  plot(fit_ent,
       col = c("darkgreen", "darkred"),
       lwd = 2,
       xlab = "Time to Second Tumor (days)",
       ylab = "Survival Probability",
       main = paste("KM Curve for Entity_Label_T1:", ent_name))
  legend("topright",
         legend = c("yes", "no"),
         col = c("darkgreen", "darkred"),
         lwd = 2)
  dev.off()

  cat("Saved:", file.path(out_dir, filename), "\n")
}

cat("\n==== DONE ====\n")
cat("All outputs written to:", out_dir, "\n")
cat("Log file written to:", logfile, "\n")



cat("\n================ DATE & TIMEDIFF MANUAL CHECK =================\n")

# Sort by timediff (smallest first)
df_check_small <- dfp_valid %>%
  arrange(timediff_raw) %>%
  select(
    patient_resource_id,
    asserted_date_1,
    asserted_date_2,
    date1,
    date2,
    timediff_raw,
    metastasis_date_first_1,
    metastasis_date_first_2,
    meta_date1,
    meta_date2
  )

cat("\n--- Smallest timediff (first 30 rows) ---\n")
print(head(df_check_small[, -1], 30))

cat("\n--- Largest timediff (last 30 rows) ---\n")
print(tail(df_check_small[, -1], 30))

cat("\n--- Random 30 rows ---\n")
set.seed(123)
print(sample_n(df_check_small[, -1], 30))

cat("\n--- Summary of timediff ---\n")
print(summary(dfp_valid$timediff_raw))

cat("\n--- Quantiles of timediff ---\n")
print(quantile(dfp_valid$timediff_raw,
               probs = c(0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1),
               na.rm = TRUE))

cat("\n--- Timediff frequency for small values (0-365 days) ---\n")
print(table(cut(dfp_valid$timediff_raw,
                breaks = c(-1, 0, 30, 90, 180, 365, 730, 1825, 3650, Inf))))

cat("\n--- Unrealistic date checks ---\n")
print(c(
  year_date1_before_1950 = sum(year(dfp_valid$date1) < 1950, na.rm = TRUE),
  year_date2_before_1950 = sum(year(dfp_valid$date2) < 1950, na.rm = TRUE),
  year_date1_after_2030  = sum(year(dfp_valid$date1) > 2030, na.rm = TRUE),
  year_date2_after_2030  = sum(year(dfp_valid$date2) > 2030, na.rm = TRUE)
))


cat("\n================ METASTASIS DATE DIFF CHECKS =================\n")

# Ensure metastasis dates are parsed (you already do this; keep it here for clarity)
# meta_date1 = ymd(metastasis_date_first_1)
# meta_date2 = ymd(metastasis_date_first_2)

dfp_valid <- dfp_valid %>%
  mutate(
    # Differences between metastasis dates (only meaningful if both exist)
    meta_timediff_12 = ifelse(!is.na(meta_date1) & !is.na(meta_date2),
                              as.numeric(meta_date2 - meta_date1),
                              NA_real_),

    # Metastasis-to-diagnosis differences per tumor
    meta_to_dx_T1 = ifelse(!is.na(meta_date1) & !is.na(date1),
                           as.numeric(meta_date1 - date1),
                           NA_real_),

    meta_to_dx_T2 = ifelse(!is.na(meta_date2) & !is.na(date2),
                           as.numeric(meta_date2 - date2),
                           NA_real_),

    # Optional: metastasis of T1 relative to diagnosis of T2 (often useful for plausibility)
    meta1_to_dx2 = ifelse(!is.na(meta_date1) & !is.na(date2),
                          as.numeric(meta_date1 - date2),
                          NA_real_)
  )

# -----------------------------
# Global NA and sign checks
# -----------------------------
cat("\nNA counts:\n")
print(c(
  na_meta_date1 = sum(is.na(dfp_valid$meta_date1)),
  na_meta_date2 = sum(is.na(dfp_valid$meta_date2)),
  na_meta_timediff_12 = sum(is.na(dfp_valid$meta_timediff_12)),
  na_meta_to_dx_T1 = sum(is.na(dfp_valid$meta_to_dx_T1)),
  na_meta_to_dx_T2 = sum(is.na(dfp_valid$meta_to_dx_T2))
))

cat("\nmeta_to_dx_T1 sign counts (metastasis relative to T1 diagnosis):\n")
print(c(
  negative = sum(dfp_valid$meta_to_dx_T1 < 0, na.rm = TRUE),
  zero     = sum(dfp_valid$meta_to_dx_T1 == 0, na.rm = TRUE),
  positive = sum(dfp_valid$meta_to_dx_T1 > 0, na.rm = TRUE)
))

cat("\nmeta_to_dx_T2 sign counts (metastasis relative to T2 diagnosis):\n")
print(c(
  negative = sum(dfp_valid$meta_to_dx_T2 < 0, na.rm = TRUE),
  zero     = sum(dfp_valid$meta_to_dx_T2 == 0, na.rm = TRUE),
  positive = sum(dfp_valid$meta_to_dx_T2 > 0, na.rm = TRUE)
))


# These correspond to the flags used above:
# meta_1_before_T2_interval == 1  <=> meta_date1 <= (date2 %m-% months(meta_to_T2_months))
# meta_before_dx_T2 == 1          <=> meta_to_dx_T2 < 0
cat("\nConsistency check with flags:\n")
calc_T1_interval <- (!is.na(dfp_valid$meta_date1) & !is.na(dfp_valid$date2) & (dfp_valid$meta_date1 <= (dfp_valid$date2 %m-% months(meta_to_T2_months))))
print(c(
  flag_T1_interval_equals_calc = sum((dfp_valid$meta_1_before_T2_interval == 1L) != calc_T1_interval, na.rm = TRUE),
  flag_T2_equals_calc = sum((dfp_valid$meta_before_dx_T2 == 1L) != (dfp_valid$meta_to_dx_T2 < 0), na.rm = TRUE)
))

# -----------------------------
# Manual inspection tables
# -----------------------------
df_meta_check <- dfp_valid %>%
  select(
    patient_resource_id,
    asserted_date_1, asserted_date_2,
    metastasis_date_first_1, metastasis_date_first_2,
    date1, date2, meta_date1, meta_date2,
    timediff_raw,
    meta_to_dx_T1, meta_to_dx_T2,
    meta_timediff_12,
    meta_1_before_T2_interval, meta_before_dx_T2,
    metastasis_loc_1, metastasis_loc_2
  )

cat("\n--- Rows where meta_to_dx_T1 is negative (first 30) ---\n")
print(head(df_meta_check %>% filter(!is.na(meta_to_dx_T1) & meta_to_dx_T1 < 0) %>% arrange(meta_to_dx_T1), 30))

cat("\n--- Rows where meta_to_dx_T2 is negative (first 30) ---\n")
print(head(df_meta_check %>% filter(!is.na(meta_to_dx_T2) & meta_to_dx_T2 < 0) %>% arrange(meta_to_dx_T2), 30))

cat("\n--- Smallest meta_to_dx_T2 (first 30) ---\n")
print(head(df_meta_check %>% filter(!is.na(meta_to_dx_T2)) %>% arrange(meta_to_dx_T2), 30))

cat("\n--- Largest meta_to_dx_T2 (last 30) ---\n")
print(tail(df_meta_check %>% filter(!is.na(meta_to_dx_T2)) %>% arrange(meta_to_dx_T2), 30))

cat("\n--- Random 30 rows (metastasis + diagnosis + diffs) ---\n")
set.seed(123)
print(sample_n(df_meta_check, 30))

cat("\n--- Summaries: meta_to_dx_T1, meta_to_dx_T2, meta_timediff_12 ---\n")
print(summary(dfp_valid$meta_to_dx_T1))
print(summary(dfp_valid$meta_to_dx_T2))
print(summary(dfp_valid$meta_timediff_12))

cat("\n--- Quantiles: meta_to_dx_T2 ---\n")
print(quantile(dfp_valid$meta_to_dx_T2,
               probs = c(0, 0.01, 0.05, 0.5, 0.95, 0.99, 1),
               na.rm = TRUE))


sink()


#
# Liste der Ausgabedateien (Name + Inhalt)
#
# Alle Dateinamen enthalten jetzt automatisch den Tag:
#
#   analysis_tag = "T1meta_before_T2dx_<Xm>"
#
# Beispiel bei meta_to_T2_months <- 1:
#   analysis_tag = "T1meta_before_T2dx_1m"
#
# CSV-Dateien
#
# Entity_Label_ICD10_combinations_pivot_T1meta_before_T2dx_<Xm>.csv
# Enth�lt die ICD/Entity-Label-Kombinationen (Mapping-/Kontroll-Export).
# (Kein Patienten-Level, eher Debug/Referenz.)
#
# MetastaseLoc_T1_EntityLabel_T2_combinations_pivot_T1meta_before_T2dx_<Xm>.csv
# Enth�lt die (ID, Metastasenlokalisation T1, Entity_Label_T1, Entity_Label_T2) Kombinationen f�r die Bubble-Basis.
# Wichtig: Dieser Export ist jetzt bereits auf meta_before_dx_T1_interval_yes == "yes" gefiltert.
#
# Plots (PNG)Bar_MetaBeforeDxT1Interval_by_EntityLabel_T2_T1meta_before_T2dx_<Xm>.png
# Balkendiagramm: Verteilung nach Entity_Label_T2, gruppiert nach meta_before_dx_T1_interval_yes.
#
# StackedBar_MetaBeforeDxT1Interval_by_EntityLabel_T2_T1meta_before_T2dx_<Xm>.png
# Gestapeltes Balkendiagramm (Anteile) nach Entity_Label_T2, nach meta_before_dx_T1_interval_yes.
#
# Bubble_MetastaseLoc_T1_EntityLabel_T2_T1meta_before_T2dx_<Xm>.png
# Bubble-Plot (overall): Metastasenlokalisation T1 vs Entity_Label_T2, nur interval_yes == yes.
#
# Bubble_MetastaseLoc_T1_EntityLabel_T2_for_<ENTITY>_T1meta_before_T2dx_<Xm>.png
# Bubble-Plot pro Entity_Label_T1 (mehrere Dateien), nur interval_yes == yes.
#
# Cumulative_Timediff_All_T1meta_before_T2dx_<Xm>.png
# Kumulative Kurve f�r timediff (Diagnoseabstand T2-T1) insgesamt.
#
# Cumulative_Timediff_By_MetaBeforeDxT1Interval_T1meta_before_T2dx_<Xm>.png
# Kumulative Kurve timediff, stratifiziert nach meta_before_dx_T1_interval_yes.
#
# KM_MetaBeforeDxT1Interval_T1meta_before_T2dx_<Xm>.png
# Kaplan-Meier-Kurve, stratifiziert nach meta_before_dx_T1_interval_yes.
#
# Log-Datei
#
# run_log_checks_T1meta_before_T2dx_<Xm>.txt
# Konsolen-/Check-Ausgaben (Dimensions, Tabellen, Prechecks), ebenfalls mit Tag.



