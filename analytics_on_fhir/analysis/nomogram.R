

# 1. Pakete laden
#install.packages("rms", type = "source")
#install.packages("survival")

library(rms)
library(survival)

# Nomogram - cohort vs conotrl 2 groups
data <- read.csv("prostate_data_for_nomogram.csv")

dd <- datadist(data)
options(datadist = "dd")

f <- cph(Surv(survival_time_months, event) ~ age_at_diagnosis +
           asserted_year +
           cohort_flag_1 +
           m_tnm_clean_M1a +
           m_tnm_clean_M1b +
           m_tnm_clean_M1c +
           m_tnm_clean_no_documented_m +
           gleason_score_7.0 +
           gleason_score_8.0 +
           gleason_score_9.0 +
           gleason_score_10.0,
         data = data,
         x = TRUE, y = TRUE, surv = TRUE)

surv <- Survival(f)
nom <- nomogram(f,
                fun = list(function(x) surv(36, x),
                           function(x) surv(60, x)),
                funlabel = c("3-Year Survival",
                             "5-Year Survival"))

plot(nom,
     xfrac = 0.45,
     cex.axis = 0.85,
     cex.var = 0.9)




# NOMOGRAM: 4-GROUPS MODEL

library(rms)
library(survival)

data <- read.csv("prostate_data_for_nomogram_4groups.csv")

# 3. WICHTIG: Datenverteilung für das rms-Paket berechnen
dd <- datadist(data)
options(datadist = "dd")

f <- cph(Surv(survival_time_months, event) ~ asserted_year +
           cohort_control_groups_label_young_low_grade_control +
           cohort_control_groups_label_old_high_grade_control +
           cohort_control_groups_label_old_low_grade_control +
           m_tnm_clean_M1a +
           m_tnm_clean_M1b +
           m_tnm_clean_M1c +
           m_tnm_clean_no_documented_m,
         data = data,
         x = TRUE, y = TRUE, surv = TRUE)

surv <- Survival(f)
nom <- nomogram(f,
                fun = list(function(x) surv(36, x),
                           function(x) surv(60, x)),
                funlabel = c("3-Year Survival",
                             "5-Year Survival"))

plot(nom,
     xfrac = 0.45,
     cex.axis = 0.85,
     cex.var = 0.9)




# NOMOGRAM: 4-GROUPS MODEL
library(rms)
library(survival)


data <- read.csv("prostate_data_for_nomogram_4groups.csv")

dd <- datadist(data)
options(datadist = "dd")

f <- cph(Surv(survival_time_months, event) ~ asserted_year +
           cohort_control_groups_label_young_low_grade_control +
           cohort_control_groups_label_old_high_grade_control +
           cohort_control_groups_label_old_low_grade_control +
           m_tnm_clean_M1a +
           m_tnm_clean_M1b +
           m_tnm_clean_M1c +
           m_tnm_clean_no_documented_m,
         data = data,
         x = TRUE, y = TRUE, surv = TRUE)

surv <- Survival(f)
nom <- nomogram(f,
                fun = list(function(x) surv(36, x),
                           function(x) surv(60, x)),
                funlabel = c("3-Year Survival",
                             "5-Year Survival"))

plot(nom,
     xfrac = 0.45,
     cex.axis = 0.85,
     cex.var = 0.9)



# NOMOGRAM: INTERACTION AGE * GLEASON
library(rms)
library(survival)

data <- read.csv("prostate_data_for_nomogram_interaction_age_gleason.csv")

data$gleason_score <- factor(data$gleason_score)
data$m_tnm_clean   <- factor(data$m_tnm_clean)
data$cohort_flag   <- factor(data$cohort_flag)

dd <- datadist(data)
options(datadist = "dd")

f <- cph(Surv(survival_time_months, event) ~ age_at_diagnosis * gleason_score +
           cohort_flag +
           m_tnm_clean +
           asserted_year,
         data = data,
         x = TRUE, y = TRUE, surv = TRUE)

surv <- Survival(f)
nom <- nomogram(f,
                fun = list(function(x) surv(36, x),
                           function(x) surv(60, x)),
                funlabel = c("3-Year Survival",
                             "5-Year Survival"))

plot(nom,
     xfrac = 0.45,
     cex.axis = 0.85,
     cex.var = 0.9)
