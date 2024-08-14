#install.packages('DSI')
#install.packages('DSOpal', dependencies=TRUE)
#install.packages('dsBaseClient', repos=c(getOption('repos'), 'http://cran.datashield.org'), dependencies=TRUE)

library(dsBaseClient)
library(DSOpal)
library(DSI)
library(sys)


# load utils.R file
source("utils_entities_gender.R")

# Login
result <- ds_login()
logindata <- result$logindata
connections <- result$connections


#### investigate size of df, colnames of df
ds.dim("D")
ds.colnames("D")


#### check gender_mapped values where "other"
#### create subsets - 1 = female, 2 = male, 3 = other/diverse

#for(loc in 1:length(connections)){
#  tryCatch(
#    ds.dataFrameSubset("D", "D$gender_mapped", "3", "==", newobj = "D1", datasources = connections[loc]),
#    error = function(cond){warning("subset gendermapped= other/diverse could not be generated in ", names(connections[loc]), ". Not enough cases?")}
#  )
#}
#ds.dim("D1", datasources = connections[1])


#### optional: additional filter D date_diagnosis_year == 2022 here to assure only data from 2022
ds.dataFrameSubset(df.name = "D", V1.name = "D$date_diagnosis_year", V2.name = "2022", Boolean.operator = "==", newobj = "D", datasources=connections)
ds.dim("D")


#### STEP 1: generate subsets for relevant diagnoses
# generate_subsets: detailed description in utils_entities_gender.R
colname <- "icd10_grouped_entities"
start_subset <- 0
end_subset <- 23
connections <- connections

generate_subsets(colname, start_subset, end_subset, connections)

# useful: list all Dataframes on Server side - check which subsets you created
all_loc_dfs = get_all_loc_dfs()
all_loc_dfs

#### STEP 2: create a list of all relevant subset names
icd_group_list <- paste0("subset_", colname, "_", start_subset:end_subset)
icd_group_list

#### STEP 3:
# subset_prevalence_by_gender: detailed description in utils_entities_gender.R
subset_by_gender_result = subset_prevalence_by_gender(icd_group_list, all_loc_dfs, connections)

#### change row names
new_rownames_eng <- c("Lip, Oral Cavity, Pharynx (C00-C14)",
                    "Oesophagus (C15)",
                    "Stomach (C16)",
                    "Colon and Rectum (C18-C21)",
                    "Liver (C22)",
                    "Gallbladder and Biliary Tract (C23-C24)",
                    "Pancreas (C25)",
                    "Larynx (C32)",
                    "Trachea, Bronchus and Lungs (C33-C34)",
                    "Malignant Melanoma of Skin (C43)",
                    "Breast (C50, D05)",
                    "Cervis Uteri (C53, D06)",
                    "Corpus Uteri (C54-C55)",
                    "Ovary (C56, D39.1)",
                    "Prostate (C61)",
                    "Testis (C62)",
                    "Kidney (C64)",
                    "Bladder (C67, D09.0, D41.4)",
                    "Brain and Central Nervous System (C70-C72)",
                    "Thyroid (C73)",
                    "Hodgkin Lymphoma (C81)",
                    "Non-Hodgkin Lymphoma (C82-C88, C96)",
                    "Plasmacytoma (C90)",
                    "Leukaemia (C91-C95)"
)

rownames(subset_by_gender_result) <- new_rownames_eng
subset_by_gender_result


# write to csv file
write.csv(subset_by_gender_result, file = "subset_by_gender_result_matrix.csv", row.names = TRUE)

# clear DataSHIELD R-Session and logout
datashield.logout(connections)

