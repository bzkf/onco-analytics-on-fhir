# utils for entities_gender.R script

# Login
ds_login <- function() {
  builder <- DSI::newDSLoginBuilder()

  builder$append(
    server = "Erlangen",
    url = "<enter-url-local-opal-server>",
    user = Sys.getenv("user_uker"),
    password = Sys.getenv("password_uker"),
    user = "",
    password = "",
    table = "<projectname.tablename>",
    driver = "OpalDriver"
  )

  builder$append(
    server = "Augsburg",
    url = "<enter-url-local-opal-server>",
    user = "",
    password = "",
    table = "<projectname.tablename>",
    driver = "OpalDriver"
  )

  builder$append(
    server = "Würzburg",
    url = "<enter-url-local-opal-server>",
    user = "",
    password = "",
    table = "<projectname.tablename>",
    driver = "OpalDriver"
  )

  builder$append(
    server = "Regensburg",
    url = "<enter-url-local-opal-server>",
    user = "",
    password = "",
    table = "<projectname.tablename>",
    driver = "OpalDriver"
  )

  builder$append(
    server = "München TUM",
    url = "<enter-url-local-opal-server>",
    user = "",
    password = "",
    table = "<projectname.tablename>",
    driver = "OpalDriver"
  )

  builder$append(
    server = "München LMU",
    url = "<enter-url-local-opal-server>",
    user = "",
    password = "",
    table = "<projectname.tablename>",
    driver = "OpalDriver"
  )

  logindata <- builder$build()
  connections <-
    DSI::datashield.login(logins = logindata,
                          assign = TRUE,
                          symbol = "D")

  return(list(logindata = logindata, connections = connections))
}


#' Create a list of all server-side dataframes with the corresponding
#' location names for all participating sites
#'
#' This function creates a list containing all dataframes present on the
#' servers, along with the names of the respective locations. The function
#' is used to iterate over these dataframes later.
#'
#' @return
#' The function returns a list where each element contains the dataframes
#' available on a server. The names of the list correspond to the names of
#' the locations.
get_all_loc_dfs <- function() {
  all_loc_dfs <- list()
  all_dfs <- ds.ls()
  for (i in 1:length(connections)) {
    all_loc_dfs <- c(all_loc_dfs, list(all_dfs[[i]][[2]]))
  }
  names(all_loc_dfs) <- names(connections)
  return(all_loc_dfs)
}


#' Create subsets of data based on a specified column
#'
#' This function creates subsets of the data based on the specified column
#' and the specified range of subset groups. It iterates over the specified
#' range of subset groups and attempts to create a subset for each group
#' within each location from the connections (from start_subset to
#' end_subset). If a subset cannot be created (e.g., due to insufficient
#' cases), a warning is issued.
#'
#' @param colname The column name used to create the subsets.
#' @param start_subset The starting group of the subset range.
#' @param end_subset The ending group of the subset range.
#' @param connections A list of DataSHIELD connections to the data sources
#' of the participating locations.
#'
#' @details
#' The function uses `ds.dataFrameSubset` to create subsets for the groups
#' between start_subset and end_subset. The subsets are dynamically named
#' based on the column name and the subset group.
#'
#' @return
#' The function does not return a value. Instead, new subsets are created
#' on the server side within each specified connection, with names in the
#' format `subset_<colname>_<subset_group>`.
#'
#' @examples
#' generate_subsets("icd10_grouped", 1, 3, connections)
generate_subsets <-
  function(colname,
           start_subset,
           end_subset,
           connections) {
    for (subset_group in start_subset:end_subset) {
      for (loc in 1:length(connections)) {
        tryCatch({
          ds.dataFrameSubset(
            "D",
            paste0("D$", colname),
            as.character(subset_group),
            "==",
            newobj = paste0("subset_", colname, "_", subset_group),
            datasources = connections[loc]
          )
        },
        error = function(cond) {
          warning(
            paste0(
              "Subset ",
              paste0("subset_", colname, "_", subset_group),
              " could not be generated in ",
              names(connections[loc]),
              ". Not enough cases?"
            )
          )
        })
      }
    }
  }


#' Generate a results table for subset frequencies (e.g., diagnoses) by gender
#'
#' This function creates a results table that contains the subset frequencies
#' by gender (male, female, and other) for each specified subset and each
#' location. The function iterates over a list of subsets and checks for each
#' subset and each location whether the subset is available. If available,
#' the total count and the counts split by gender are calculated and stored
#' in a vector. This vector is then converted into a matrix, which is returned
#' as a tabular summary of the results.
#'
#' @param subset_list A list of subsets to be analyzed.
#' @param all_loc_dfs A list of data sources containing the available dataframes
#'     for each connection, e.g., obtained via get_all_loc_dfs().
#' @param connections A list of DataSHIELD connections to the data sources of
#'     the participating locations.
#'
#' @details The function uses the `ds.dataFrameSubset` function to create subsets
#'     based on gender (`gender_mapped`). For each subset and each connection,
#'     the total count as well as the counts for the genders "male" (2), "female"
#'     (1), and "other" (3) are calculated. The results are stored in a vector and
#'     then converted into a matrix.
#'
#' @return The function returns a matrix that contains the total count and the
#'     counts split by gender for each subset and each connection.
#'
#' @examples
#' generate_gender_subset_output(c("subset_1", "subset_2"), all_loc_dfs,
#'     connections)
subset_prevalence_by_gender <-
  function(subset_list, all_loc_dfs, connections) {
    # Generate the output as a vector
    output_vector <- NULL

    for (subs in subset_list) {
      for (loc in 1:length(connections)) {
        # Check if the subset is available
        if (subs %in% all_loc_dfs[[loc]]) {
          male <- NA
          female <- NA
          other <- NA
          print(paste0(subs, " at site ", names(connections[loc])))

          # total count
          total <-
            ds.dim(subs, datasources = connections[loc])[[1]][1]

          # male
          male <- tryCatch({
            ds.dataFrameSubset(
              subs,
              paste0(subs, "$gender_mapped"),
              "2",
              "==",
              newobj = paste0(subs, "_male"),
              datasources = connections[loc]
            )
            ds.dim(paste0(subs, "_male"), datasources = connections[loc])[[1]][1]
          },
          error = function(cond) {
            warning(paste0(
              subs,
              "_male could not be generated in ",
              names(connections[loc])
            ))
            return(NA)
          })

          # female
          female <- tryCatch({
            ds.dataFrameSubset(
              subs,
              paste0(subs, "$gender_mapped"),
              "1",
              "==",
              newobj = paste0(subs, "_female"),
              datasources = connections[loc]
            )
            ds.dim(paste0(subs, "_female"), datasources = connections[loc])[[1]][1]
          },
          error = function(cond) {
            warning(paste0(
              subs,
              "_female could not be generated in ",
              names(connections[loc])
            ))
            return(NA)
          })

          # other
          other <- tryCatch({
            ds.dataFrameSubset(
              subs,
              paste0(subs, "$gender_mapped"),
              "3",
              "==",
              newobj = paste0(subs, "_other"),
              datasources = connections[loc]
            )
            ds.dim(paste0(subs, "_other"), datasources = connections[loc])[[1]][1]
          },
          error = function(cond) {
            warning(paste0(
              subs,
              "_other could not be generated in ",
              names(connections[loc])
            ))
            return(NA)
          })

          output_vector <-
            c(output_vector, total, male, female, other)
        } else {
          output_vector <- c(output_vector, NA, NA, NA, NA)
        }
      }
    }

    # format output
    output <-
      matrix(output_vector,
             ncol = length(connections) * 4,
             byrow = TRUE)
    colnames(output) <-
      paste0(rep(names(connections), each = 4),
             c("_total", "_male", "_female", "_other"))
    rownames(output) <- subset_list

    return(output)
  }
