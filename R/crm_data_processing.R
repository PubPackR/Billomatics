################################################################################-
# ----- Description -------------------------------------------------------------
#
# Data processing utilities for CRM data.
# These functions help with unnesting and transforming CRM data structures.
#
# ------------------------------------------------------------------ #
# Authors@R: Moritz Hemmann
# Date: 2024/08
#
################################################################################-
# ----- Start -------------------------------------------------------------------

#' unnesting_lists
#' this function is used to unnest the lists that are part of retrieved table

#' @param df the df which contains the list wise variable
#' @param listname the lists which should be transformed -- this has to be a list with this name
#' @return the tibble which contains all information, this can be stored in a single vector or lists

#' @export
unnesting_lists <- function(df,listname = listname){
  df %>%
    tidyr::unnest(listname,names_sep = "_") %>%
    dplyr::select(contains(c(listname,"id"))) %>%
    # this selects only the variables that are of interest, possibly we need to keep more variables?
    dplyr::select(c(contains(c("name","department","type","precise_time","badge")),"id"))
}



#'create_single_table
#' this function creates the a single crm table with all information as a tibble from one list
#' the table is long and contains a name and value for each id
#' to ensure that names can be distinct, I create an entry_number based on the grouped id and nam.
#' If an id has the same field multiple times, then the entry_number will be >1.
#' @param df the df which contains the list wise variable
#' @param listname the lists which should be transformed -- this has to be a list with this name
#' @return the tibble which contains all information, this is stored in a tibble

#' @export
create_single_table <- function(df,listname = "companies",i = 1){
  # this function gets the tables that were lists into separate tables
  # unnest the list
  this_list_table <- unnesting_lists(df,all_lists[i]) %>%
    # make the list into one long data frame, only the id is kept as another column
    tidyr::pivot_longer(.,cols = -id) %>%
    # drop all na values to avoid empty cells
    tidyr::drop_na(value) %>%
    dplyr::group_by(id,name) %>%
    dplyr::mutate(entry_number = dplyr::row_number())

  # adding the new data to the final data table
  return(this_list_table)
}



#'create_full_table
#'this function joins all retrieved lists together in one long table
#'all_lists <- c("emails","tags","positions","companies","tels","tasks","tasks_pending")
#' @param df the df which contains the list wise variable
#' @param all_lists the lists which should be transformed -- this has to be a list with this name
#' @return the tibble which contains all information, this is stored in a tibble
#' to create the dataset, run the function and assign the result to a named object:
#' result <- create_full_table(crm_data,all_lists)
#' The id of the person is the primary key to link the resulting tables together
#' @export
create_full_table <- function(df, all_lists) {
  # the have a table that will be filled, the final table is initiated here
  final_table <- tidyr::tibble(
    name = as.character(),
    value = as.character(),
    id = as.integer()
  )
  i <- 1
  for (i in 1:length(all_lists)) {
    print(all_lists[i])
    # here i create a single table with the entries of the respective list found in i
    this_list_table <- create_single_table(df, listname = all_lists[i], i)
    # here I add the newly created list to the existing final table
    final_table <- rbind(final_table, this_list_table)
  }
  return(final_table)
}
