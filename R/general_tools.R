# -------------------------- Start script --------------------------------

#' generate_password
#'
#' This function creates a random password of a specified length using lowercase
#' letters, uppercase letters, numbers, and special characters, ensuring at
#' least one character from each set.

#' @param length the length of the password you want to generate
#' @return a password including lower and upper case letters, numbers and special characters

#' @export
generate_password <- function(length = 10) {

  # Define the character sets
  lowercase <- sample(setdiff(letters, c('i', 'j', 'o')), length, replace = TRUE)
  uppercase <- sample(setdiff(LETTERS, c('I', 'J', 'O')), length, replace = TRUE)
  numbers <- sample(1:9, length, replace = TRUE)
  special_characters <- sample(c('!', '@', '#', '$', '%', '&', '*', '(', ')', '-', '_', '=', '+', '|', '<', '>', '?', '/'), length, replace = TRUE)

  # Combine the character sets
  all_characters <- c(lowercase, uppercase, numbers, special_characters)

  # Shuffle and select the required length
  password <- sample(all_characters, length, replace = FALSE)

  # Ensure the password contains at least one character from each set
  password[1] <- sample(setdiff(letters, c('i', 'j', 'o')), 1)
  password[2] <- sample(setdiff(LETTERS, c('I', 'J', 'O')), 1)
  password[3] <- sample(1:9, 1)
  password[4] <- sample(c('!', '@', '#', '$', '%', '&', '*', '(', ')', '-', '_', '=', '+', '|', '<', '>', '?', '/'), 1)

  # Shuffle the password to randomize it again
  password <- sample(password, length)

  # Return the password as a single string
  return(paste(password, collapse = ""))
}

#' save_downloadable_excel
#'
#' This function generates a random password, creates a downloadable Excel file
#' with the provided filename, and saves it in the specified directory. It also
#' manages password encryption and updates the password log.

#' @param dashboard_nr the dashboard number to be included in the file path
#' @param file_name the name of the Excel file to be generated
#' @return no return values

#' @export
save_downloadable_excel <- function(data, billomat_key, file_name, dashboard_nr_string = "02", shiny_download_files = "../../base-data/shiny_download_files") {

  # Generate a random password for Excel file encryption
  pwd_excel <- generate_password(12)

  # Create the Excel file with KPI data and password protection
  data %>%
    xlsx::write.xlsx(paste0(shiny_download_files, "/shiny_", dashboard_nr_string, "/", file_name), password = pwd_excel)

  # Load and update the password log, encrypting it for security
  readRDS(paste0(shiny_download_files, "/encryption_download_files.RDS")) %>%
    safer::decrypt_object(billomat_key) %>%
    bind_rows(data.frame(file = paste0("shiny_", dashboard_nr_string, "/", file_name), password = pwd_excel, ts = Sys.time())) %>%
    arrange(ts) %>%
    group_by(file) %>%
    summarise_all(last) %>%
    safer::encrypt_object(billomat_key) %>%
    saveRDS(paste0(shiny_download_files, "/encryption_download_files.RDS"))
}

#' Replace Internal IDs with External IDs
#'
#' This function takes a table containing internal IDs and replaces them (or
#' adds a new column with) corresponding external IDs by joining with a
#' separate ID mapping table.
#'
#' @param internal_table A data frame that contains the internal IDs you want to replace.
#' @param internal_id_column A string. The name of the column in `internal_table`
#'   that holds the internal IDs. This column will be used as the join key.
#' @param id_mapping_table A data frame that serves as the mapping between
#'   internal and external IDs. It must contain a column named `id` (for internal IDs)
#'   and a column specified by `external_id_column`.
#' @param external_id_column A string. The name of the column in `id_mapping_table`
#'   that holds the external IDs.
#' @param new_column_name A string. The desired name for the column containing
#'   the external IDs in the output table. Defaults to `internal_id_column`,
#'   which means the original internal ID column will be overwritten.
#'
#' @return A data frame similar to `internal_table`, but with the specified
#'   internal ID column replaced by or augmented with the corresponding external IDs.
#'   If an internal ID does not have a match in the mapping table, the external ID
#'   will be `NA`.
#'
#' @importFrom dplyr select mutate left_join rename sym
#' @importFrom stats setNames
#' @importFrom rlang `:=`
#' @export
#'
#' @examples
#' \dontrun{
#'   internal_data <- data.frame(
#'     user_id = c(1, 2, 3, 4),
#'     value = c("A", "B", "C", "D")
#'   )
#'
#'   id_map <- data.frame(
#'     id = c(1, 2, 3, 5),
#'     external_user_id = c("ext_A", "ext_B", "ext_C", "ext_E")
#'   )
#'
#'   # Overwrite the 'user_id' column with external IDs
#'   result_overwrite <- replace_internal_ids_with_external(
#'     internal_table = internal_data,
#'     internal_id_column = "user_id",
#'     id_mapping_table = id_map,
#'     external_id_column = "external_user_id"
#'   )
#'   print(result_overwrite)
#'
#'   # Create a new column named 'new_external_id'
#'   result_new_column <- replace_internal_ids_with_external(
#'     internal_table = internal_data,
#'     internal_id_column = "user_id",
#'     id_mapping_table = id_map,
#'     external_id_column = "external_user_id",
#'     new_column_name = "new_external_id"
#'   )
#'   print(result_new_column)
#' }
replace_internal_ids_with_external <- function(internal_table, internal_id_column, id_mapping_table, external_id_column, new_column_name = internal_id_column) {

  id_mapping_prepared <- id_mapping_table %>%
    dplyr::select(id, !!dplyr::sym(external_id_column))

  join_by_args <- stats::setNames("id", internal_id_column)

  joined_table <- internal_table %>%
    dplyr::left_join(id_mapping_prepared, by = join_by_args)

  # Handles potential `.y` suffix if external_id_column is also present in internal_table
  # and if it was the same as internal_id_column after the join.
  # This part of the logic implies a specific column naming conflict resolution after left_join.
  if(internal_id_column == external_id_column) {
    external_id_column <- paste0(external_id_column, ".y")
  }

  if (new_column_name == internal_id_column) {
    final_table <- joined_table %>%
      dplyr::mutate(
        !!dplyr::sym(internal_id_column) := !!dplyr::sym(external_id_column)
      ) %>%
      dplyr::select(-!!dplyr::sym(external_id_column))
  } else {
    final_table <- joined_table %>%
      dplyr::mutate(!!dplyr::sym(new_column_name) := !!dplyr::sym(external_id_column)) %>%
      dplyr::select(-!!dplyr::sym(internal_id_column))

    # This conditional removal ensures that the original external_id_column (which came from the join)
    # is removed if the new_column_name is different from it, preventing redundant columns.
    if (new_column_name != external_id_column) {
      final_table <- final_table %>%
        dplyr::select(-!!dplyr::sym(external_id_column))
    }
  }

  return(final_table)
}

#' Replace External IDs with Internal IDs
#'
#' This function takes a table containing external IDs and replaces them (or
#' adds a new column with) corresponding internal IDs by joining with a
#' separate ID mapping table.
#'
#' @param external_table A data frame that contains the external IDs you want to replace.
#' @param external_id_column A string. The name of the column in `external_table`
#'   that holds the external IDs. This column will be used as the join key.
#' @param id_mapping_table A data frame that serves as the mapping between
#'   internal and external IDs. It must contain a column named `id` (for internal IDs)
#'   and a column specified by `external_id_column_mapping`.
#' @param external_id_column_mapping A string. The name of the column in `id_mapping_table`
#'   that holds the external IDs. This column will be used as the join key from the mapping table.
#' @param new_column_name A string. The desired name for the column containing
#'   the internal IDs in the output table. Defaults to `external_id_column`,
#'   which means the original external ID column will be overwritten.
#'
#' @return A data frame similar to `external_table`, but with the specified
#'   external ID column replaced by or augmented with the corresponding internal IDs.
#'   If an external ID does not have a match in the mapping table, the internal ID
#'   will be `NA`.
#'
#' @importFrom dplyr select mutate left_join sym
#' @importFrom stats setNames
#' @importFrom rlang `:=`
#' @export
#'
#' @examples
#' \dontrun{
#'   external_data_with_external_ids <- data.frame(
#'     item_id = c("ext_A", "ext_B", "ext_Z", "ext_D"),
#'     quantity = c(10, 20, 15, 30)
#'   )
#'
#'   id_map <- data.frame(
#'     id = c(1, 2, 3, 5),
#'     external_user_id = c("ext_A", "ext_B", "ext_C", "ext_E")
#'   )
#'
#'   # Overwrite the 'item_id' column with internal IDs
#'   result_overwrite_internal <- replace_external_ids_with_internal(
#'     external_table = external_data_with_external_ids,
#'     external_id_column = "item_id",
#'     id_mapping_table = id_map,
#'     external_id_column_mapping = "external_user_id"
#'   )
#'   print(result_overwrite_internal)
#'
#'   # Create a new column named 'internal_item_id'
#'   result_new_column_internal <- replace_external_ids_with_internal(
#'     external_table = external_data_with_external_ids,
#'     external_id_column = "item_id",
#'     id_mapping_table = id_map,
#'     external_id_column_mapping = "external_user_id",
#'     new_column_name = "internal_item_id"
#'   )
#'   print(result_new_column_internal)
#' }
replace_external_ids_with_internal <- function(external_table, external_id_column, id_mapping_table, external_id_column_mapping, new_column_name = external_id_column) {

  id_mapping_prepared <- id_mapping_table %>%
    dplyr::select(id, !!dplyr::sym(external_id_column_mapping))

  join_by_args <- stats::setNames(external_id_column_mapping, external_id_column) # Join by column with same name in both

  joined_table <- external_table %>%
    dplyr::left_join(id_mapping_prepared, by = join_by_args)

  if (new_column_name == external_id_column) {
    final_table <- joined_table %>%
      dplyr::mutate(
        !!dplyr::sym(external_id_column) := id
      ) %>%
      dplyr::select(-id)
  } else {
    final_table <- joined_table %>%
      dplyr::mutate(!!dplyr::sym(new_column_name) := id) %>%
      dplyr::select(-!!dplyr::sym(external_id_column), -id)
  }

  return(final_table)
}
