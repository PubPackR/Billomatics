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
save_downloadable_excel <- function(data, dashboard_nr, file_name) {

  # Generate a random password for Excel file encryption
  pwd_excel <- generate_password(12)

  # Create the Excel file with KPI data and password protection
  data %>%
    write.xlsx(paste0(shiny_download_files, "/shiny_", dashboard_nr, "/", file_name), password = pwd_excel)

  # Load and update the password log, encrypting it for security
  passwort_df <- readRDS(paste0(shiny_download_files, "/encryption_download_files.RDS")) %>%
    safer::decrypt_object(keys$billomat[1]) %>%
    bind_rows(data.frame(file = paste0("shiny_", dashboard_nr, "/", file_name), password = pwd_excel, ts = Sys.time())) %>%
    arrange(ts) %>%
    group_by(file) %>%
    summarise_all(last) %>%
    safer::encrypt_object(keys$billomat[1]) %>%
    saveRDS(paste0(shiny_download_files, "/encryption_download_files.RDS"))
}
