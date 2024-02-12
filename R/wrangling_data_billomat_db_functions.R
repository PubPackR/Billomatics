


## dictionaries-----
## select the relevant key to create columns
## dictionaries
## select the relevant key to create columns
#' @export
invoice_items_columns <- c(
  "id",
  "article_id",
  "invoice_id",
  "position",
  "title",
  "quantity",
  "description",
  "total_net",
  "total_gross",
  "total_net_unreduced",
  "reduction",
  "template_id"
)

#' @export
articles_columns <- c("id",
                      "article_number",
                      "title",
                      "description")

#' @export
offers_columns <- c(
  "id",
  "created",
  "updated",
  "client_id",
  "offer_number",
  "title",
  "date",
  "validity_date",
  "status",
  "label",
  "intro",
  "note",
  "total_net",
  "total_gross",
  "reduction",
  "total_reduction",
  "total_net_unreduced",
  "total_gross_unreduced",
  "template_id"
)


#' @export
offer_items_columns <- c(
  "id",
  "article_id",
  "offer_id",
  "position",
  "quantity",
  "title",
  "description",
  "total_net",
  "total_net_unreduced"
)
#' @export
templates_columns <- c("id",
                       "name",
                       "type")
#' @export
confirmations_columns <- c(
  "id",
  "created",
  "updated",
  "client_id",
  "confirmation_number",
  "number",
  "date",
  "status",
  "label",
  "intro",
  "note",
  "total_net",
  "total_gross",
  "reduction",
  "total_reduction",
  "total_net_unreduced",
  "total_gross_unreduced",
  "offer_id",
  "template_id"
)

#' @export
confirmation_items_columns <- c(
  "id",
  "article_id",
  "confirmation_id",
  "position",
  "quantity",
  "title",
  "description",
  "total_net",
  "reduction",
  "total_net_unreduced",
  "template_id"
)



#' @export
clients_columns <- c("id",
                     "created",
                     "updated",
                     "client_number",
                     "number",
                     "name",
                     "Kunde_kurz_Datev",
                     "country_code",
                     "Debitorennummer (neu)",
                     "Rechnungsform",
                     "Rechnungsemailadresse",
                     "Rechnungsadresse",
                     "Zusätzliche Rechnungswünsche",
                     "vat_number",
                     "due_days",
                     "facturx_identifier",
                     "salutation",
                     "first_name",
                     "last_name",
                     "email",
                     "street",
                     "city",
                     "zip",
                     "archived",
                     "address",
                     "revenue_net")

#' @export
invoices_columns <- c(
  "id",
  "invoice_number",
  "created",
  "updated",
  "client_id",
  "status",
  "address",
  "title",
  "intro",
  "note",
  "total_net",
  "total_gross",
  "reduction",
  "total_reduction",
  "total_net_unreduced",
  "total_gross_unreduced",
  "open_amount",
  "paid_amount",
  "date",
  "due_date",
  "discount_rate",
  "label",
  "invoice_id",
  "confirmation_id",
  "offer_id",
  "recurring_id",
  "template_id"
)



##functions----
#' get_tables_billomat
#'
#' This function loads the table specified from the local Billomat DB.

#' @param billomatDB the path to the billomat db
#' @param db_table_name the name of the table you are interested in
#' @return the data from the DB as dataframe

#' @export
get_tables_billomat <-
  function(db_table_name = "confirmations",
           billomatDB = billomatDB,
           encryption_key_db = encryption_key_db) {
    # here I read the whole table with db_name from the billomat db
   shinymanager::read_db_decrypt(billomatDB, db_table_name,encryption_key_db)
  }

#' get_db_2_wide_df
#'
#' this function turns the db entry that is in a long format into a readable rowwise entry
#'
#' @param db_table_name the name of the table you are interested in
#' @param export_csv set to true if you want to export the wide df as a csv, in this case,
#' you have to provide the path to the project
#' @param path_for_export the path where you want to store the exported file - default is the working directory
#' @return the data from the long data frame turned into a wide data frame with all columns of interest

#' @export
get_db_2_wide_df <- function (db_table_name,
                              billomatDB = billomatDB,
                              export_csv = FALSE,
                              encryption_key_db = encryption_key_db,
                              path_for_export = getwd()) {
  ## paste the db name and the keyword columns in order to select the correct dictionary for the Key selection
  column_names <- paste0(db_table_name, "_columns")

  # read the db that is going to be turned into the df
  df <-
    get_tables_billomat (db_table_name = db_table_name, billomatDB = billomatDB,encryption_key_db =encryption_key_db) %>%
    # keep only the rows which have a relevant key
    dplyr::filter (name %in% get(column_names)) %>%
    tidyr::pivot_wider(
      id_cols = c("ids", "page"),
      names_from = "name",
      values_from = "value"
    )
  # here I save the resulting df as csv
  if (export_csv) {
    readr::write_csv2(df,
                      paste0(path_for_export, "/export/", db_table_name, ".csv"),
    )
  }
  return(df)
}

#' write_db2csv
#'
#' this function exports the data to a csv
#'
#' @param table2process the name of the tables you are interested in, this can be one or a vector
#' @param path_for_export the path where you want to store the exported file - default is the working directory
#' @return the data will be stored in the local folder

#' @export
write_db2csv <- function(table2process = "invoices",
                         billomatDB = billomatDB,
                         encryption_key_db=encryption_key_db,
                         path_for_export = getwd()) {
  tables <-
    c(
      "invoices",
      "invoice_items",
      "confirmations",
      "confirmation_items",
      "clients",
      "offers",
      "offer_items",
      "articles",
      "offer_tags",
      "templates"
    )
  # i create the range for the for loop based on the entered tables of interest
  #table2process <- c("invoices","templates")
  #which(tables %in% table2process)

  for (table in tables[which(tables %in% table2process)]) {
    #print(paste0("test",table))
    get_db_2_wide_df(
      table,
      billomatDB = billomatDB,
      encryption_key_db = encryption_key_db,
      path_for_export = path_for_export,
      export_csv = TRUE
    )
  }
}


#' read_most_recent_data
#'
#' this function reads the latest RDS, csv or xlsx file in a certain directory
#'
#' @param location the directors you want to read the most recent file from
#' @param filetype the file type you want to read (xlsx,RDS,csv)
#' @param name_starts_with filter files by beginning of name
#' @return the most recent data table

#' @export
read_most_recent_data <- function(location, filetype = "RDS", name_starts_with = "") {

  # filter on files that starts with the parameter name_starts_with and
  # ends with the filetype
  file_pattern <- paste0("^", name_starts_with, ".*.", filetype, "$")

  #read all the rds, csv and xlsx files
  filename <-

    # list.files(data_folder, full.names = TRUE, pattern = "*.xlsx") %>%
    list.files(location, full.names = TRUE, pattern = paste0("*.",filetype)) %>%

    #keep the information about the file
    file.info() %>%
    dplyr::mutate(name = rownames(.)) %>%

    dplyr::filter(grepl(pattern = file_pattern, basename(name))) %>%

    #only keep the most recent filename
    dplyr::slice_max(mtime) %>%
    dplyr::pull(name)


  filetype <- tail(unlist(strsplit(filename, "\\.")), 1)
  print(paste0("The filetype: ", filetype))
  print(paste0("Last modified file: ", filename))

  if(length(filename)==0){
    print(paste0("No file found that matches your pattern: ", file_pattern))
    print("These are all files in your specified location:")
    print(list.files(path = location), full.names = TRUE)
  }

  if (filetype == "xlsx") {
    df <-
      openxlsx::read.xlsx(filename)
  }

  if (filetype == "csv") {
    df <-
      readr::read_csv2(filename)
  }

  if (filetype == "RDS") {
    df <-
      readRDS(filename)
  }

  return(df)
}
