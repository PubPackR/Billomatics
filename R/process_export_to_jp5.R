
#' aggregation for excel files for export to jp5
#'
#' The function compiles excel files in a given directory and returns a large excelfile
#' @param tmp_folder the location where the temporary files are stored
#' @param export_folder the location where the final combined file is stored
#'
#' @return this fcuntion returns nothing, but has side effects.
#'
#' @export
aggregate_export_jp5 <- function(tmp_folder = "../../base-data/PMI/export/tmp/",
                                 output_folder = "../../base-data/PMI/export/") {


  # iterate over all names and cbind them
  files <- list.files(tmp_folder, full.names = TRUE) %>%
    file.info() %>%
    dplyr::mutate(name = rownames(.),
           filename= stringr::str_extract_all(name,"(?<=/tmp/).*(?=_2)")) %>%
    dplyr::distinct(filename,.keep_all = TRUE)

  # I want to get all unique names and then get the start
  aggregate_files <- function(tmp_folder,name_starts_with = "") {
    Billomatics::read_most_recent_data(location = tmp_folder,filetype = "xlsx",name_starts_with = name_starts_with) %>%
      dplyr::mutate(across(starts_with("Kopftext"),as.character),
                    dplyr::across(dplyr::starts_with("Positionstext"),as.character),
                    dplyr::across(dplyr::starts_with("Sachkonto_GL_account"),as.character),
                    dplyr::across(dplyr::starts_with("Kundenauftrag_sales_order"),as.character),
                    dplyr::across(dplyr::starts_with("Kostenstelle_customer_cost_center"),as.character),
                    dplyr::across(dplyr::starts_with("PSPElement_Kunde"),as.character),
                    dplyr::across(dplyr::starts_with("Steuerklassi_fikation_Kunde_tax_classification_customer"),as.character),

             Referenz = as.character(Referenz))
  }

  if (nrow(files > 0)) {
    files_combined <- purrr::map(1:nrow(files), ~ aggregate_files(tmp_folder,name_starts_with = files$filename[.])) %>%
      purrr::list_rbind()

    openxlsx::write.xlsx(files_combined ,
                         paste0(output_folder,"buchungsfile_Studyflix_",
                                format(Sys.time(), "%Y-%m-%d_%H-%M-%S"),".xlsx"))
  } else {

    print("no files to export to jp5")

  }

  ## clean up the temporary folder after saving it to the export
  all_files <- list.files(tmp_folder,full.names = TRUE)
  purrr::map(all_files, ~ unlink(.))

}
