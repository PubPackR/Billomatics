
test_that("filter_positions_to_bill", {

  df_sap_posten <- openxlsx::read.xlsx(testthat::test_path("Copy of EXPORT_test.xlsx")) %>%
    filter(Document.Type %in% c(
      "SE",
      "SV",
      "DZ",
      "RV",
      "RZ",
      "RG")) %>%
    mutate(Posting.Date = as.Date(Posting.Date, origin = "1899-12-30"))

  df_sap_posten_confirmation <- df_sap_posten %>%
    mutate(AB_referenz = str_extract_all(Reference,"(AB\\d{4}SF\\d{4})"),
           AB_zuordnung = str_extract_all(Assignment,"(AB\\d{4}SF\\d{4})")) %>%
    unnest(cols = c(AB_referenz,AB_zuordnung), keep_empty = TRUE) %>%
    mutate(confirmation_number = coalesce(AB_referenz, AB_zuordnung))

  df_all_positions <- openxlsx::read.xlsx(testthat::test_path("buchungsfile_test.xlsx")) %>%
    mutate(Startdatum_Vertrag = as.Date(as.numeric(Startdatum_Vertrag), origin = "1899-12-30"))

  df_out <- openxlsx::read.xlsx(testthat::test_path("buchungsfile_test_out.xlsx")) %>%
    mutate(Startdatum_Vertrag = as.Date(as.numeric(Startdatum_Vertrag), origin = "1899-12-30"),
           last_posting_date = as.Date(as.numeric(last_posting_date), origin = "1899-12-30"))

  fun_out <- filter_positions2bill(df_sap_posten_confirmation, df_all_positions)

  testthat::expect_equal(fun_out, df_out)

})
