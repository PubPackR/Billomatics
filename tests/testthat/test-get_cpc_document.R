testthat::test_that("cpc_campaign_extracted", {

  df_items<- read.csv2(testthat::test_path("test_cpc_function.csv"))
  test_out <- read.csv2(testthat::test_path("test_cpc_function_out.csv"))

  fun_out <- get_cpc_document(df_items,
                   document_type = "confirmation",
                   field = "description")
  testthat::expect_equal(fun_out, test_out)
})
