test_that("get_information_from_comments_cpc", {
  ## get the datasets
  df_comments<- readRDS(testthat::test_path("comments_test.RDS"))
  df_comments_cpc<- readRDS(testthat::test_path("comments_test_cpc_out.RDS"))

  ## run the function
  fun_out <- get_information_from_comments(df_comments,
                              document_type = "confirmation",
                              desired_information = "CPC Kampagne")
  ## testing if the CPC campaign information is retrieved
  testthat::expect_equal(fun_out, df_comments_cpc)


})

test_that("get_information_from_comments_rechnungsinformation", {

  ## get the datasets
  df_comments<- readRDS(testthat::test_path("comments_test.RDS"))
  df_comments_rechnungsinfo<- readRDS(testthat::test_path("comments_test_rechnungsInfo_out.RDS"))

  ## run the function
  fun_out <- get_information_from_comments(df_comments,
                                           document_type = "confirmation",
                                           desired_information = "Rechnungszusatzinformation")

  ## testing if the CPC campaign information is retrieved
  testthat::expect_equal(fun_out, df_comments_rechnungsinfo,)



})
