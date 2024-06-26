
testthat::test_that("create_header_billing_info_text", {

  #### read the test data----
  df_positions_test<- readRDS(testthat::test_path("df_positions_test.RDS"))
  df_information_bill_test <- readRDS(testthat::test_path("consolidateInfo_intro_out.RDS"))
  #### read the expected resulting data
  df_create_header_billing_info_out <- readRDS(testthat::test_path("create_header_billing_info_text_intro_out.RDS"))
  #### run the test ----
  fun_out <- create_header_billing_info_text(df_positions = df_positions_test,
                                             df_information_bill = df_information_bill_test)

  testthat::expect_equal(fun_out,df_create_header_billing_info_out)


})




testthat::test_that("get_deviating_invoice_recipient",{
  df_info_test <-  readRDS(testthat::test_path("df_info_invoice_recipient_test.RDS"))
  df_info_rec_out <- readRDS(testthat::test_path("df_info_deviating_recipient_out.RDS"))


  ### run the test ----
  fun_out <-get_deviating_invoice_recipient(Information_bill_df = df_info_test)
  testthat::expect_equal(fun_out,df_info_rec_out)


})


testthat::test_that("create_invoice_recipient",{
  df_doc_test<- readRDS(testthat::test_path("df_doc_invoice_recipient_test.RDS"))
  df_info_test <-  readRDS(testthat::test_path("df_info_invoice_recipient_test.RDS"))

  df_fun_out <-  readRDS(testthat::test_path("create_invoice_recipient_out.RDS"))



  ### run the test ----
  fun_out <-create_invoice_recipient(df_positions = df_doc_test,
                                     Information_bill_df = df_info_test)

  testthat::expect_equal(fun_out,df_fun_out)


})
