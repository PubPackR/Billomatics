library(tidyverse)
library(testthat)
source(here::here("R", "extract_information_billomat_functions.R"))

test_that("fixed_commont_typos in note", {
  ## get the datasets
  test_df <- tibble(
    id = c(1, 2),
    note = c(
      "GeplanterLaufzeitbeginn: 01.02.2021 und geplanter Leistungsbeginn: 01.02.2021",
      "GeplanterLaufzeitbeginn: 10.10.2023 Geplanter Leistungsbeginn: 10.10.2023"
    )
  )

  ## run the function
  fun_out <- extract_note(test_df, field = "note") |>
    unnest(cols = c(note))

  ## testing if the typos are fixed
  testthat::expect_equal(
    fun_out,
    tibble(
      id = c(1, 1, 1, 2, 2, 2),
      note = c(
        "",
        "Laufzeitbeginn: 01.02.2021 und ",
        "Leistungsbeginn: 01.02.2021",
        "",
        "Laufzeitbeginn: 10.10.2023 ",
        "Leistungsbeginn: 10.10.2023"
      )
    )
  )
})


test_that("get_information_from_comments_cpc", {
  ## get the datasets
  df_comments <- readRDS(testthat::test_path("comments_test.RDS"))
  df_comments_cpc <- readRDS(testthat::test_path(
    "comments_test_cpc_out.RDS"
  )) %>%
    mutate(document_id = as.numeric(document_id))

  ## run the function
  fun_out <- get_information_from_comments(
    df_comments,
    document_type = "confirmation",
    desired_information = "CPC Kampagne"
  )
  ## testing if the CPC campaign information is retrieved
  testthat::expect_equal(fun_out, df_comments_cpc)
})

test_that("get_information_from_comments_rechnungsinformation", {
  ## get the datasets
  df_comments <- readRDS(testthat::test_path("comments_test.RDS"))
  df_comments_rechnungsinfo <- readRDS(testthat::test_path(
    "comments_test_rechnungsInfo_out.RDS"
  ))

  ## run the function
  fun_out <- get_information_from_comments(
    df_comments,
    document_type = "confirmation",
    desired_information = "Rechnungszusatzinformation"
  )

  ## testing if the CPC campaign information is retrieved
  testthat::expect_equal(fun_out, df_comments_rechnungsinfo)
})


testthat::test_that("get_information_from_confirmation", {
  ## get the datasets
  df_test <- readRDS(testthat::test_path("intro_test.RDS"))
  df_test_out <- readRDS(testthat::test_path("intro_test_out.RDS"))

  ## run the function
  fun_out <- get_invoice_information_from_document(df_test, field = "intro")

  ## testing if the CPC campaign information is retrieved
  testthat::expect_equal(fun_out, df_test_out)
})

testthat::test_that("get_information_from_invoice", {
  ## get the datasets
  df_test <- readRDS(testthat::test_path("note_test.RDS"))
  df_test_out <- readRDS(testthat::test_path("note_test_out.RDS"))

  ## run the function
  fun_out <- get_invoice_information_from_document(df_test, field = "note")

  ## testing if the CPC campaign information is retrieved
  testthat::expect_equal(fun_out, df_test_out)
})

testthat::test_that("get_information_from_wrong_field", {
  ## get the datasets
  df_test <- readRDS(testthat::test_path("intro_test.RDS"))

  ## run the function
  fun_out <- get_invoice_information_from_document(df_test, field = "test")

  ## testing if the CPC campaign information is retrieved
  testthat::expect_equal(fun_out, "no known field")
})


testthat::test_that("consolidate_information from comments and document", {
  ## get the datasets
  df_doc_intro_test <- readRDS(testthat::test_path("intro_test_out.RDS"))
  df_doc_note_test <- readRDS(testthat::test_path("note_test_out.RDS"))
  df_comment_test <- readRDS(testthat::test_path(
    "comments_test_rechnungsInfo_out.RDS"
  ))

  ## output

  df_consolidate_intro_test <- readRDS(testthat::test_path(
    "consolidateInfo_intro_out.RDS"
  )) %>%
    mutate(document_id = as.numeric(document_id))
  df_consolidate_note_test <- readRDS(testthat::test_path(
    "consolidateInfo_note_out.RDS"
  )) %>%
    mutate(document_id = as.numeric(document_id))

  ## run the function
  fun_out <- consolidate_invoice_information(
    df_Billing_information_comment = df_comment_test,
    df_Billing_information_document = df_doc_intro_test
  )

  ## testing if the CPC campaign information is retrieved
  testthat::expect_equal(fun_out, df_consolidate_intro_test)

  fun_out <- consolidate_invoice_information(
    df_Billing_information_comment = df_comment_test,
    df_Billing_information_document = df_doc_note_test
  )

  testthat::expect_equal(fun_out, df_consolidate_note_test)
})
