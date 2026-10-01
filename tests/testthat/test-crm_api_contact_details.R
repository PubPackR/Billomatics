test_that("get_crm_contact_details() labels contact_type from the loop, not from the API's own type field", {
  # The CRM API returns a `type` field inside every contact detail ("Tel", "Email", ...)
  # as well as its own attachable_id / attachable_type ("Person"). Inside mutate() these
  # columns must not shadow the local variables of the same name.
  api_json <- '[{"email":{"id":11,"type":"Email","attachable_id":123456,"attachable_type":"Person","atype":"office","name":"a@b.de"}},
                {"tel":{"id":12,"type":"Tel","attachable_id":123456,"attachable_type":"Person","atype":"office_hq","name":"+49 821 1"}}]'

  mockery::stub(get_crm_contact_details, "httr::GET", mockery::mock(NULL))
  mockery::stub(get_crm_contact_details, "httr::status_code", 200)
  mockery::stub(get_crm_contact_details, "httr::content", api_json)

  result <- get_crm_contact_details(
    headers = c("X-apikey" = "dummy"),
    df = data.frame(attachable_id = 123456, attachable_type = "people")
  )

  expect_equal(nrow(result), 2L)
  expect_equal(result$contact_type[result$id == 12], "tel")
  expect_equal(result$contact_type[result$id == 11], "email")
  expect_equal(unique(result$attachable_type), "people")
  expect_equal(unique(result$attachable_id), 123456)
})
