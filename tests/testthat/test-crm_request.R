test_that("crm_request: unsupported HTTP method throws error", {
  expect_error(
    crm_request("PATCH", "https://example.com", c("X-apikey" = "test")),
    "Unsupported HTTP method"
  )
})

test_that("crm_request wrapper functions exist and are callable", {
  # Test that wrapper functions are defined

expect_true(is.function(crm_POST))
  expect_true(is.function(crm_PUT))
  expect_true(is.function(crm_DELETE))
  expect_true(is.function(crm_GET))
})
