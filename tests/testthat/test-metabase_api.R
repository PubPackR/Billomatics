test_that("metabase_sanitize_card entfernt dynamische Felder", {
  card <- list(
    id                     = 12,
    name                   = "Umsatz pro Monat",
    dataset_query          = list(type = "native", native = list(query = "SELECT 1")),
    updated_at             = "2026-01-01T10:00:00Z",
    created_at             = "2025-01-01T10:00:00Z",
    query_average_duration = 1234,
    creator_id             = 5,
    can_write              = TRUE
  )

  out <- metabase_sanitize_card(card)

  expect_named(out, c("id", "name", "dataset_query"))
  expect_equal(out$dataset_query$native$query, "SELECT 1")
})

test_that("metabase_sanitize_card laesst Karten ohne dynamische Felder unveraendert", {
  card <- list(id = 1, name = "X", dataset_query = list(type = "query"))
  expect_equal(metabase_sanitize_card(card), card)
})

test_that("metabase_sanitize_card lehnt Nicht-Listen ab", {
  expect_error(metabase_sanitize_card("kein card objekt"), "Liste")
})
