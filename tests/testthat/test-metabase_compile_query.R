test_that("Fehlermeldung bei 403 enthaelt den Response-Body", {
  app <- webfakes::new_app()
  app$get("/api/collection", function(req, res) {
    res$set_status(403)$send("<html><title>Sign In</title></html>")
  })
  srv <- webfakes::new_app_process(app)
  on.exit(srv$stop(), add = TRUE)

  err <- expect_error(
    Billomatics:::metabase_request("GET", c("api", "collection"),
                                   "mb_test", srv$url(), max_retries = 0)
  )
  expect_match(conditionMessage(err), "Sign In", fixed = TRUE)
  expect_match(conditionMessage(err), "403", fixed = TRUE)
})

test_that("metabase_compile_query gibt die kompilierte SQL zurueck", {
  app <- webfakes::new_app()
  app$post("/api/dataset/native", function(req, res) {
    res$set_status(200)$send_json(list(query = "SELECT 1", params = NULL),
                                  auto_unbox = TRUE)
  })
  srv <- webfakes::new_app_process(app)
  on.exit(srv$stop(), add = TRUE)

  dq  <- list(`lib/type` = "mbql/query", database = 3,
              stages = list(list(`lib/type` = "mbql.stage/mbql", `source-table` = 207)))
  sql <- Billomatics::metabase_compile_query(dq, "mb_test", srv$url(), max_retries = 0)

  expect_type(sql, "character")
  expect_length(sql, 1)
  expect_equal(sql, "SELECT 1")
})

test_that("metabase_compile_query wirft, wenn die Antwort kein query-Feld hat", {
  app <- webfakes::new_app()
  app$post("/api/dataset/native", function(req, res) {
    res$set_status(200)$send_json(list(params = NULL), auto_unbox = TRUE)
  })
  srv <- webfakes::new_app_process(app)
  on.exit(srv$stop(), add = TRUE)

  expect_error(
    Billomatics::metabase_compile_query(list(a = 1), "mb_test", srv$url(), max_retries = 0),
    "kein Feld 'query'"
  )
})
