test_that("assert_jobs_fresh: fresh, stale, missing and multiple keys", {

  # The function's only outside contact is the one dbGetQuery() against
  # raw.metadata_jobs_and_datafiles. Mocking it is enough for the whole
  # decision logic - no database is involved.
  fake_marks <- function(...) {
    data.frame(
      key = c("job_fresh", "job_stale", "job_no_timestamp"),
      updated_at = c(
        Sys.time() - as.difftime(2, units = "hours"),
        Sys.time() - as.difftime(48, units = "hours"),
        as.POSIXct(NA)
      ),
      stringsAsFactors = FALSE
    )
  }

  testthat::local_mocked_bindings(dbGetQuery = fake_marks, .package = "DBI")

  # 1. A fresh mark passes and comes back invisibly
  status <- assert_jobs_fresh(NULL, "job_fresh")
  expect_equal(status$job_key, "job_fresh")
  expect_s3_class(status$last_run, "POSIXct")

  # 2. A stale mark aborts - this is the case the function exists for
  expect_error(assert_jobs_fresh(NULL, "job_stale"), "older than 26 hours")

  # 3. A missing key aborts too. It is indistinguishable from a job that never
  #    ran, so it must not pass silently.
  expect_error(assert_jobs_fresh(NULL, "job_never_stamped"), "No run mark for")

  # 4. A mark without a timestamp counts as stale, not as fresh
  expect_error(assert_jobs_fresh(NULL, "job_no_timestamp"), "no timestamp")

  # 5. Several keys at once: one bad one is enough to abort, and the message
  #    names the offender rather than the whole list
  expect_error(
    assert_jobs_fresh(NULL, c("job_fresh", "job_stale")),
    "job_stale"
  )
  expect_silent(assert_jobs_fresh(NULL, c("job_fresh")))

  # 6. The threshold is a parameter, not a constant: the 48h-old mark passes
  #    once the caller allows it
  expect_equal(
    assert_jobs_fresh(NULL, "job_stale", max_age_hours = 72)$job_key,
    "job_stale"
  )
})
