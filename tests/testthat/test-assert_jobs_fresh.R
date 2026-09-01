test_that("check_job_marks: fresh, stale, missing and multiple keys", {

  # Kein Mock und keine Verbindung: assert_jobs_fresh() liest die Tabelle,
  # check_job_marks() entscheidet. Getestet wird die Entscheidung, und die
  # nimmt ein schlichtes data.frame entgegen.
  jetzt <- as.POSIXct("2026-09-01 12:00:00", tz = "UTC")

  marken <- data.frame(
    key = c("job_fresh", "job_stale", "job_no_timestamp"),
    updated_at = c(
      jetzt - as.difftime(2, units = "hours"),
      jetzt - as.difftime(48, units = "hours"),
      as.POSIXct(NA)
    ),
    stringsAsFactors = FALSE
  )

  # 1. Eine frische Marke passiert und kommt unsichtbar zurueck
  stand <- check_job_marks(marken, "job_fresh", now = jetzt)
  expect_equal(stand$job_key, "job_fresh")
  expect_s3_class(stand$last_run, "POSIXct")

  # 2. Eine veraltete Marke bricht ab - dafuer gibt es die Funktion
  expect_error(
    check_job_marks(marken, "job_stale", now = jetzt),
    "older than 26 hours"
  )

  # 3. Ein fehlender Schluessel bricht ebenfalls ab. Von einem Job, der nie
  #    gelaufen ist, ist er nicht zu unterscheiden, er darf nicht durchwinken.
  expect_error(
    check_job_marks(marken, "job_never_stamped", now = jetzt),
    "No run mark for"
  )

  # 4. Eine Marke ohne Zeitstempel gilt als veraltet, nicht als frisch
  expect_error(
    check_job_marks(marken, "job_no_timestamp", now = jetzt),
    "no timestamp"
  )

  # 5. Mehrere Schluessel: eine schlechte reicht zum Abbruch, und die Meldung
  #    nennt den Schuldigen statt der ganzen Liste
  expect_error(
    check_job_marks(marken, c("job_fresh", "job_stale"), now = jetzt),
    "job_stale"
  )

  # 6. Die Schwelle ist ein Parameter und keine Konstante
  expect_equal(
    check_job_marks(marken, "job_stale", max_age_hours = 72, now = jetzt)$job_key,
    "job_stale"
  )

  # 7. Eine leere Markentabelle winkt nicht durch, sondern bricht ab. Das ist
  #    der Zustand direkt nach einem Deploy, bevor der Vorgaenger gestempelt hat.
  leer <- marken[0, ]
  expect_error(check_job_marks(leer, "job_fresh", now = jetzt), "No run mark for")
})
