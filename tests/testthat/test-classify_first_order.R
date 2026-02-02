test_that("classify_first_order: standard and edge cases", {
  library(lubridate)
  library(dplyr)

  # 1. Standard: Single client, clear first and follow-up orders
  df1 <- tibble(
    client_id = c(1, 1, 1),
    confirmation_number = c("A", "B", "C"),
    order_date = as.Date(c("2020-01-01", "2021-01-01", "2024-01-01")),  # Changed from 2023-09-01
    service_start = as.Date(c("2020-01-10", "2021-01-10", "2024-01-10")),
    service_end = as.Date(c("2020-12-31", "2021-12-31", "2024-12-31"))
  )
  result1 <- classify_first_order(df1)
  expect_equal(result1$is_first_order, c(TRUE, FALSE, TRUE))

  # 2. Parallel contracts for same client (overlapping service periods)
  df2 <- tibble(
    client_id = c(2, 2),
    confirmation_number = c("D", "E"),
    order_date = as.Date(c("2022-01-01", "2022-01-01")),
    service_start = as.Date(c("2022-01-10", "2022-01-10")),
    service_end = as.Date(c("2022-12-31", "2022-12-31"))
  )
  result2 <- classify_first_order(df2)
  expect_equal(result2$is_first_order, c(TRUE, FALSE))

  # 3. Multiple clients, each with only one contract
  df3 <- tibble(
    client_id = c(3, 4),
    confirmation_number = c("F", "G"),
    order_date = as.Date(c("2021-05-01", "2021-06-01")),
    service_start = as.Date(c("2021-05-10", "2021-06-10")),
    service_end = as.Date(c("2022-05-09", "2022-06-09"))
  )
  result3 <- classify_first_order(df3)
  expect_true(all(result3$is_first_order))

  # 4. Gap exactly 24 months (should be follow-up, not first order)
  df4 <- tibble(
    client_id = c(5, 5),
    confirmation_number = c("H", "I"),
    order_date = as.Date(c("2020-01-01", "2022-01-01")),
    service_start = as.Date(c("2020-01-10", "2022-01-10")),
    service_end = as.Date(c("2020-12-31", "2022-12-31"))
  )
  result4 <- classify_first_order(df4)
  expect_equal(result4$is_first_order, c(TRUE, FALSE))

  # 5. Gap > 24 months (should be first order)
  df5 <- tibble(
    client_id = c(6, 6),
    confirmation_number = c("J", "K"),
    order_date = as.Date(c("2020-01-01", "2023-02-02")),  # Changed from 2022-02-02
    service_start = as.Date(c("2020-01-10", "2023-02-10")),
    service_end = as.Date(c("2020-12-31", "2023-12-31"))
  )
  result5 <- classify_first_order(df5)
  expect_equal(result5$is_first_order, c(TRUE, TRUE))

  # 6. Missing required columns
  df6 <- tibble(
    client_id = 7,
    confirmation_number = "L",
    order_date = as.Date("2021-01-01")
    # missing service_start and service_end
  )
  expect_error(classify_first_order(df6), "Missing required columns")

  # 7. NA in service_end for first contract (should still be first order)
  df7 <- tibble(
    client_id = c(8, 8),
    confirmation_number = c("M", "N"),
    order_date = as.Date(c("2021-01-01", "2022-01-01")),
    service_start = as.Date(c("2021-01-10", "2022-01-10")),
    service_end = as.Date(c(NA, "2022-12-31"))
  )
  result7 <- classify_first_order(df7)
  expect_equal(result7$is_first_order, c(TRUE, TRUE))

  # 8. Multiple contracts on same day, different confirmation numbers
  df8 <- tibble(
    client_id = c(9, 9, 9),
    confirmation_number = c("O", "P", "Q"),
    order_date = as.Date(c("2022-01-01", "2022-01-01", "2025-02-01")),  # Changed from 2024-12-01
    service_start = as.Date(c("2022-01-10", "2022-01-10", "2025-02-10")),
    service_end = as.Date(c("2022-12-31", "2022-12-31", "2025-12-31"))
  )
  result8 <- classify_first_order(df8)
  expect_equal(result8$is_first_order, c(TRUE, FALSE, TRUE))

  # 9. Client with contracts out of chronological order in input
  df9 <- tibble(
    client_id = c(10, 10, 10),
    confirmation_number = c("R", "S", "T"),
    order_date = as.Date(c("2022-05-01", "2020-01-01", "2025-06-01")),  # Changed from 2024-03-01
    service_start = as.Date(c("2022-05-10", "2020-01-10", "2025-06-10")),
    service_end = as.Date(c("2023-05-09", "2020-12-31", "2025-12-31"))
  )
  result9 <- classify_first_order(df9)
  # Should sort by order_date internally, so first is TRUE, next FALSE, last TRUE (gap > 24 months)
  expect_equal(result9$is_first_order[order(df9$order_date)], c(TRUE, FALSE, TRUE))

  # 10. Client with only NA dates (should error or all TRUE)
  df10 <- tibble(
    client_id = c(11, 11),
    confirmation_number = c("U", "V"),
    order_date = as.Date(c(NA, NA)),
    service_start = as.Date(c(NA, NA)),
    service_end = as.Date(c(NA, NA))
  )
  expect_true(all(is.na(classify_first_order(df10)$is_first_order)))

  # 11. Different clients with contracts on same day
  df11 <- tibble(
    client_id = c(12, 13),
    confirmation_number = c("W", "X"),
    order_date = as.Date(c("2022-01-01", "2022-01-01")),
    service_start = as.Date(c("2022-01-10", "2022-01-10")),
    service_end = as.Date(c("2022-12-31", "2022-12-31"))
  )
  result11 <- classify_first_order(df11)
  expect_true(all(result11$is_first_order))

  # 12. Client with NA in previous_service_end (should be TRUE for first, then depends)
  df12 <- tibble(
    client_id = c(14, 14),
    confirmation_number = c("Y", "Z"),
    order_date = as.Date(c("2021-01-01", "2022-01-01")),
    service_start = as.Date(c("2021-01-10", "2022-01-10")),
    service_end = as.Date(c(NA, "2022-12-31"))
  )
  result12 <- classify_first_order(df12)
  expect_equal(result12$is_first_order, c(TRUE, TRUE))

  # 13. Client with negative gap (order_date before previous service_end)
  df13 <- tibble(
    client_id = c(15, 15),
    confirmation_number = c("AA", "AB"),
    order_date = as.Date(c("2022-01-01", "2021-01-01")),
    service_start = as.Date(c("2022-01-10", "2021-01-10")),
    service_end = as.Date(c("2022-12-31", "2021-12-31"))
  )
  result13 <- classify_first_order(df13)
  # Should sort by order_date, so first TRUE, second FALSE (negative gap)
  expect_equal(result13$is_first_order[order(df13$order_date)], c(TRUE, FALSE))

  # 14. Dataframe with duplicate confirmation_numbers (should warn or handle gracefully)
  df14 <- tibble(
    client_id = c(16, 16),
    confirmation_number = c("AC", "AC"),
    order_date = as.Date(c("2022-01-01", "2023-01-01")),
    service_start = as.Date(c("2022-01-10", "2023-01-10")),
    service_end = as.Date(c("2022-12-31", "2023-12-31"))
  )
  result14 <- classify_first_order(df14)
  expect_true(all(result14$is_first_order %in% c(TRUE, FALSE)))

  # 15. Dataframe with only one row
  df15 <- tibble(
    client_id = 17,
    confirmation_number = "AD",
    order_date = as.Date("2022-01-01"),
    service_start = as.Date("2022-01-10"),
    service_end = as.Date("2022-12-31")
  )
  result15 <- classify_first_order(df15)
  expect_true(result15$is_first_order)
})