#### This script contains all functions to
#### create a monthly table from a start and end date
#### create the nrr and dbner for a given data set

## Funktionen für die Datenaufbereitung
##
##
#' df_transpose
#'
#' this function transposes a table

#' @param df the tabel to transpose
#' @return a tibble

#' @export
df_transpose <- function(df) {
  df %>%
    tidyr::pivot_longer(-1) %>%
    tidyr::pivot_wider(names_from = 1, values_from = value)

}

## Funktionen für die Datenaufbereitung
##
#' split_contract_into_days_per_month
#'
#' this function takes a starting and ending date and turns it into as many rows as months.
#'
#' @param start_date the starting date has to be in the format mm-dd-yyyy
#' @param start_date the ending date has to be in the format mm-dd-yyyy
#'
#' @return a tibble
#' @export
split_contract_into_days_per_month <-
  function(date_start, date_end) {
    # the contract is OVER on the NEXT day otherwise his misses a day
    corrected_date_end <- date_end + lubridate::days(1)

    # contract is OVER NEXT month, so this allows me to not stop one month too soon
    # create the length of the interval from month start till end
    month_n <-
      lubridate::interval(
        lubridate::floor_date(date_start, unit = "months"),
        lubridate::ceiling_date((corrected_date_end), unit = "months")
      ) %/% months(1)

    # pre-allocate for speed and create log place for the months and all days for a given contract
    all_months <- rep(lubridate::NA_Date_, month_n)
    all_days_in_month <- rep(NA, month_n)
    # so for each month in a sequence of months i set the start and end date of the month
    for (iM in 1:month_n) {
      # when it is the first month, i have to start counting on the day the contract starts
      if (iM == 1) {
        # starting month
        d_start <- date_start
      }  else {
        # any other month I start on the first day of the month
        d_start <- lubridate::floor_date(d_start, "month")
      }
      # when I get to the last month in which the contract ran, then I have to take the end of the contract as end
      if (iM == month_n) {
        # ending month
        d_end <- corrected_date_end
      } else {
        # any other month take the end of the month as last day of counter
        d_end <- lubridate::ceiling_date(d_start, "month")
      }

      # logging
      ## keep the month
      all_months[iM] <- lubridate::floor_date(d_start, "month")
      ## keep the days of the month in which the contract ran
      all_days_in_month[iM] <- lubridate::interval(d_start,
                                                   d_end) %/% lubridate::days(1)

      # next month, please!
      # move the start up one month, this works because we then set it to the floor if it is not the first month

      if (iM <= month_n) {
        # add with rollback means that it will always move to the next first day of a month,
        # even if date + month(1) would put you on the last day of the month
        # eg: 1.1. + 1month = 31.1. while 1.2. + 1month = 3.3.
        d_start <- lubridate::add_with_rollback(d_start, months(1))
      }
    }
    # save the monthly split up information as tibble
    tidyr::tibble(
      StartDate = date_start,
      EndDate = date_end,
      Month = all_months,
      #Months_running = months_running,
      ContractDaysInMonth = all_days_in_month,
    ) |>
      dplyr::filter(ContractDaysInMonth > 0)
  }


#' revenue_month
#'
#' Here I compute the revenue per month considering the booking date and the sales person.
#' Function is used within the compute_revenue_month() function.
#' This #does not calculate any KPI, only the revenue stream.
#' this file contains the functions to split revenues in order to later calculate kpis
#'
#' @param df the data frame which must be passed from compute_revenue_month(), there the data is prepared
#' @param current_period, the period of interest
#' @param previous_period, the period to compare the current period to
#' @param revenue the variable containing the revenue
#'
#' @return returns a tibble with grouped revenue streams
#'
#' @export
revenue_month <-
  function(df,
           current_period,
           previous_period,
           revenue) {
    df <- df %>%
      # removing company types
      dplyr::mutate(Kunde = compress_column(Kunde))
    ### get all the orders in the previous period
    df_past <- df %>%
      dplyr::filter(time_period %in% previous_period ,
                    is.finite(revenue))

    ## get all the customers that have at least one order that ends in the previous year,so that they are truly recurring
    potential_recurrent_customers <-
      df_past %>%
      dplyr::select(Kunde, orderID) %>%
      dplyr::distinct(Kunde, orderID) %>%
      dplyr::group_by(Kunde, orderID) %>%
      dplyr::mutate(numberOfOrders = dplyr::n()) %>%
      dplyr::group_by(Kunde) %>%
      dplyr::mutate(orderNumber = cumsum(numberOfOrders))


    # either the current order is a second order that started in the previous period
    # OR the current order is another order that is not in the previous period

    # all current orders
    df_current <- df %>%
      dplyr::filter(time_period %in% current_period,
                    is.finite(revenue)) %>%
      # classify order according to new or recurring

      dplyr::mutate(customer_type =
                      dplyr::case_when(
                        ## conditions are either the order was not first OR the order is different
                        # first condition that qualifies as recurring customer is
                        # the current order is a second order that started in the previous period
                        (Kunde %in% potential_recurrent_customers$Kunde[potential_recurrent_customers$orderNumber >
                                                                          1]) |
                          # second possible case the order is a new order AND customer was in the past period - this also applies to all cases given the Auftragdatum
                          (
                            Kunde %in% potential_recurrent_customers$Kunde &
                              !(orderID %in% potential_recurrent_customers$orderID)
                          )

                        ~ "recurrent",
                        TRUE ~ "new"
                      ))
    ## get the sum of the current revenue
    current_revenue <- df_current %>%
      dplyr::group_by(
        Month,
        customer_type,
        Vertriebler,
        Kunde,
        Auftragseingang = lubridate::floor_date(Auftragsdatum, unit = "month")
      ) %>%
      dplyr::summarise (current_revenue_sum = sum(revenue),
                        .groups = "drop")

    ## calculate revenue streams
    recurring_revenue <- current_revenue %>%
      dplyr::filter(customer_type == "recurrent") %>%
      dplyr::group_by(Month, Vertriebler, Auftragseingang,
                      Kunde) %>% #
      dplyr::summarise(sum_recurring = sum(current_revenue_sum, na.rm = TRUE),
                       .groups = "drop")

    new_revenue <- current_revenue %>%
      dplyr::filter(customer_type == "new") %>%
      dplyr::group_by(Month, Auftragseingang, Vertriebler,
                      Kunde) %>% #
      dplyr::summarise(sum_new = sum(current_revenue_sum, na.rm = TRUE),
                       .groups = "drop")

    revenue_month <- new_revenue %>%
      dplyr::full_join(recurring_revenue,
                       by = c("Month", "Auftragseingang", "Vertriebler", "Kunde")) %>%
      dplyr::mutate(
        sum_recurring = ifelse(is.na(sum_recurring), 0, sum_recurring),
        sum_new = ifelse(is.na(sum_new), 0, sum_new),
        total_revenue = sum_new + sum_recurring,
        Leistungs_Jahr = current_period
      ) %>%
      dplyr::select(
        Leistungs_Jahr,
        Month,
        Auftragseingang,
        Kunde,
        Vertriebler,
        total_revenue,
        sum_recurring,
        sum_new
      )
    tidyr::tibble(revenue_month)
  }



#' compute_revenue_month
#'
#' Here I compute the revenue per month considering the booking date and the sales person.
#' Function uses the revenue_month() function to purrr over the df creating the revenue types
#'
#' @param df the data frame which must contain the customer ("Kunde"), orderID, Month and MonthlyRevenue
#'
#' @return returns a tibble with grouped revenue streams
#'
#' @export
compute_revenue_month <- function(df) {
  df <- df %>%
    dplyr::mutate(
      time_period = lubridate::year(Month),
      month_in_period = lubridate::month(Month),
      revenue = MonthlyRevenue,
      Kunde = compress_column(Kunde)
    ) %>%
    dplyr::ungroup()
  years_of_interest <- sort(unique(df$time_period))
  # instead of subtracting one year - which was possible with just -1, I had to subtract the month - for this I had to explicitly use a date notation
  purrr::map_dfr(years_of_interest, ~ revenue_month(df, ., . - 1, revenue))
}


#' yearly_nrr_for_month
#'
#' This is a helper function where I compute the nrr and dbner, the nrr and dbner can either be for the company or overall.
#' The function is called within the compute_nrr_for_month() which also passes the data
#'
#' @param df date frame containing the months and revenue streams
#'
#' @param current_period, the period of interest
#'
#' @param previous_period, the period to compare the current period to
#'
#' @param revenue the variable containing the revenue
#'
#' @param focus_company whether an overall (FALSE) or company specific (TRUE) NRR should be calculated
#'
#' @param focus_year whether the focus of the KPI is the year (TRUE) or a monthly number (FALSE)
#'
#' @return returns a tibble with a calculated NRR
#'
#' @export
yearly_nrr_for_month <-
  function(df,
           current_period,
           previous_period,
           revenue,
           focus_company,
           focus_year) {
    ## with agentur ################################
    df <- df %>%
      dplyr::ungroup() %>%
        # removing company types
        dplyr::mutate(Kunde = compress_column(Kunde))
    ### get all the orders in the previous period
    df_past <- df %>%
      dplyr::filter(time_period %in% previous_period ,
                    is.finite(revenue))
    past_revenue_sum <- sum(df_past$revenue)

    ## get all the customers that have at least one order that ends in the previous year,so that they are truly recurring
    potential_recurrent_customers <-
      df_past %>%
      dplyr::select(Kunde, orderID) %>%
      dplyr::distinct(Kunde, orderID) %>%
      dplyr::group_by(Kunde, orderID) %>%
      dplyr::mutate(numberOfOrders = dplyr::n()) %>%
      dplyr::group_by(Kunde) %>%
      dplyr::mutate(orderNumber = cumsum(numberOfOrders))


    # either the current order is a second order that started in the previous period
    # OR the current order is another order that is not in the previous period

    # all current orders
    df_current <- df %>%
      dplyr::filter(time_period %in% current_period,
                    is.finite(revenue)) %>%
      # classify order according to new or recurring

      dplyr::mutate(revenue_type =
                      dplyr::case_when(
                        ## conditions are either the order was not first OR the order is different
                        # first condition that qualifies as recurring customer is
                        # the current order is a second order that started in the previous period
                        (Kunde %in% potential_recurrent_customers$Kunde[potential_recurrent_customers$orderNumber >
                                                                          1]) |
                          # second possible case the order is a new order AND customer was in the past period - this also applies to all cases given the Auftragsdatum
                          (
                            Kunde %in% potential_recurrent_customers$Kunde &
                              !(orderID %in% potential_recurrent_customers$orderID)
                          )

                        ~ "recurrent",
                        (
                          Kunde %in% potential_recurrent_customers$Kunde &
                            orderID %in% potential_recurrent_customers$orderID
                        ) ~ "recurrent_new",
                        TRUE ~ "new"
                      ))

    ### I need to expand the grid to show all combinations of months and type
    floorPeriod <-
      lubridate::floor_date(lubridate::ymd(current_period, truncated = 2L), unit = "months")
    ceilingPeriod <-
      lubridate::ceiling_date(lubridate::ymd(current_period, truncated = 2L) + months(11),
                              unit = "months") - lubridate::days(1)

    Months_type = tidyr::expand_grid(
      revenue_type = c("recurrent", "recurrent_new", "new", "churned"),
      Month = seq(
        lubridate::ymd(floorPeriod),
        lubridate::ymd(ceilingPeriod),
        by = "months"
      )
    )

    df_past <- df_past %>%
      dplyr::mutate(
        revenue_type = dplyr::case_when(
          !(Kunde %in% df_current$Kunde)  ~ "churned",
          Kunde %in% df_current$Kunde &
            orderID %in% df_current$orderID ~ "recurrent_new",
          Kunde %in% df_current$Kunde &
            !(orderID %in% df_current$orderID) ~ "recurrent"
        )
      )


    ## ---------------------- aggregating sums and customers --------------
    ## counting the types of customers and revenue per month in previous period
    previous_period_sums <- df_past %>%
      dplyr::group_by(Month, revenue_type) %>%
      dplyr::summarise(
        Kunden = dplyr::n_distinct(Kunde),
        revenue = sum(revenue),
        .groups = "drop"
      ) %>%
      dplyr::full_join(Months_type %>%
                         dplyr::mutate(Month = Month - lubridate::years(1)),
                       by = c("Month", "revenue_type")) %>%
      dplyr::mutate(
        Kunden = tidyr::replace_na(Kunden, 0),
        revenue = tidyr::replace_na(revenue, 0)
      )


    ## count types of customers in the current period according to type and month

    current_period_sums <- df_current %>%
      dplyr::group_by(Month, revenue_type) %>%
      dplyr::summarise(
        Kunden = dplyr::n_distinct(Kunde),
        revenue = sum(revenue),
        .groups = "drop"
      ) %>%
      dplyr::full_join(Months_type, by = c("Month", "revenue_type")) %>%
      dplyr::mutate(Kunden = tidyr::replace_na(Kunden, 0),
                    revenue = tidyr::replace_na(revenue, 0))

    ### put it in one tibble


    ### calculate the yearly overall sums and NRR / DBNER
    revenues_sum <- previous_period_sums %>%
      dplyr::ungroup() %>%
      dplyr::mutate(Month_int = lubridate::month(Month)) %>%
      dplyr::inner_join(
        current_period_sums %>%
          dplyr::ungroup() %>%
          dplyr::mutate(Month_int = lubridate::month(Month)),
        by = c("Month_int", "revenue_type"),
        suffix = c("_previous", "_current")
      ) %>%
      dplyr::ungroup() %>%
      dplyr::group_by(revenue_type) %>%
      dplyr::mutate(
        yearly_revenue_previous = sum(revenue_previous),
        yearly_revenue_current = sum(revenue_current)
      ) %>%
      dplyr::ungroup() %>%
      dplyr::mutate(
        yearlyAll_previous = sum(revenue_previous),
        yearlyAll_current = sum(revenue_current),
        current_period = current_period
      )

    ## classify the customers of the past period either as recurring or churned and also the current customers

    ### first collect all the company names in their respective types
    recurring <- df_current %>%
      dplyr::distinct(Kunde, revenue_type) %>%
      dplyr::filter(revenue_type == "recurrent") %>%
      dplyr::pull(Kunde)
    ## a company that becomes a recurring customer cannot be a recurring new customer,
    recurring_new <- df_current %>%
      dplyr::distinct(Kunde, revenue_type) %>%
      dplyr::filter(revenue_type == "recurrent_new") %>%
      dplyr::pull(Kunde) %>% .[!(. %in% recurring)]

    new <- df_current$Kunde[!df_current$Kunde %in% df_past$Kunde]
    churned <- df_past$Kunde[!df_past$Kunde %in% df_current$Kunde]

    current_customers <-
      tidyr::tibble(
        current_period = current_period,
        Anzahl_Bestandskunden = dplyr::n_distinct(recurring),
        Anzahl_Neukunden = dplyr::n_distinct(new),
        Anzahl_NeukundenAusVorjahr = dplyr::n_distinct(recurring_new),
        Anzahl_churn = dplyr::n_distinct(churned),
        gesamt_Kunden = dplyr::n_distinct(df_current$Kunde)
      )


    #--------------------------tibble for aggregated data on period level-----------------------------#


    ## aggregated for each period
    aggregated_kpi <- tidyr::tibble(
      # because I only want the yearly NRR, I use only yearly numbers
      revenues_sum %>%
        dplyr::distinct(
          current_period,
          revenue_type,
          yearly_revenue_previous,
          yearly_revenue_current
        )
    ) %>% dplyr::rename(previous_revenue = yearly_revenue_previous,
                        current_revenue = yearly_revenue_current) %>%
      tidyr::pivot_wider(
        names_from = revenue_type,
        values_from = c(previous_revenue, current_revenue)
      ) %>%
      dplyr::select(-c(previous_revenue_new, current_revenue_churned)) %>%
      dplyr::left_join(current_customers, by = "current_period")

    aggregated_kpi <- aggregated_kpi %>%
      dplyr::mutate(
        previous_revenue = past_revenue_sum,
        current_revenue = current_revenue_recurrent + current_revenue_recurrent_new + current_revenue_new,
        current_revenue_new_All = current_revenue_recurrent_new + current_revenue_new,
        churn = (
          past_revenue_sum - (
            previous_revenue_recurrent + previous_revenue_recurrent_new
          )
        ) * -1,
        expansion =  current_revenue_recurrent - previous_revenue_recurrent,
        #CustomerChurnRate = (churned_customers/beginning_customers * 100),
        NRR_clean = (current_revenue_recurrent / past_revenue_sum) * 100,
        NRR_simple = (current_revenue_recurrent + current_revenue_recurrent_new) / past_revenue_sum *
          100,
        #NRR_wrong = (current_revenue_recurrent) / (past_revenue_sum - previous_revenue_recurrent_new) *100,
        DBNER = (current_revenue_recurrent / previous_revenue_recurrent) *
          100,
        DBNER_alternative = (
          current_revenue_recurrent / (
            previous_revenue_recurrent + previous_revenue_recurrent_new
          )
        ) * 100
      )

    #----------------tibble for aggregated data company for each period level-----------------------#

    ## aggregated for each company and period
    company_level_past <- df_past %>%
      dplyr::ungroup() %>%
      ## getting customers total revenue for the previous period
      dplyr::group_by(Kunde, time_period, revenue_type) %>%
      dplyr::summarise(revenue_customer = sum(revenue), .groups = "drop") %>%
      tidyr::pivot_wider(names_from = revenue_type, values_from = revenue_customer)

    company_level_current <- df_current %>%
      dplyr::ungroup() %>%
      ## getting customers total revenue for the previous period
      dplyr::group_by(
        Kunde,
        orderID,
        Auftragsdatum,
        Vertriebler,
        StartDate,
        EndDate,
        Laufzeit,
        time_period,
        revenue_type
      ) %>%
      dplyr::summarise(revenue_customer = sum(revenue), .groups = "drop") %>%
      tidyr::pivot_wider(names_from = revenue_type, values_from = revenue_customer) %>%
      dplyr::ungroup()

    # types = t0: recurrent, recurrent_new, churned, t1: recurrent, recurrent_new, new
    ## joining
    aggregated_companies <- company_level_current %>%
      dplyr::full_join(
        company_level_past,
        by = c("Kunde"),
        suffix = c("_current", "_previous")
      )

    ## -----------------------tibble for the monthly revenue streams ----#

    aggregated_month <-
      revenues_sum %>%
      dplyr::select(
        -c(
          yearly_revenue_previous,
          yearly_revenue_current,
          yearlyAll_previous,
          yearlyAll_current
        )
      )
    ## ------------------- one tibble for both tibbles ------------------#
    if (focus_year) {
      if (focus_company) {
        return(aggregated_companies)
      }
      else {
        return (aggregated_kpi)
      }
    } else {
      return (aggregated_month)
    }
  }
#' compute_yearly_nrr_month
#'
#' The function is used to purrr over all contracts and get the KPI of customers, type of revenue, churn,
#' nrr and dbner per year.
#'
#' @param df contains the data frame with all contracts
#'
#' @param focus_company is set to FALSE because I want to compute the overall NRR
#'
#' @param focus_year is set to to TRUE because I want to compute the overall NRR for the year
#'
#' @param pasting is set to FALSE because I want THE number and not a sampled estimate
#'
#' @param pasting_p is the proportion of companies that should be sampled - if pasting is TRUE,
#' then this is relevant for setting the sampling proportion in the pasting
#'
#' @return this returns a tibble with the main KPIs as columns per year
#'
#' @export
compute_yearly_nrr_month <-
  function(df,
           focus_company = FALSE,
           focus_year = TRUE,
           pasting = FALSE,
           pasting_p = 0.7) {

    df <- df %>%
      # removing company types
      dplyr::mutate(Kunde = compress_column(Kunde))
    if (pasting) {
      # here I remove part of the sample in order get some
      df <- df %>%
        dplyr::mutate(
        time_period = lubridate::year(Month),
        month_in_period =  lubridate::month(Month),
        revenue = MonthlyRevenue,
        Kunde = toupper(Kunde)
      )

      all_customers <- df %>%
        tidyr::drop_na(revenue) %>%
        dplyr::pull(Kunde) %>%
        unique()
      sample_customers <-
        sample(all_customers,
               size = as.integer(length(all_customers) * pasting_p),
               replace = FALSE)
      df <- df %>%
        dplyr::filter(Kunde %in% sample_customers)

    }
    else {
      df <- df %>%
        dplyr::mutate(
        time_period = lubridate::year(Month),
        month_in_period = lubridate::month(Month),
        revenue = MonthlyRevenue,
        Kunde = toupper(Kunde)
      )
    }

    years_of_interest <- sort(unique(df$time_period))
    # instead of subtracting one year - which was possible with just -1, I had to subtract the month - for this I had to explicitly use a date notation
    purrr::map_dfr(
      years_of_interest,
      ~ yearly_nrr_for_month(df, ., . - 1, revenue, focus_company, focus_year)
    )
  }

