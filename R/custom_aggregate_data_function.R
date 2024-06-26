
#' custom_aggregate_data
#'
#' This function aggregates monthly data across a data.frame by applying an aggregate_function and respecting Categories.
#' It can be either used to aggregate data for a bar chart or display as an aggregated table.

#' @param df the data.frame to aggregate over
#' @param Monat choose Date Column to calculate Monat upon; "Auftragsmonat" for order month or "Month" for booking month of earnings
#' @param Category_Column choose column with factors to aggregate by; Default is "Product_short" to aggregate by Studyflixs' products
#' @param Start_Date Start date to start aggregating data in data.frame
#' @param End_Date End date to end aggregating data in data.frame
#' @param turn_2_wide boolean wether to plot data or show as table; TRUE: pivots table, use to show summary table; FALSE: default, use to generate plot with aggregated data
#' @param aggregate_function function to aggregate data by; Default is "sum"; the other possible aggregate_function is "count"
#' @param dependenet_variable column in df to apply aggregate_function to; Default is "MonthlyRevenue"
#' @returns the aggregated data.frame indexed by months; if turn_2_wide = TRUE then Categories in columns and column "Summe" is added to the right of the aggregated data.frame

#' @export
custom_aggregate_data <- function(df,
                                  Monat,
                                  Category_Column = "Produkt_short",
                                  Start_Date,
                                  End_Date,
                                  turn2wide = FALSE,
                                  aggregate_function ="sum",
                                  dependent_variable = "MonthlyRevenue") {

  if (aggregate_function == "sum") {
    df <- df %>%
      # group the data by month and categories, use floor_date to always use first of month for further processing
      group_by(Monat = floor_date(get(Monat), unit = "months"),
               Categories = get(Category_Column)) %>%
      # aggregate by sum
      summarise(agg_sum = sum(get(dependent_variable))) %>%
      # filter for relevant months between Start_Date and End_Date
      filter(Monat >= Start_Date,
             Monat <= End_Date) %>%
      # use German date format
      mutate(Monat = format(ymd(Monat), "%d.%m.%Y"))

    if (turn2wide) {
      df %>%
        # pivot aggregated data.frame/table, rows indexed by month, Categories in Columns
        pivot_wider(names_from = Categories, values_from = agg_sum) %>%
        rowwise() %>%
        # add column "Summe" to table
        mutate(Summe = sum(c_across(1:length(unique(df$Categories))), na.rm = TRUE))
    }
    # return data.frame without pivoting
    else {
        return(df)
    }
  }
  else if (aggregate_function  == "count"){
    df <- df %>%
      # group the data by month and categories, use floor_date to always use first of month for further processing
      group_by(Monat = floor_date(get(Monat), unit = "months"),
               Categories = get(Category_Column)) %>%
      # aggregate by count
      summarise(Count = n_distinct(get(dependent_variable))) %>%
      # filter for relevant months between Start_Date and End_Date
      filter(Monat >= Start_Date,
             Monat <= End_Date) %>%
      # use German date format
      mutate(Monat = format(ymd(Monat), "%d.%m.%Y"))

    if (turn2wide) {
      df %>%
        # pivot aggregated data.frame/table, rows indexed by month, Categories in Columns
        pivot_wider(names_from = Categories, values_from = Count) %>%
        rowwise() %>%
        # add column "Summe" to table
        mutate(Summe = sum(c_across(1:length(unique(df$Produkt))), na.rm = TRUE))
    }
    # return data.frame without pivoting
    else {
      return(df)
    }
  }
  # error handling: wrong aggregate_function
  else {
    stop("Not a aggregate_function, select sum or count")
  }
}
