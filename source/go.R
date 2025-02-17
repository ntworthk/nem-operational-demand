library(httr)
library(readr)
library(dplyr)
library(glue)
library(lubridate)
library(tidyr)
library(RPushbullet)

query_function <- function(query, convert_to_chr = FALSE) {
  
  # Setup query parameters
  post_param <- list(
    query = query,
    key = Sys.getenv("NEMPOINT_KEY"),
    format = "csv"
  )
  
  # Make SQL query
  query_data <- POST(
    "http://www.nempoint.com/data/query",
    body = post_param,
    encode = "form"
  ) |> 
    content(encoding = "UTF-8", show_col_types = FALSE)
  
  # Return the data
  return(query_data)
  
}

# operational_demand_query <- read_file(file = "source/operational_demand_overall_query.sql")
# 
# query_function(operational_demand_query) |>
#   select(-RowNo) |>
#   group_by(MONTH, REGIONID) |>
#   mutate(type = ifelse(OPERATIONAL_DEMAND == min(OPERATIONAL_DEMAND), "MIN", "MAX")) |>
#   ungroup() |>
#   arrange(REGIONID, MONTH) |>
#   select(REGIONID, MONTH, INTERVAL_DATETIME, type ,OPERATIONAL_DEMAND) |>
#   pivot_wider(names_from = type, values_from = c(OPERATIONAL_DEMAND, INTERVAL_DATETIME)) |> 
#   rename(
#     TOT_MIN = OPERATIONAL_DEMAND_MIN,
#     TOT_MAX = OPERATIONAL_DEMAND_MAX,
#     INT_MIN = INTERVAL_DATETIME_MIN,
#     INT_MAX = INTERVAL_DATETIME_MAX
#   ) |> 
#   write_rds("data/max_mins.rds")

# track_max <- read_file(file = "source/track_max.sql")
# df_max <- query_function(track_max)
# track_min <- read_file(file = "source/track_min.sql")
# df_min <- query_function(track_min)
# df_max_min_over_time <- df_max |> 
#   mutate(type = "MAX") |> 
#   bind_rows(df_min |> mutate(type = "MIN"))
# df_max_min_over_time |> write_rds("data/max_mins_over_time.rds")
# df_max_min_over_time |> write_csv("data/max_mins_over_time.csv")


df_max_mins <- read_rds("data/max_mins.rds")
df_max_mins_overall <- df_max_mins |> 
  group_by(REGIONID) |> 
  summarise(
    TOT_MIN = min(TOT_MIN), TOT_MAX = max(TOT_MAX),
    INT_MIN = min(INT_MIN), INT_MAX = max(INT_MAX)
  )

operational_demand_daily <- read_file(file = "source/operational_demand_daily.sql")

date_1 <- Sys.Date() - 1
date_2 <- Sys.Date()

operational_demand_daily <- glue(operational_demand_daily)

df_day <- query_function(operational_demand_daily)

df_day <- df_day |> 
  mutate(REGIONID = "AUS1") |> 
  group_by(INTERVAL_DATETIME, REGIONID) |> 
  summarise(
    OPERATIONAL_DEMAND = sum(OPERATIONAL_DEMAND),
    LASTCHANGED = max(LASTCHANGED),
    .groups = "drop"
  ) |> 
  bind_rows(df_day)

df_check <- df_day |> 
  select(-LASTCHANGED) |>
  group_by(REGIONID, MONTH = month(INTERVAL_DATETIME)) |> 
  arrange(REGIONID, OPERATIONAL_DEMAND) |> 
  filter(row_number() == 1 | row_number() == n()) |> 
  mutate(type = ifelse(row_number() == 1, "MIN", "MAX")) |> 
  ungroup() |> 
  pivot_wider(
    names_from = type, values_from = c(OPERATIONAL_DEMAND, INTERVAL_DATETIME)
  ) |> 
  left_join(df_max_mins, by = c("REGIONID", "MONTH")) |> 
  filter(OPERATIONAL_DEMAND_MIN < TOT_MIN | OPERATIONAL_DEMAND_MAX > TOT_MAX) |> 
  mutate(
    TYPE = if_else(
      OPERATIONAL_DEMAND_MIN < TOT_MIN,
      "MIN",
      "MAX"
    )
  )

if (nrow(df_check) > 0) {
  
  overall_check <- df_check |> 
    mutate(DEMAND = ifelse(TYPE == "MIN", OPERATIONAL_DEMAND_MIN, OPERATIONAL_DEMAND_MAX)) |> 
    select(REGIONID, TYPE, DEMAND) |> 
    left_join(df_max_mins_overall, by = "REGIONID") |> 
    mutate(
      TEST = ifelse(TYPE == "MAX", DEMAND > TOT_MAX, DEMAND < TOT_MAX)
    ) |>
    filter(TEST)
  
  if (nrow(overall_check) > 0) {
    regionid <- overall_check$REGIONID[1]
    i <- min(which(df_check$REGIONID == regionid))
    type_additional <- " OVERALL"
  } else {
    i <- 1
    type_additional <- ""
  }
  
  RPushbullet::pbPost(
    type = "note",
    title = "New NEM record",
    body = paste0(df_check$TYPE[i], type_additional, " in ", df_check$REGIONID[i]),
    apikey = Sys.getenv("APIKEY"),
    devices = Sys.getenv("DEVICE")
  )
  
  df_max_mins <- df_max_mins |> 
    left_join(df_check, by = join_by(REGIONID, MONTH, TOT_MIN, TOT_MAX, INT_MIN, INT_MAX)) |> 
    replace_na(list(OPERATIONAL_DEMAND_MIN = Inf, OPERATIONAL_DEMAND_MAX = -Inf)) |> 
    mutate(
      TOT_MIN = pmin(TOT_MIN, OPERATIONAL_DEMAND_MIN),
      TOT_MAX = pmax(TOT_MAX, OPERATIONAL_DEMAND_MAX),
      INT_MIN = pmax(INT_MIN, INTERVAL_DATETIME_MIN, na.rm = TRUE),
      INT_MAX = pmax(INT_MAX, INTERVAL_DATETIME_MAX, na.rm = TRUE)
    ) |> 
    select(REGIONID, MONTH, TOT_MIN, TOT_MAX, INT_MIN, INT_MAX)
  
  write_rds(df_max_mins, "data/max_mins.rds")
  write_csv(df_max_mins, "data/max_mins.csv")
  
  df_max_min_over_time <- read_rds("data/max_mins_over_time.rds")
  
  # For new maximums
  if ("MAX" %in% df_check$TYPE) {
    max_records <- df_check |> 
      filter(TYPE == "MAX") |> 
      select(-TOT_MIN, -TOT_MAX, -INT_MIN, -INT_MAX) |> 
      left_join(df_max_mins_overall, by = "REGIONID") |> 
      filter(OPERATIONAL_DEMAND_MAX > TOT_MAX) |> 
      select(REGIONID, INTERVAL_DATETIME_MAX, OPERATIONAL_DEMAND_MAX) |> 
      rename(
        INTERVAL_DATETIME = INTERVAL_DATETIME_MAX,
        OPERATIONAL_DEMAND = OPERATIONAL_DEMAND_MAX
      ) |> 
      mutate(type = "MAX")
    
    if(nrow(max_records) > 0) {
      df_max_min_over_time <- bind_rows(df_max_min_over_time, max_records)
    }
  }
  
  # For new minimums
  if ("MIN" %in% df_check$TYPE) {
    min_records <- df_check |> 
      filter(TYPE == "MIN") |> 
      select(-TOT_MIN, -TOT_MAX, -INT_MIN, -INT_MAX) |> 
      left_join(df_max_mins_overall, by = "REGIONID") |> 
      filter(OPERATIONAL_DEMAND_MIN < TOT_MIN) |> 
      select(REGIONID, INTERVAL_DATETIME_MIN, OPERATIONAL_DEMAND_MIN) |> 
      rename(
        INTERVAL_DATETIME = INTERVAL_DATETIME_MIN,
        OPERATIONAL_DEMAND = OPERATIONAL_DEMAND_MIN
      ) |> 
      mutate(type = "MIN")
    
    if(nrow(min_records) > 0) {
      df_max_min_over_time <- bind_rows(df_max_min_over_time, min_records)
    }
  }
  
  write_rds(df_max_min_over_time, "data/max_mins_over_time.rds")
  write_csv(df_max_min_over_time, "data/max_mins_over_time.csv")
  
  

  
}


