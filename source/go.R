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
#   select(REGIONID, MONTH, type ,OPERATIONAL_DEMAND) |>
#   pivot_wider(names_from = type, values_from = OPERATIONAL_DEMAND, names_prefix = "TOT_") |>
#   write_rds("data/max_mins.rds")

df_max_mins <- read_rds("data/max_mins.rds")
df_max_mins_overall <- df_max_mins |> 
  group_by(REGIONID) |> 
  summarise(TOT_MIN = min(TOT_MIN), TOT_MAX = max(TOT_MAX))

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
    type_additional <- " OVERALL "
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
    left_join(df_check, by = join_by(REGIONID, MONTH, TOT_MIN, TOT_MAX)) |> 
    replace_na(list(OPERATIONAL_DEMAND_MIN = Inf, OPERATIONAL_DEMAND_MAX = -Inf)) |> 
    mutate(
      TOT_MIN = pmin(TOT_MIN, OPERATIONAL_DEMAND_MIN),
      TOT_MAX = pmax(TOT_MAX, OPERATIONAL_DEMAND_MAX)
    ) |> 
    select(REGIONID, MONTH, TOT_MIN, TOT_MAX)
  
  write_rds(df_max_mins, "data/max_mins.rds")
  
}


