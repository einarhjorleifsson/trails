# Preamble ---------------------------------------------------------------------
# run this as:
#  nohup R < R/02-1_logbooks-merge.R --vanilla > logs/02-1_logbooks-merge_2023-09-04.log &
lubridate::now()

# Input:  Oracle database
# Output: data/logbooks/logbooks_2009p.parguet
#         data/logbooks/catch_2009p.parquet
# Downstream usage: R/02-2_logbooks-gear-correction.R

## Brief summary ---------------------------------------------------------------
# The main output file is just a flat file containing station information as 
#   well as basic effort information. The latter are for some historical reasons
#   stored in tables for different "gear types".
# In addition, the catch by species is also dumped
#
# Processing data loss are related to orphan effort files
#
## TODO ------------------------------------------------------------------------
# ...

# 
YEARS <- 2022:2009

library(data.table)
library(tidyverse)
library(lubridate)
library(omar)
con <- connect_mar()

GEARS <- 
  omar::gid_orri_plus(con) |> 
  collect(n = Inf) |> 
  rename(gclass = gid2,
         m4 = dcf4,
         m5 = dcf5b)
GEARS_trim <-
  GEARS |> 
  select(gid, veidarfaeri, gclass, m4, m5)
# Only Icelandic vessels
q_vessels_icelandic <- 
  omar::vessels_vessels(con) |> 
  filter(!vid %in% c(0, 1, 3:5, 9999)) %>% 
  filter(!between(vid, 3700, 4999)) |> 
  select(vid)

# 1 Old logbooks ---------------------------------------------------------------

## Mobile gear ----------------------------------------------------------------
MOBILE_old <-
  omar::lb_mobile(con, correct_gear = FALSE, trim = TRUE) |> 
  filter(year %in% YEARS,
         # only towing gear
         gid %in% c(5, 6, 7, 8, 9, 14, 15, 38, 40)) |> 
  # limit to Icelandic vesssels
  inner_join(q_vessels_icelandic %>% select(vid),
             by = join_by(vid)) |> 
  select(visir, vid, gid, date, t1, t2, lon, lat, lon2, lat2, z1, z2, datel, effort, effort_unit,
         sweeps, plow_width) |> 
  collect(n = Inf) |> 
  mutate(table = "mobile",
         date = as_date(date),
         datel = as_date(datel))

## Static gear -----------------------------------------------------------------
STATIC_old <-
  omar::lb_static(con, correct_gear = FALSE, trim = TRUE) |> 
  filter(year %in% YEARS,
         gid %in% c(1, 2, 3)) |> 
  # limit to Icelandic vesssels
  inner_join(q_vessels_icelandic %>% select(vid),
             by = join_by(vid)) |> 
  select(visir, vid, gid, date, t0, t1, t2, lon, lat, lon2, lat2, z1, z2, datel, effort, effort_unit) |> 
  collect(n = Inf) |> 
  mutate(table = "static",
         date = as_date(date),
         datel = as_date(datel))

## Traps -----------------------------------------------------------------------
TRAP_old <- 
  omar::lb_trap(con, correct_gear = FALSE, trim = TRUE) |> 
  filter(year %in% YEARS,
         # only towing gear
         gid %in% c(18, 39))  |> 
  # limit to Icelandic vesssels
  inner_join(q_vessels_icelandic %>% select(vid),
             by = join_by(vid)) |> 
  select(visir, vid, gid, date, lon, lat, lon2, lat2, datel, effort, effort_unit) |> 
  collect(n = Inf) |> 
  mutate(table = "trap",
         date = as_date(date),
         datel = as_date(datel))

## Pelagic seine ---------------------------------------------------------------
SEINE_old <-
  omar::lb_seine(con, correct_gear = FALSE, trim = TRUE) |> 
  filter(year %in% YEARS,
         # only towing gear
         gid %in% c(10, 12))  |> 
  # limit to Icelandic vesssels
  inner_join(q_vessels_icelandic %>% select(vid),
             by = join_by(vid)) |> 
  select(visir, vid, gid, date, t1, lon, lat, datel, effort, effort_unit) |> 
  collect(n = Inf) |> 
  mutate(table = "seine",
         date = as_date(date),
         datel = as_date(datel))

## Combine the logbooks --------------------------------------------------------
LGS_old <-
  bind_rows(MOBILE_old,
            STATIC_old,
            SEINE_old,
            TRAP_old) |> 
  mutate(base = "old") |> 
  rename(.sid = visir)
LGS_old <- 
  LGS_old |> 
  select(.sid, vid, gid, date, t1, t2, lon, lat, lon2, lat2, z1, z2,
         datel, effort, effort_unit, sweeps, plow_width, base, t0)

## The catch -------------------------------------------------------------------
CATCH_old <-
  lb_catch(con) |> 
  collect(n = Inf) |> 
  rename(.sid = visir) |> 
  filter(.sid %in% LGS_old$.sid)

## Checks ----------------------------------------------------------------------
### Data loss ------------------------------------------------------------------
n0 <- 
  lb_base(con) |> 
  filter(year %in% YEARS) |> 
  count() |> 
  collect() |> 
  pull(n)
n1 <- nrow(LGS_old)
print(paste0("Original settings: ", n0, " Settings retained: ", n1))
print(paste0("Records lossed: ", n0-n1, " Proportion retained: ", n1/n0))

### Missingness ----------------------------------------------------------------
LGS_old |> count(is.na(effort))


# 2 New logbooks ---------------------------------------------------------------
# The new logbooks are in principle a total mess that need to be fixed upstream
#  Following is thus just an interrim hack. The function call to the new data
#  are a little different since it is still in development.

## Functions -------------------------------------------------------------------
# should possible move functions to the omar-package
lb_trip_new <- function(con) {
  tbl_mar(con, "adb.trip_v") |> 
    select(trip_id, 
           vid = vessel_no,
           T1 = departure,
           hid1 = departure_port_no,
           T2 = landing,
           hid2 = landing_port_no,
           source)
}
lb_station_new0 <- function(con) {
  tbl_mar(con, "adb.station_v") |> 
    select(trip_id,
           station_id,
           gid = gear_no,
           t1 = fishing_start,
           t2 = fishing_end,
           lon = longitude,
           lat = latitude,
           lon2 = longitude_end,
           lat2 = latitude_end,
           z1 = depth,
           z2 = depth_end,
           tow_start,
           everything())
}
lb_base_new <- function(con) {
  lb_trip_new(con) |> 
    inner_join(lb_station_new0(con) |> 
                 select(trip_id:tow_start),
               by = "trip_id") |> 
    select(vid, gid, t1:tow_start, everything()) |> 
    mutate(whack = case_when(between(lon, 10, 30) & between(lat, 62.5, 67.6) ~ "mirror",
                             between(lon, -3, 3) & gid != 7 ~ "ghost",
                             .default = "ok"),
           lon = ifelse(whack == "mirror",
                        -lon,
                        lon),
           lon2 = ifelse(whack == "mirror",
                         -lon,
                         lon))
}
lb_catch_new <- function(con) {
  tbl_mar(con, "adb.catch") |> 
    mutate(catch = case_when(condition == "GUTT" ~ quantity / 0.8,
                             condition == "UNGU" ~ quantity,
                             .default = NA)) |> 
    select(station_id = fishing_station_id,
           sid = species_no, 
           catch, 
           weight, 
           quantity, 
           condition, 
           catch_type = source_type)
}


## Only records not in old logbooks --------------------------------------------
BASE_new <- 
  lb_base_new(con) |> 
  filter(year(t1) %in% 2021:2022) |> 
  collect(n = Inf) |> 
  select(vid:z2, trip_id, datel = T2, source:whack) |> 
  mutate(date = as_date(t1),
         datel = as_date(datel),
         base = "new")
BASE_new_n0 <- nrow(BASE_new)
# only data where the date fishing and vessels are not already in the old
#  logbooks. This reduces the number of records from ~212 thousand to
#  ~88 thousand
BASE_new <-
  BASE_new |> 
  left_join(LGS_old |> 
              select(vid, date) |> 
              distinct() |> 
              mutate(in.old = "yes"),
            #multiple = "all",
            by = join_by(vid, date)) |> 
  mutate(in.old = replace_na(in.old, "no"))
BASE_new |> 
  count(source, in.old) |> 
  spread(in.old, n) |> 
  knitr::kable(caption = "Number of records in new database that are also in the old database.")
BASE_new <- 
  BASE_new |> 
  filter(in.old == "no") |> 
  select(-in.old)

## Checks ----------------------------------------------------------------------
### Should one remove whacks?? - not if using positions from ais ---------------
#   mirror: record where lon is positive but should be negative
#   ghost: records around the meridian
BASE_new |> 
  count(source, whack) |> 
  spread(whack, n)
### Any abberrant trend in the number of sets by month? ------------------------
bind_rows(
  LGS_old |>   select(gid, date, base),
  BASE_new  |> select(gid, date, base)) |> 
  left_join(GEARS_trim |> mutate(gclass = paste(str_pad(gid, 2, pad = "0"), veidarfaeri)) |> select(gid, gclass)) |> 
  mutate(date = floor_date(date, "month")) |> 
  count(date, gclass) |> 
  filter(year(date) %in% 2018:2022) |> 
  ggplot(aes(date, n)) +
  geom_point(size = 0.5) +
  facet_wrap(~ gclass, scales = "free_y")

## Mobile gear -----------------------------------------------------------------
MOBILE_new <- 
  BASE_new |> 
  inner_join(
    tbl_mar(con, "adb.trawl_and_seine_net_v") |> collect(n = Inf),
    by = join_by(station_id)
  ) |> 
  mutate(effort = case_when(gid %in% c(6, 7) ~ as.numeric(difftime(t2, t1, units = "hours")),
                            gid %in% 5 ~ 1,
                            .default = NA),
         effort_unit = case_when(gid %in% c(6, 7) ~ "hours towed",
                                 gid %in% 5  ~  "setting",
                                 .default = NA)) |> 
  rename(sweeps = bridle_length) |> 
  select(station_id, sweeps, effort, effort_unit)
## Static gear -----------------------------------------------------------------
STATIC_new <- 
  BASE_new |> 
  inner_join(
    tbl_mar(con, "adb.line_and_net_v") |> collect(n = Inf),
    by = join_by(station_id)
  ) |> 
  mutate(dt = as.numeric(difftime(t2, t1, unit = "hours")),
         effort = case_when(gid == 3 ~ dt,
                            gid %in% c(2, 11, 25, 29, 91, 92) ~ dt/24 * nets,
                            gid %in% 1 ~ hooks,
                            .default = NA),
         effort_unit = case_when(gid == 3 ~ "hookhours",
                                 gid %in% c(2, 11, 25, 29, 91, 92) ~ "netnights",
                                 gid %in% 1 ~ "hooks",
                                 .default = NA)) |> 
  select(station_id, effort, effort_unit)
## Dredge gear -----------------------------------------------------------------
DREDGE_new <- 
  BASE_new |> 
  inner_join(
    tbl_mar(con, "adb.dredge_v") |> collect(n = Inf),
    by = join_by(station_id)
  ) |> 
  mutate(effort = as.numeric(difftime(t2, t1, units = "hours")),
         effort_unit = "hours towed",
         plow_width = 2) |> 
  select(station_id, plow_width, effort, effort_unit)
## Trap gear -------------------------------------------------------------------
TRAP_new <- 
  BASE_new |> 
  inner_join(
    tbl_mar(con, "adb.trap_v") |> collect(n = Inf),
    by = join_by(station_id)
  ) |> 
  mutate(dt = as.numeric(difftime(t2, t1, units = "hours")),
         effort = dt * number_of_traps,
         effort_unit = "trap hours") |> 
  select(station_id, effort, effort_unit)
## Seine gear ------------------------------------------------------------------
SEINE_new <- 
  BASE_new |> 
  inner_join(
    tbl_mar(con, "adb.surrounding_net_v") |> collect(n = Inf),
    by = join_by(station_id)
  ) |> 
  mutate(effort = 1,
         effort_unit = "settings") |> 
  select(station_id, effort, effort_unit)

BASE_new_aux <- 
  bind_rows(MOBILE_new,
            STATIC_new,
            DREDGE_new,
            TRAP_new,
            SEINE_new)

## Check -----------------------------------------------------------------------
### Orphan effort files --------------------------------------------------------
n1 <- nrow(BASE_new)
n2 <- nrow(BASE_new_aux)
print(paste0("Records in base: ", n1, " Records in auxillary: ", n2))
BASE_new |> 
  mutate(orphan = ifelse(station_id %in% BASE_new_aux$station_id, "no", "yes")) |> 
  count(source, orphan) |> 
  spread(orphan, n) |> 
  knitr::kable(caption = "Source of effort orphan files")
BASE_new |> 
  mutate(orphan = ifelse(station_id %in% BASE_new_aux$station_id, "no", "yes")) |> 
  filter(orphan == "yes") |> 
  count(source, gid) |> 
  spread(gid, n) |> 
  knitr::kable(caption = "Gear list of effort orphan files")

## Combine the (new) logbooks --------------------------------------------------
LGS_new <- 
  BASE_new |> 
  left_join(BASE_new_aux,
            by = join_by(station_id)) |> 
  select(.sid = station_id, vid, gid, date, t1, t2, lon, lat, lon2, lat2, z1, z2,
         datel, effort, effort_unit, sweeps, plow_width, base)

## Catch -----------------------------------------------------------------------
CATCH_new <- 
  lb_catch_new(con) |> 
  collect(n = Inf) |> 
  filter(station_id %in% BASE_new$station_id) |> 
  select(.sid = station_id, sid, catch)

# Merge the old and the new logbooks -------------------------------------------
LGS <- 
  bind_rows(LGS_old, 
            LGS_new) 
CATCH <- 
  bind_rows(CATCH_old,
            CATCH_new)

# Add landing id and gid from landings data ------------------------------------
## Landings data ---------------------------------------------------------------
LN_raw <- 
  omar::ln_agf(con) |> 
  filter(wt > 0,
         year(date) %in% YEARS) |> 
  rename(.lid = .id,
         hid_ln = hid,
         gid_ln = gid,
         datel = date) |> 
  collect(n = Inf) |> 
  filter(vid %in% c(2, 5:3699, 5000:9998)) |> 
  mutate(datel = as_date(datel),
         vid = as.integer(vid),
         gid_ln = as.integer(gid_ln),
         hid_ln = as.integer(hid_ln)) |> 
  arrange(vid, datel, .lid, gid_ln, sid) |>
  group_by(vid, datel) |> 
  mutate(.lid_min = min(.lid)) |> 
  ungroup()

## Checks ----------------------------------------------------------------------
checks <- 
  LN_raw |> 
  group_by(vid, datel) |> 
  summarise(n_harbours = n_distinct(hid_ln),
            n_gears = n_distinct(gid_ln),
            n_lid = n_distinct(.lid),
            .groups = "drop")
#### Same landings date, different harbours ------------------------------------
checks |> count(n_harbours) |> mutate(p = round(n / sum(n), 3))
#### Same landings date, different gear ----------------------------------------
checks |> count(n_gears) |> mutate(p = round(n / sum(n), 3))
#### Same landings date, different landings id ---------------------------------
checks |> count(n_lid) |> mutate(p = round(n / sum(n), 3))

## Landings and gear id only ---------------------------------------------------
LN <-  
  LN_raw |> 
  select(vid, datel, gid_ln, .lid_min) |> 
  # here only keep one gid record and the lowest .lid wihtin a landings date
  distinct(vid, datel, .lid_min, .keep_all = TRUE)
### Nearest date match ---------------------------------------------------------
#### Function ------------------------------------------------------------------
match_nearest_date <- function(lb, ln) {
  
  lb.dt <-
    lb %>%
    select(vid, datel) %>%
    distinct() %>%
    setDT()
  
  ln.dt <-
    ln %>%
    select(vid, datel) %>%
    distinct() %>%
    mutate(dummy = datel) %>%
    setDT()
  
  res <-
    lb.dt[, date.ln := ln.dt[lb.dt, dummy, on = c("vid", "datel"), roll = "nearest"]] %>%
    as_tibble()
  
  lb %>%
    left_join(res,
              by = c("vid", "datel")) %>%
    left_join(ln %>% select(vid, date.ln = datel, gid_ln, .lid_min),
              by = c("vid", "date.ln"))
  
}

LGS <-
  LGS |> 
  match_nearest_date(LN) |> 
  rename(date_ln = date.ln)

# Save -------------------------------------------------------------------------
LGS   |> write_rds("data/logbooks/station.rds")
CATCH |> write_rds("data/logbooks/catch.rds")
library(arrow)
LGS   |> arrow::write_parquet("data/logbooks/station.parquet")
CATCH |> arrow::write_parquet("data/logbooks/catch.parquet")

# Info -------------------------------------------------------------------------
devtools::session_info()
