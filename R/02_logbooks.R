# How to run things ------------------------------------------------------------
# run this as:
#  nohup R < R/02_logbooks.R --vanilla > logs/02_logbooks_2023-09-01.log &

# Taken from the ices datacall directory
#  Here we need to fix the visir stuff (before change the visir is overwritten
#   with new numbers)
#   And possibly rerun the trail stuff

lubridate::now()



# A brief outline --------------------------------------------------------------
# A single logbook munging to then be used downstream for stk match
# This script is based on merging 2020 ices datacall internal script and that
# used in the 2019 anr-request. Added then gear corrections and other things.
#
# The output has the same number of records as the input. In the downstream
# process the data used should be those where variable **use** is TRUE.
#
# TODO:
#      Should check the lb_functions in the mar-package
#      Check vids in landings - could be used to filter out wrong vids in
#       logbooks
#      Higher resolution of dredge than just gid == 15, domestic purpose only
#      Higher resolution of nets than just  gid == 2, domestic purpose only
#      Should ICES metier be set here or further downstream?
#
# PROCESSING STEPS:
#  1. Get and merge logbook and landings data
#  2. Gear corrections
#  3. Lump some gears
#  4. Cap effort and end of action
#  5. Mesh size "corrections"
#  6. Set gear width proxy
#  7. gear class of corrected gid
#  8. Match vid with mobileid in stk
#  9. Add vessel information - only needed for ICES datacall
# 10. Add metier - only needed for ICES datacall
# 11. ICES rectangles - only needed for ICES datacall
# 12. Anonymize vid - only needed for ICES datacall



YEARS <- 2022:2009

library(data.table)
library(tidyverse)
library(lubridate)
library(omar)
con <- connect_mar()

# 0. Functions -----------------------------------------------------------------
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
    left_join(ln %>% select(vid, date.ln = datel, gid.ln),
              by = c("vid", "date.ln"))
  
}
# end: 0. Functions

# 0. ICES lingo ----------------------------------------------------------------
metier4 <-  
  icesVocab::getCodeList("GearTypeL4") |> 
  as_tibble() |> 
  janitor::clean_names() |> 
  select(key, description)
metier5 <-  
  icesVocab::getCodeList("TargetAssemblage") |> 
  as_tibble() |> 
  janitor::clean_names() |> 
  select(key, description)
metier6 <-  
  icesVocab::getCodeList("Metier6_FishingActivity") |> 
  as_tibble() |> 
  janitor::clean_names() |> 
  select(key, description)

# 0. GEARS ---------------------------------------------------------------------
GEARS <- 
  omar::gid_orri_plus(con) |> 
  collect(n = Inf) |> 
  rename(gclass = gid2,
         m4 = dcf4,
         m5 = dcf5b)
GEARS_trim <-
  GEARS |> 
  select(gid, veidarfaeri, gclass, m4, m5)


# 1. Get and merge logbook and landings data -----------------------------------
vessels <- 
  omar::vessels_vessels(con) |> 
  filter(!vid %in% c(0, 1, 3:5, 9999)) %>% 
  filter(!between(vid, 3700, 4999))

## 1.1. Old logbooks -----------------------------------------------------------
CATCH_old <-
  omar::lb_base(con) %>%
  filter(year %in% YEARS) %>% 
  # limit to Icelandic vesssels
  inner_join(vessels %>% select(vid),
             by = "vid") |> 
  select(visir) %>%
  left_join(omar::lb_catch(con) %>%
              mutate(catch = catch / 1e3),    # catch in tonnes
            by = "visir") %>%
  collect(n = Inf) %>% 
  arrange(visir, desc(catch), sid) %>%
  # If catch is NA, assume it is zero
  mutate(catch = replace_na(catch, 0)) %>% 
  group_by(visir) %>%
  mutate(total = sum(catch),
         p = catch / total,
         n.sid = n()) %>%
  ungroup() |> 
  group_by(visir) %>%
  slice(1) %>%
  ungroup() %>% 
  rename(sid.target = sid)
# sanity: should have only one record per visir id
CATCH_old |> 
  count(visir) |> 
  filter(n > 1)
### Mobile gear ----------------------------------------------------------------
MOBILE_old <-
  omar::lb_mobile(con, correct_gear = FALSE, trim = TRUE) |> 
  filter(year %in% YEARS,
         # only towing gear
         gid %in% c(5, 6, 7, 8, 9, 14, 15, 38, 40)) |> 
  # limit to Icelandic vesssels
  inner_join(vessels %>% select(vid),
             by = "vid") |> 
  select(visir, vid, gid, date, t1, t2, lon, lat, datel, effort, effort_unit,
         sweeps, plow_width) |> 
  collect(n = Inf) |> 
  mutate(source = "mobile",
         date = as_date(date),
         datel = as_date(datel))

### Static gear ----------------------------------------------------------------
STATIC_old <-
  omar::lb_static(con, correct_gear = FALSE, trim = TRUE) |> 
  filter(year %in% YEARS,
         gid %in% c(1, 2, 3)) |> 
  # limit to Icelandic vesssels
  inner_join(vessels %>% select(vid),
             by = "vid") |> 
  select(visir, vid, gid, date, t0, t1, t2, lon, lat, datel, effort, effort_unit) |> 
  collect(n = Inf) |> 
  mutate(source = "static",
         date = as_date(date),
         datel = as_date(datel))

### Traps ----------------------------------------------------------------------
TRAP_old <- 
  omar::lb_trap(con, correct_gear = FALSE, trim = TRUE) |> 
  filter(year %in% YEARS,
         # only towing gear
         gid %in% c(18, 39))  |> 
  # limit to Icelandic vesssels
  inner_join(vessels %>% select(vid),
             by = "vid") |> 
  select(visir, vid, gid, date, lon, lat, datel, effort, effort_unit) |> 
  collect(n = Inf) |> 
  mutate(source = "trap",
         date = as_date(date),
         datel = as_date(datel))

### Pelagic seine --------------------------------------------------------------
SEINE_old <-
  omar::lb_seine(con, correct_gear = FALSE, trim = TRUE) |> 
  filter(year %in% YEARS,
         # only towing gear
         gid %in% c(10, 12))  |> 
  # limit to Icelandic vesssels
  inner_join(vessels %>% select(vid),
             by = "vid") |> 
  select(visir, vid, gid, date, t1, lon, lat, datel, effort, effort_unit) |> 
  collect(n = Inf) |> 
  mutate(source = "seine",
         date = as_date(date),
         datel = as_date(datel))
### Combine the logbooks -------------------------------------------------------
LGS_old <-
  bind_rows(MOBILE_old,
            STATIC_old,
            SEINE_old,
            TRAP_old) |> 
  left_join(CATCH_old) |> 
  mutate(base = "old")
LGS_old |> 
  count(source, gid) |> 
  left_join(GEARS |> select(gid, veidarfaeri)) |> 
  knitr::kable(caption = "All gears: Number of records by gear")


## 1.2 New logbooks ------------------------------------------------------------
# 2023-05-25: The new logbooks are in principle a total mess
#             Here just some quick has is done for the ICES datacall
# [cut]
## 2023-05-29 Ignore above -----------------------------------------------------
# the function flow results in slow excecution
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

n_base <- 
  lb_base_new(con) |> 
  filter(year(t1) %in% 2021:2022) |> 
  collect(n = Inf) |> 
  select(vid:lat, trip_id, datel = T2, source:whack) |> 
  mutate(date = as_date(t1),
         datel = as_date(datel))


# only data where the date fishing and vessels are not already in the old
#  logbooks. This reduces the number of records from ~212 thousand to
#  ~88 thousand
n_base <-
  n_base |> 
  left_join(LGS_old |> 
              select(vid, date) |> 
              distinct() |> 
              mutate(in.old = TRUE),
            multiple = "all") |> 
  filter(is.na(in.old)) |> 
  select(-in.old)
# need to remove ghost-whacks
n_base |> 
  count(whack)
n_base <-
  n_base |> 
  filter(whack %in% c("ok", "mirror")) |> 
  select(-whack)
# what are the sources
n_base |> count(source)

# sanity check, see if any abberrant trend in the number of sets by month
p <-
  bind_rows(
    LGS_old |> select(gid, date) |> mutate(what = "old"),
    n_base  |> select(gid, date) |> mutate(what = "new")) |> 
  left_join(GEARS_trim |> select(gid, gclass)) |> 
  mutate(date = floor_date(date, "month")) |> 
  count(date, gclass) |> 
  filter(year(date) %in% 2018:2022) |> 
  ggplot(aes(date, n)) +
  geom_point() +
  facet_wrap(~ gclass, scales = "free_y")
p


# 2023-05-29: There are lot of orphan sub-information, becomes apparent if on
#             uses inner_join below
n_mobile <- 
  n_base |> 
  inner_join(
    tbl_mar(con, "adb.trawl_and_seine_net_v") |> collect(n = Inf)
  ) |> 
  mutate(effort = case_when(gid %in% c(6, 7) ~ as.numeric(difftime(t2, t1, units = "hours")),
                            gid %in% 5 ~ 1,
                            .default = NA),
         effort_unit = case_when(gid %in% c(6, 7) ~ "hours towed",
                                 gid %in% 5  ~  "setting",
                                 .default = NA)) |> 
  rename(sweeps = bridle_length) |> 
  select(station_id, sweeps, effort, effort_unit)
n_static <- 
  n_base |> 
  inner_join(
    tbl_mar(con, "adb.line_and_net_v") |> collect(n = Inf)
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

n_dredge <- 
  n_base |> 
  inner_join(
    tbl_mar(con, "adb.dredge_v") |> collect(n = Inf)
  ) |> 
  mutate(effort = as.numeric(difftime(t2, t1, units = "hours")),
         effort_unit = "hours towed",
         plow_width = 2) |> 
  select(station_id, plow_width, effort, effort_unit)
n_trap <- 
  n_base |> 
  inner_join(
    tbl_mar(con, "adb.trap_v") |> collect(n = Inf)
  ) |> 
  mutate(dt = as.numeric(difftime(t2, t1, units = "hours")),
         effort = dt * number_of_traps,
         effort_unit = "trap hours") |> 
  select(station_id, effort, effort_unit)

n_seine <- 
  n_base |> 
  inner_join(
    tbl_mar(con, "adb.surrounding_net_v") |> collect(n = Inf)
  ) |> 
  mutate(effort = 1,
         effort_unit = "settings") |> 
  select(station_id, effort, effort_unit)

# sanity check - am i missing settings when using inner_join above
n_base_aux <- 
  bind_rows(n_mobile,
            n_static,
            n_dredge,
            n_trap,
            n_seine)
nrow(n_base)
nrow(n_base_aux)

n_base <-
  n_base |> 
  left_join(n_base_aux |>
              mutate(orphan = "no")) |> 
  mutate(orphan = replace_na(orphan, "yes"))
n_base |> 
  count(gid, orphan) |> 
  spread(orphan, n)

# repeat sanity check, see if any abberrant trend in the number of sets by month
bind_rows(
  LGS_old |> select(gid, date) |> mutate(what = "old"),
  n_base  |> 
    select(gid, date) |> mutate(what = "new")) |> 
  left_join(GEARS_trim |> select(gid, gclass)) |> 
  mutate(date = floor_date(date, "month")) |> 
  count(date, gclass) |> 
  filter(year(date) %in% 2018:2022) |> 
  ggplot(aes(date, n)) +
  geom_point() +
  facet_wrap(~ gclass, scales = "free_y")


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


lb_catch_new(con) |> count(catch_type)

n_catch <- 
  n_base |> 
  select(station_id) |> 
  left_join(lb_catch_new(con) |> 
              filter(catch_type == "CATC") |> 
              select(station_id, sid, catch) |> 
              collect(n = Inf) |> 
              arrange(station_id, desc(catch), sid) %>%
              group_by(station_id) %>%
              mutate(total = sum(catch),
                     p = catch / total,
                     total = total / 1e3,
                     n.sid = n()) %>%
              ungroup() |>
              group_by(station_id) %>%
              slice(1) %>%
              ungroup()) |> 
  select(-catch) |> 
  mutate(total = replace_na(total, 0))


LGS_new <-
  n_base |> 
  left_join(n_catch)

LGS <- 
  bind_rows(LGS_old |> mutate(base = "old", visir_org = visir),
            LGS_new |> mutate(base = "new")) |> 
  # NEW VISIR
  mutate(visir = 1:n())

# testing if vid-date by source are unique
LGS |> 
  select(vid, date, base) |> 
  distinct() |> 
  count(vid, date, base) |> 
  filter(n > 1)
LGS |> 
  count(vid, date, base) |> 
  ggplot(aes(date, n, fill = base)) +
  geom_col() +
  scale_fill_brewer(palette = "Set1") +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y")
LGS |> 
  # It is obvious that jiggers are largely missing in 2021
  # filter(gid == 3) |> 
  mutate(date = floor_date(date, unit = "month")) |> 
  count(vid, date, base, gid) |> 
  ggplot(aes(date, n, fill = base)) +
  geom_col() +
  scale_fill_brewer(palette = "Set1") +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y") +
  facet_wrap(~ gid, scales = "free_y")

# nrows to double-check if and where we "loose" or for that matter accidentally
#  "add" data (may happen in joins) downstream
n0 <- nrow(LGS)
paste("Original number of records:", n0)

# get the gear from landings
tmp.ln.base <-
  omar::ln_catch(con) %>%
  filter(!is.na(vid), !is.na(date)) %>%
  mutate(year = year(date)) %>%
  filter(year %in% YEARS) %>%
  select(vid, gid.ln = gid, datel = date) %>%
  distinct() %>%
  collect(n = Inf) %>%
  mutate(datel = as_date(datel),
         gid.ln = as.integer(gid.ln))



tmp.lb.ln.match <-
  LGS %>% 
  match_nearest_date(tmp.ln.base) %>% 
  select(visir, gid.ln) %>% 
  # just take the first match, i.e. ignore second landing within a day
  distinct(visir, .keep_all = TRUE)
LGS <- 
  LGS %>% 
  left_join(tmp.lb.ln.match,
            by = "visir")
# Vessels per year - few vessels in 2021 is because of records for some
#  small vessels have not been entered
LGS %>% 
  mutate(year = year(date)) |> 
  group_by(year, gid) %>% 
  summarise(n.vids = n_distinct(vid)) %>% 
  spread(gid, n.vids) |> 
  knitr::kable()
paste("Number of records:", nrow(LGS))
LGS %>% write_rds("LGS_raw.rds")
# rm(tmp.lb.base, tmp.lb.catch, tmp.lb.mobile, tmp.lb.static, tmp.ln.base, tmp.lb.ln.match)
# end: Get and merge logbook and landings data


# NOTE: What to do if no effort registered?? -----------------------------------
LGS %>% 
  mutate(has.effort = ifelse(!is.na(effort), "yes", "no")) |> 
  count(gid, has.effort) %>%
  spread(has.effort, n, fill = 0) |> 
  knitr::kable(caption = "Missing effort (no) - correct once gid has been corrected")


# 2. Gear corrections ----------------------------------------------------------
gears <-
  #tbl_mar(con, "ops$einarhj.gear") %>% collect(n = Inf) %>%
  # 2021-08-23 changes
  tbl_mar(con, "ops$einarhj.gid_orri_plus") %>% 
  select(gid = veidarfaeri, gclass = gid2) %>% 
  collect(n = Inf) %>% 
  mutate(#description = ifelse(gid == 92, "G.halibut net", description),
    gid = as.integer(gid),
    gclass = as.integer(gclass))
LGS <- 
  LGS %>% 
  mutate(gid.lb = gid) |> 
  left_join(gears %>% select(gid.lb = gid, gc.lb = gclass), by = "gid.lb") %>% 
  left_join(gears %>% select(gid.ln = gid, gc.ln = gclass), by = "gid.ln") %>% 
  select(visir:gid.ln, gc.lb, gc.ln, everything()) %>% 
  mutate(gid = NA_integer_,
         gid.source = NA_character_) %>% 
  mutate(i = gid.lb == gid.ln,
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.lb=gid.ln", gid.source),
         step = ifelse(i, 1L, NA_integer_)) %>% 
  mutate(i = is.na(gid) & gc.lb == gc.ln,
         gid = ifelse(i, gc.lb, gid),
         gid.source = ifelse(i, "gc.lb=gc.ln   -> gid.lb", gid.source),
         step = ifelse(i, 2L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 15L & gid.lb %in% c(5L, 6L),
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=15, gid.lb=5,6   -> gid.ln", gid.source),
         step = ifelse(i, 3L, step)) %>% 
  mutate(i = is.na(gid) & 
           gid.ln == 21 & 
           gid.lb == 6 &
           !sid.target %in% c(30, 36, 41),
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "gid.ln=21, gid.lb=6, sid.target != 30,36,41   -> gid.lb",
                             gid.source),
         step = ifelse(i, 4L, step)) %>% 
  mutate(i = is.na(gid) &
           gid.ln == 21 &
           gid.lb == 6 & 
           sid.target %in% c(22, 41),
         gid = ifelse(i, 14, gid),
         gid.source = ifelse(i, "gid.ln=21, gid.lb=6, sid.target = 22,41   -> 14", gid.source),
         step = ifelse(i, 5L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 6 & gid.lb == 7 & sid.target %in% c(11, 19, 30:36),
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "gid.ln=6, gid.lb=7, sid.target = 11,19,30:36   -> gid.lb", gid.source),
         step = ifelse(i, 6L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 18 & gid.lb == 5,
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=18, gid.lb=5   -> gid.ln", gid.source),
         step = ifelse(i, 7L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 18 & gid.lb == 40,
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "gid.ln=18, gid.lb=40   -> gid.lb", gid.source),
         step = ifelse(i, 8L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 14 & gid.lb == 6 & sid.target == 41,
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=14, gid.lb=6, sid.target = 41   -> gid.ln", gid.source),
         step = ifelse(i, 9L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 7 & gid.lb == 6,
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=7, gid.lb=6   -> gid.ln", gid.source),
         step = ifelse(i, 10L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 9 & gid.lb == 6,
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=9, gid.lb=6   -> gid.ln", gid.source),
         step = ifelse(i, 11L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 18 & gid.lb == 38,
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=18, gid.lb=38   -> gid.ln", gid.source),
         step = ifelse(i, 12L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 6 & gid.lb == 14,
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "gid.ln=6, gid.lb=14   -> gid.lb", gid.source),
         step = ifelse(i, 13L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 15 & gid.lb == 39,
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "gid.ln=15, gid.lb=39   -> gid.lb", gid.source),
         step  = ifelse(i, 14L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 40 & gid.lb %in% c(5, 6),
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=40, gid.lb=5,6   -> gid.ln", gid.source),
         step = ifelse(i, 15L, step)) %>% 
  mutate(i = is.na(gid) & is.na(gid.ln) & gid.lb %in% c(1:3),
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "is.na(gid.ln), gid.lb=1:3   -> gid.lb", gid.source),
         step = ifelse(i, 16L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 21 & gid.lb == 6,
         gid = ifelse(i, 7, gid),
         gid.source = ifelse(i, "gid.ln=21, gid.lb=6   -> 7", gid.source),
         step = ifelse(i, 17L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 21 & gid.lb == 14,
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "gid.ln=21, gid.lb=14   -> gid.lb", gid.source),
         step = ifelse(i, 18L, step))


paste("Number of records:", nrow(LGS))
LGS %>% 
  count(step, gid, gid.lb, gid.ln, gid.source) %>% 
  mutate(p = round(n / sum(n) * 100, 3)) %>% 
  arrange(-n) %>% 
  knitr::kable(caption = "Overview of gear corrections")
LGS %>% 
  mutate(missing = is.na(gid)) %>% 
  count(missing) %>% 
  mutate(p = n / sum(n) * 100) %>% 
  knitr::kable(caption = "Number and percentage of missing gear")
# end: Gear corrections


# 3. Lump some gears -----------------------------------------------------------
LGS <-
  LGS %>% 
  mutate(gid = case_when(gid %in% c(10, 12) ~ 4,   # purse seines
                         gid %in% c(18, 39) ~ 18,      # traps
                         TRUE ~ gid)) %>% 
  # make the rest a negative number
  mutate(gid = ifelse(is.na(gid), -666, gid)) %>% 
  # "skip" these also in downstream processing
  mutate(gid = ifelse(gid %in% c(4, 12, 42), -666, gid)) %>% 
  # lump dredges into one single gear
  mutate(gid = ifelse(gid %in% c(15, 37, 38, 40), 15, gid))
paste("Number of records:", nrow(LGS))
# end: Lump some gears


# 4. Cap effort and end of action ----------------------------------------------
# Guestimate median effort where effort is missing, not critical
median.effort <- 
  LGS %>% 
  group_by(gid) %>% 
  summarise(median = median(effort, na.rm = TRUE)) %>% 
  drop_na()
LGS <- 
  LGS %>% 
  left_join(median.effort, by = "gid") %>% 
  mutate(effort = ifelse(!is.na(effort), effort, median)) %>% 
  select(-median) %>% 
  # cap effort hours
  mutate(effort = case_when(effort > 12 & gid ==  6 ~ 12,
                            effort > 24 & gid ==  7 ~ 24,
                            effort > 15 & gid == 14 ~ 15,
                            TRUE ~ effort)) %>% 
  # Cap on the t2 so not overlapping with next setting
  #    NOTE: Effort not adjusted accordingly
  arrange(vid, t1) %>%
  group_by(vid) %>%
  mutate(overlap = if_else(t2 > lead(t1), TRUE, FALSE, NA),
         t22 = if_else(overlap,
                       lead(t1) - minutes(1), # need to subtract 1 minute but get the format right
                       t2,
                       as.POSIXct(NA)),
         t22 = ymd_hms(format(as.POSIXct(t22, origin="1970-01-01", tz="UTC"))),
         t2 = if_else(overlap & !is.na(t22), t22, t2, as.POSIXct(NA))) %>%
  ungroup() %>% 
  select(-t22) 
paste("Number of records:", nrow(LGS))
# end: 4. Cap effort and end of action


# 5. Mesh size "corrections" ---------------------------------------------------
# 2023-05-24: This is no longer used to generate metier 6

# end:5. Mesh size "corrections"


# 6. Set gear width proxy ------------------------------------------------------
LGS <- 
  LGS %>% 
  mutate(gear.width = case_when(gid %in% c(6L, 7L, 9L, 14L) ~ as.numeric(sweeps),
                                gid %in% c(15L, 38L, 40L) ~ as.numeric(plow_width),
                                TRUE ~ NA_real_)) %>% 
  # cap gear width
  mutate(gear.width = case_when(gid ==  6L & gear.width > 250 ~ 250,
                                gid ==  7L & gear.width > 250 ~ 250,
                                gid ==  9L & gear.width >  75 ~  75,
                                gid == 14L & gear.width >  55 ~  55,
                                TRUE ~ gear.width))
gear.width <- 
  LGS %>% 
  filter(gid %in% c(6, 7, 9, 14, 15, 37, 38, 40)) %>% 
  group_by(gid) %>% 
  summarise(gear.width.median = median(gear.width, na.rm = TRUE),
            .groups = "drop")
# use median gear width by gear, if gear width is missing
#   could also try to do this by vessels. time scale (year) may also matter here
LGS <- 
  LGS %>% 
  left_join(gear.width,
            by = "gid") %>% 
  mutate(gear.width = ifelse(gid %in% c(6, 7, 9, 14, 15, 37, 38, 40) & is.na(gear.width),
                             gear.width.median,
                             gear.width)) %>% 
  select(-gear.width.median)

# Put gear width of 5 as 500 - this is taken from thin air
#   TODO: What gear width should be used
LGS <- 
  LGS %>% 
  mutate(gear.width = ifelse(gid == 5, 500, gear.width))
LGS %>% 
  group_by(gid) %>% 
  summarise(median = median(gear.width),
            mean = mean(gear.width),
            sd = sd(gear.width),
            min = min(gear.width),
            max = max(gear.width)) %>% 
  knitr::kable(caption = "Statistics on gear width - check min values")
paste("Number of records:", nrow(LGS))


# Get rid of intermediary variables
LGS <- 
  LGS %>% 
  select(-c(gid.lb, gid.ln, gc.lb, gc.ln, sid.target, catch, p, n.sid, i,
            overlap))


# 7. gear class of corrected gid -----------------------------------------------
LGS <- 
  LGS %>% 
  left_join(gears %>% select(gid, gclass),
            by = "gid")
paste("Number of records:", nrow(LGS))


# 8. Match vid with mobileid in stk --------------------------------------------

# 2023-05-19 - new approach, older approach below - set to FALSE
#              This step is no longer necessary, trail by vid is already available
#              in ~/stasi/gis/AIS_TRAIL
#              Here just some summary statistics for printout
mobile_vid <-
  tbl_mar(con, "ops$einarhj.mobile_vid") |> 
  mutate(t1 = to_date(t1, "YYYY:MM:DD"),
         t2 = to_date(t2, "YYYY:MM:DD")) |> 
  select(mid, vid, t1, t2, pings) |> 
  collect(n = Inf)
summary.lgs <-
  LGS %>% 
  group_by(vid) %>% 
  summarise(n.lgs = n(),
            n.gid = n_distinct(gid),
            min.date = min(date),
            max.date = max(date),
            .groups = "drop") |> 
  # can have multiple matches
  left_join(mobile_vid)
summary.lgs |> 
  filter(is.na(mid)) |> 
  knitr::kable(caption = "Vessels with no matching mobileid")


# 2023-05-19 - earlier years stuff
if(FALSE) {
  vid.stk <-
    # 2022-04-18 change
    # mar:::stk_mobile_icelandic(con, correct = TRUE, vidmatch = TRUE) %>% 
    # 2023-05-10 change
    tbl_mar(con, "ops$einarhj.mobile_vid") %>% 
    filter(pings > 0) |> 
    select(mid, vid, pings, t1, t2) |>  
    collect(n = Inf)
  # Create a summary overview of logbook and stk informations, not necessary
  #  for the workflow
  summary.lgs <-
    LGS %>% 
    group_by(vid) %>% 
    summarise(n.lgs = n(),
              n.gid = n_distinct(gid),
              min.date = min(date),
              max.date = max(date),
              .groups = "drop")
  MIDs <- 
    summary.lgs %>% 
    left_join(vid.stk, by = "vid") %>% 
    pull(mid) %>% 
    unique()
  # unexpected
  table(!is.na(MIDs))
  MIDs <- MIDs[!is.na(MIDs)]
  # can only have 1000 records in filter downstream
  mids1 <- MIDs[1:1000]
  mids2 <- MIDs[1001:length(MIDs)]
  summary.stk <-
    bind_rows(stk_trail(con) %>% 
                filter(mid %in% mids1,
                       time >= to_date("2009-01-01", "YYYY:MM:DD"),
                       time <  to_date("2022-12-31", "YYYY:MM:DD")) %>% 
                group_by(mid) %>% 
                summarise(n.stk = n(),
                          min.time = min(time, na.rm = TRUE),
                          max.time = max(time, na.rm = TRUE)) %>% 
                collect(n = Inf),
              stk_trail(con) %>% 
                filter(mid %in% mids2,
                       time >= to_date("2009-01-01", "YYYY:MM:DD"),
                       time <  to_date("2022-12-31", "YYYY:MM:DD")) %>% 
                group_by(mid) %>% 
                summarise(n.stk = n(),
                          min.time = min(time, na.rm = TRUE),
                          max.time = max(time, na.rm = TRUE)) %>% 
                collect(n = Inf))
  # NOTE: get here more records than in the logbooks summary because 
  #       some vessels have multiple mid, and then some, ... 
  print(c(nrow(summary.lgs), nrow(summary.stk)))
  d <- 
    summary.stk %>% 
    left_join(vid.stk %>% 
                select(mid, vid), by = "mid") %>% 
    full_join(summary.lgs, by = "vid") %>% 
    group_by(vid) %>% 
    mutate(n.mid = n()) %>% 
    ungroup() %>% 
    # 2021-08-23 changes
    left_join(omar:::vid_registry(con, standardize = TRUE) %>% 
                select(vid, vessel) %>% 
                collect(n = Inf), by = "vid") %>% 
    select(vid, mid, vessel, n.lgs, n.stk, n.mid, everything()) %>% 
    arrange(vid)
  d %>% 
    filter(is.na(mid)) %>% 
    select(vid, vessel, n.lgs, n.gid:max.date) %>% 
    knitr::kable(caption = "Vessels with no matching mobileid")
  d %>% 
    filter(!is.na(mid),
           n.lgs <= 10) %>% 
    arrange(n.lgs, -n.stk) %>% 
    knitr::kable(caption = "Vessels with low logbook records")
  # NOTE: Not sure if this is needed:
  d %>% write_rds("data/VID_MID.rds")
  vidmid_lookup <- 
    d %>% 
    select(vid, mid)
  # add mobileid as string to LGS, this could also have been archieved with nest
  # 2022-04-18: Not sure why doing this
  d2 <- 
    d %>% 
    select(vid, mid) %>% 
    arrange(vid) %>% 
    group_by(vid) %>% 
    mutate(midnr = 1:n()) %>% 
    spread(midnr, mid) %>% 
    mutate(mids = case_when(#!is.na(`3`) ~ paste(`1`, `2`, `3`, sep = "-"),
      !is.na(`2`) ~ paste(`1`, `2`, sep = "-"),
      TRUE ~ as.character(`1`))) %>% 
    select(vid, mids)
  # NOTE: Should maybe put a flag here for no mid match
  LGS <- 
    LGS %>% 
    left_join(d2)
  paste("Number of records:", nrow(LGS))
}
# end: 8. Match vid with mobileid in stk


# 9. Add vessel information ----------------------------------------------------
vessels <- 
  omar::vessels_vessels(con) |> 
  filter(!vid %in% c(0, 1, 3:5),
         !is.na(vessel)) %>% 
  arrange(vid) %>% 
  collect(n = Inf) %>% 
  filter(vid %in% unique(LGS$vid))



vessel.miss <- 
  vessels %>% 
  filter(is.na(engine_kw) | is.na(length)) %>% 
  select(vid, vessel, length, engine_kw)
vessel.miss |> 
  knitr::kable(caption = "Skip sem eru ekki skráð með kw eða lengd\nspurning hvort eigi að sleppa")
hreidar <- 
  readxl::read_excel("~/stasi/hreidar/data-raw/HREIDAR_Islensk_skip.xlsx") |> 
  janitor::clean_names() |> 
  select(year = ar, vid = skskr_nr, length = lengd_skrad,  kw) |> 
  group_by(vid) |> 
  filter(year == max(year)) |> 
  ungroup() |> 
  filter(vid %in% vessel.miss$vid)
vessels <- 
  vessels |> 
  left_join(hreidar |> select(vid, kw)) |> 
  mutate(engine_kw = ifelse(is.na(engine_kw) & !is.na(kw) & kw > 0,
                            kw,
                            engine_kw)) |> 
  select(-kw) |> 
  left_join(hreidar |> select(vid, length_hreidar = length)) |> 
  mutate(length = ifelse(is.na(length) & !is.na(length_hreidar) & length_hreidar > 0,
                         length_hreidar,
                         length)) |> 
  select(-length_hreidar)
vessel.miss <- 
  vessels %>% 
  filter(is.na(engine_kw) | is.na(length)) %>% 
  select(vid, vessel, length, engine_kw)
vessel.miss |> 
  knitr::kable(caption = "Skip sem eru ekki skráð með kw eða lengd\nspurning hvort eigi að sleppa")
vessels <- 
  vessels |> 
  mutate(engine_kw = case_when(vid == 3035 & is.na(engine_kw) ~ 5000,
                               vid == 3030 & is.na(engine_kw) ~ 1000,
                               .default = engine_kw)) |> 
  mutate(engine_kw = ifelse(is.na(engine_kw), 10, engine_kw),
         length = ifelse(is.na(length), 10, length))
vessels %>% 
  filter(is.na(engine_kw) | is.na(length)) %>% 
  select(vid, vessel, length, engine_kw)


LGS <- 
  LGS %>% 
  left_join(vessels %>% 
              select(vid, kw = engine_kw, length, length_class))
paste("Number of records:", nrow(LGS))

# 10. Add metier ---------------------------------------------------------------
metier <-
  tribble(
    ~gid, ~m4, ~m5_comment, ~m5, ~m6,
    1, "LLS", "Fish",      "DEF",  "LLS_DEF_0_0_0",         # Long line
    2, "GNS", "Fish",      "DEF",  "GNS_DEF_>=16_0_0",      # Gill net
    3, "LHM", "Fish",      "DEF",  "LHM_DEF_0_0_0",         # Jiggers (hooks)
    4, "PS",  "Fish",      "SPF",  NA,                      # "Cod" seine
    5, "SDN", "Fish",      "DEF",  "SDN_DEF_>=120_0_0",     # Scottish seine
    6, "OTB", "Fish",      "DEF",  "OTB_DEF_>=120_0_0",     # Bottom fish trawl
    7, "OTM", "Fish",      "SPF",  "OTM_SPF_40-54_0_0",     # Pelagic trawl
    9, "OTB", "Nephrops",  "MCD",  "OTB_MCD_70-89_0_0",     # Bottom nephrops trawl
    10, "PS",  "Fish",      "SPF",  NA,                      # "Herring" seine
    12, "PS",  "Fish",      "SPF",  NA,                      # "Capelin" seine
    14, "OTB", "Shrimp",    "MCD",  "OTB_MCD_40-54_0_0",     # Bottom shrimp trawl
    15, "DRB", "Misc",      "MOL",  "DRB_MOL_>0_0_0",        # Mollusc (scallop) dredge
    17, "FPO", "Misc",      "DEF",  "FPO_DEF_>0_0_0",        # Traps
    18, "FPO", "Misc",      "DEF",  "FPO_DEF_>0_0_0",        # Crab trap
    25, "GNS", "Fish",      "DEF",  "GNS_DEF_>=16_0_0",      # Lumpsucker (female) net
    38, "DRB", "Mollusc",   "MOL",  "DRB_MOL_>0_0_0",        # Mollusc	(cyprine) dredge
    39, "TRP", "Mollusc",   "MOL",  NA,                      # Buccinum trap
    40, "DRB", "Echinoderm","MOL",  "DRB_MOL_>0_0_0",        # Sea-urchins dredge
    91, "GNS", "Fish",      "DEF",  "GNS_DEF_>=16_0_0",      # Monkfish net
    92, "GNS", "Fish",      "DEF",  "GNS_DEF_>=16_0_0")      # Greenland halibut net

# metiers check if in vocabulary
LGS <-
  LGS %>% 
  left_join(metier |> select(gid, m4, m5, m6)) |> 
  mutate(type = "LE",
         country = "ICE")


LGS |> 
  count(gid, m4, m5, m6) |> 
  mutate(m4.valid = m4 %in% metier4$key,
         m5.valdid = m5 %in% metier5$key,
         m6.valid = m6 %in% metier6$key) |> 
  knitr::kable(caption = "Metiers used and check if in vocabulary")

paste("Number of records:", nrow(LGS))


# 11. add ICES rectangles ------------------------------------------------------
res <- data.frame(SI_LONG = LGS$lon,
                  SI_LATI = LGS$lat)
LGS <- 
  LGS %>% 
  mutate(ices = vmstools::ICESrectangle(res)) %>% 
  mutate(valid.ices = !is.na(vmstools::ICESrectangle2LonLat(ices)$SI_LONG))
LGS %>% 
  count(valid.ices) %>% 
  knitr::kable(caption = "Valid ICES rectangles")
paste("Number of records:", nrow(LGS))


# 12. Anonymize vid ------------------------------------------------------------
#     May want to do this more downstream
anonymize.vid <-
  LGS %>%
  select(vid) %>%
  distinct() %>%
  mutate(vid0 = 1:n(),
         vid0 = paste0("ICE", str_pad(vid0, 4, pad = "0")))
LGS <- 
  LGS %>% 
  left_join(anonymize.vid, by = "vid")
paste("Number of records:", nrow(LGS))



# 13. Determine what records to filter downstream ------------------------------
# 
LGS22 <- LGS
LGS <- 
  LGS %>% 
  mutate(i = !gid %in% c(-666, 17, 18, 29),
         use = ifelse(i, TRUE, FALSE),
         use.not = ifelse(i, NA_character_, "drop gear")) %>% 
  mutate(i = gid %in% c(6, 7, 9, 14) & (is.na(t1) | is.na(t2)),
         use = ifelse(i, FALSE, use),
         use.not = ifelse(i, "t1 or t2 missing", use.not)) %>% 
  group_by(vid) %>% 
  mutate(n.records = n()) %>% 
  ungroup() %>% 
  mutate(i = n.records <= 5,
         use = ifelse(i, FALSE, use),
         use.not = ifelse(i, "lb vid records <= 5", use.not)) %>% 
  mutate(i = is.na(kw) | is.na(length),
         use = ifelse(i, FALSE, use),
         use.not = ifelse(i, "vid w. no kw or length", use.not)) %>% 
  select(-n.records)
LGS %>% 
  count(use, gid, use.not) %>% 
  knitr::kable(caption = "Records that retained (FALSE are dropped)")
LGS %>% 
  count(use) %>% 
  mutate(p = round(n / sum(n) * 100, 3)) %>% 
  knitr::kable(caption = "Proportion of records")
# LGS %>% 
#   mutate(no.mid = ifelse(is.na(mids), TRUE, FALSE)) %>% 
#   count(no.mid) %>% 
#   mutate(p = round(n / sum(n) * 100, 3)) %>% 
#   knitr::kable(caption = "Records with no mid, will be lost in the ais/vms processing")
# end: XX. Determine what records to filter in later down streaming


# 14. Save raw (no records filtered) file --------------------------------------
#     Should be as downstream as possible
LGS %>% write_rds("data/LGS_corrected.rds")


# 15. FILTER OUT RECORDS -------------------------------------------------------
#   NOTE: Need to think about non-valid ices rectangles
#         The issue is that it may be invalid in the lgs but valid in the ais
LGS %>% 
  count(use, use.not, valid.ices) %>% 
  mutate(p = round(n / sum(n) * 100, 3))
LGS <- 
  LGS %>% 
  filter(use, valid.ices) %>% 
  select(-c(use, use.not, valid.ices))
# end: FILTER OUT RECORDS


# 16. Collapse all gear but 6, 7, 9, 14 to daily records -----------------------
lgs1 <-
  LGS %>%
  filter(gid %in% c(6, 7, 9, 14))
#  For gear class not 6, 7, 9, 14 summarise the catch for the day
#  and generate a new visir and set t1 and t2 as start and end time of the day
#  The visir used is the lowest visir within a day
lgs2 <-
  LGS %>%
  filter(!gid %in% c(6, 7, 9, 14))
nrow(lgs1) + nrow(lgs2) == nrow(LGS)
# generate a visir-visir lookup within the day
#     the statistics will be summarised by visir.min (to be renamed visir
#     downstream .
visir_visir_lookup <- 
  lgs2 %>% 
  group_by(vid, date, gid) %>% 
  mutate(visir.min = min(visir),
         n.set = n()) %>% 
  ungroup() %>% 
  select(visir.min, visir, n.set)
# there are some large number of records occurring within a single day
#   check if this make sense, but at least expected for dredge per example
#   below.
visir_visir_lookup %>% count(n.set) %>% arrange(-n.set)
visir_visir_lookup %>% 
  arrange(-n.set) %>% 
  filter(visir.min %in% -1204185) %>% 
  left_join(LGS) %>% 
  select(visir.min:vid, date, lon, lat, effort, effort_unit) %>% 
  knitr::kable()
# "median" ICES rectangle? 
# Calculate the median lon and lat within a day and the new ices-rectangle.
#    Note, by calculating median one may end up with an ices rectangle that is 
#     solely on land.
lgs2 <- 
  lgs2 %>% 
  group_by(vid, date) %>% 
  mutate(lon.m = median(lon, na.rm = TRUE),
         lat.m = median(lat, na.rm = TRUE)) %>% 
  ungroup()
nrow(lgs1) + nrow(lgs2) == nrow(LGS)
# Recalculate ICES rectangle based on the median position
res <- data.frame(SI_LONG = lgs2$lon.m,
                  SI_LATI = lgs2$lat.m)

# 2023-05-19: Generates error --------------------------------------------------
# lgs2 <- 
#   lgs2 %>% 
#   select(-c(ices)) %>% 
#   mutate(ices = vmstools::ICESrectangle(res)) %>% 
#   mutate(valid.ices = !is.na(vmstools::ICESrectangle2LonLat(ices)$SI_LONG))
# 
# lgs2 %>% 
#   count(valid.ices)

nrow(lgs1) + nrow(lgs2) == nrow(LGS)
# End: "median" ICES rectangle
# Collapse daily records and then merge with daily records
lgs2B <- 
  lgs2 %>% 
  mutate(year = year(date)) |> 
  # group by all variables that are needed downstream and then some
  group_by(vid, vid0, year, date, gid, ices, m4, m5, m6, length, length_class) %>%
  # get here all essential variables that are needed downstream
  summarise(visir = min(visir),
            n.sets = n(),
            effort = sum(effort, na.rm = TRUE),
            total = sum(total, na.rm = TRUE),
            kw = mean(kw, na.rm = TRUE),
            gear.width = mean(gear.width, na.rm = TRUE),
            .groups = "drop") %>%
  mutate(t1 = ymd_hms(paste0(year(date), "-", month(date), "-", day(date),
                             " 00:00:00")),
         t2 = ymd_hms(paste0(year(date), "-", month(date), "-", day(date),
                             " 23:59:00")))
sum(lgs2B$effort)
sum(lgs2$effort)    # 2023-05-19 Have missing effort needs checks
# minor checks
#   missing unit of effort, fix upstream - not really critical
LGS %>% 
  group_by(gid, effort_unit) %>% 
  summarise(effort = sum(effort),
            kw = mean(kw))
LGS %>% filter(is.na(effort_unit)) %>% pull(vid) %>% unique()
LGS %>% 
  filter(is.na(kw)) %>% 
  count(vid)
LGS %>% 
  filter(is.na(effort)) %>% 
  count(gid)
# Merge stuff again, note some variables in lgs1 not in lgs2B
LGS.collapsed <- 
  bind_rows(lgs1, lgs2B) |> 
  mutate(year = year(date))

LGS.collapsed %>% write_rds("data/logbooks.rds")

devtools::session_info()
