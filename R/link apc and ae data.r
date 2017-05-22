# ae_data <- getQueryResults("relevant_ae_attendances", c("encrypted_hesid", "arrivaldate", "arrivaltime", "aekey"), limit = 1000000)
# ae_data[, arrival_datetime := lubridate::fast_strptime(paste(arrivaldate, arrivaltime), format = "%Y-%m-%d %H:%M:%S", tz = "Europe/London", lt = FALSE)]
# ae_data[, arrival_hour := as.integer(format(arrival_datetime, format = "%H"))]
#
# apc_data <- getQueryResults("relevant_apc_cips_data", c("encrypted_hesid", "cips", "cips_start"), logic = "emergency_admission = TRUE", limit = 1000000)
# apc_data[, admission_datetime := lubridate::fast_strptime(paste(cips_start, "23:59:59"), format = "%Y-%m-%d %H:%M:%S", tz = "Europe/London", lt = FALSE)]
#
# ans = merge(apc_data, ae_data, by = "encrypted_hesid", allow.cartesian = TRUE)
# ans[, time_diff_hours := (as.double(admission_datetime) - as.double(arrival_datetime)) / 3600]
# ans <- ans[time_diff_hours > -48 & time_diff_hours < 48]
#
# ggplot2::ggplot(ans[sample.int(nrow(ans), 100000), .(time_diff_hours)], ggplot2::aes(x = time_diff_hours)) + ggplot2::geom_histogram(binwidth = 1)
# ggplot2::ggplot(ans[sample.int(nrow(ans), 100000), .(arrival_hour)], ggplot2::aes(x = arrival_hour)) + ggplot2::stat_count()
#
# ans <- ans[time_diff_hours >= 0 & time_diff_hours < 48]

# reqd
library(data.table)
source("r/auxillary functions.R")
source("r/database functions.R")

linkAEandAPCdata <- function() {

  ae_fields <- c("encrypted_hesid",
                 "procode3",
                 "arrivaldate",
                 "arrivaltime",
                 "sex",
                 "arrivalage",
                 "aearrivalmode",
                 "aeattendcat",
                 "aeattenddisp",
                 "aedepttype",
                 "aeincloctype",
                 "aepatgroup",
                 "aerefsource",
                 "inittime",
                 "trettime",
                 "concltime",
                 "deptime",
                 paste0("invest_" , c(paste0("0", 1:9), 10:12)),
                 paste0("treat_" , c(paste0("0", 1:9), 10:12)),
                 "lsoa01",
                 "sushrg",
                 "sushrgvers",
                 "aekey")


  apc_fields <- c("encrypted_hesid",
                  "cips",
                  "cips_start",
                  "emergency_admission",
                  paste0("diag_" , c(paste0("0", 1:2))),
                  "cause",
                  "startage",
                  "sex",
                  "admimeth",
                  "lsoa01",
                  "sitetret",
                  "elecdate",
                  "sushrg",
                  "nights_admitted",
                  "cips_finished",
                  "died")

  sql_get_ae <- paste("SELECT",
                      paste0("ae.", ae_fields, " AS ae_", ae_fields, collapse = ", "),
                      "FROM relevant_ae_attendances AS ae")

  sql_get_apc <- paste("SELECT",
                       paste0(apc_fields, " AS apc_", apc_fields, collapse = ", "),
                       "FROM relevant_apc_cips_data WHERE emergency_admission = TRUE")

  db_conn <- connect2DB()
  # ~10 mins
#  pc <- proc.time()
  ae_data <- getAdHocQueryResults(db_conn, sql_get_ae, 50000)
#  proc.time() - pc

  # ~3 mins
#  pc <- proc.time()
  apc_data <- getAdHocQueryResults(db_conn, sql_get_apc, 50000)
#  proc.time() - pc

  RJDBC::dbDisconnect(db_conn)
  db_conn <- NULL

  ae_data[, ':=' (ae_arrivaldate = as.Date(lubridate::fast_strptime(ae_arrivaldate, format = "%Y-%m-%d", lt = FALSE)),
                  ae_arrivaltime = lubridate::fast_strptime(paste(ae_arrivaldate, ae_arrivaltime), format = "%Y-%m-%d %H:%M:%S", lt = FALSE))]
  apc_data[, apc_cips_start := as.Date(lubridate::fast_strptime(apc_cips_start, format = "%Y-%m-%d", lt = FALSE))]

  ae_apc_data_link <- merge(ae_data[, .(ae_encrypted_hesid, ae_aekey, ae_arrivaldate, ae_arrivaltime)],
                            apc_data[, .(apc_encrypted_hesid, apc_cips, apc_cips_start)],
                            by.x = "ae_encrypted_hesid", by.y = "apc_encrypted_hesid",
                            allow.cartesian = TRUE)

  ae_apc_data_link <- ae_apc_data_link[as.integer(apc_cips_start) - as.integer(ae_arrivaldate) >= 0 & as.integer(apc_cips_start) - as.integer(ae_arrivaldate) < 2]

  # Keep only first CIPS after A&E
  ae_apc_data_link[, apc_rank_per_ae_attendance := frank(ae_apc_data_link, ae_encrypted_hesid, ae_aekey, apc_cips)]
  ae_apc_data_link[, apc_rank_per_ae_attendance := frank(apc_rank_per_ae_attendance), by = .(ae_encrypted_hesid, ae_aekey)]
  # ae_apc_data_link[apc_rank_per_ae_attendance > 1, .N]
  # ae_apc_data_link[apc_rank_per_ae_attendance > 1, .N, by = .(ae_encrypted_hesid, ae_aekey)][, .N]
  ae_apc_data_link_unique <- copy(ae_apc_data_link[apc_rank_per_ae_attendance == 1])

  # Keep only last A&E attendance before CIPS
  ae_apc_data_link_unique[, ae_rank_per_cips := -1 * frank(ae_apc_data_link_unique, ae_encrypted_hesid, apc_cips, ae_arrivaltime, ae_aekey)]
  ae_apc_data_link_unique[, ae_rank_per_cips := frank(ae_rank_per_cips), by = .(ae_encrypted_hesid, apc_cips)]
  # ae_apc_data_link_unique[ae_rank_per_cips > 1, .N]
  # ae_apc_data_link_unique[ae_rank_per_cips > 1, .N, by = .(ae_encrypted_hesid, apc_cips)][, .N]
  ae_apc_data_link_unique <- ae_apc_data_link_unique[ae_rank_per_cips == 1]

  ae_apc_data_link_unique[, c("apc_rank_per_ae_attendance", "ae_rank_per_cips") := NULL]

  # ensure all linked attendances and all CIPS are distinct
  stopifnot(ae_apc_data_link_unique[, .N, by = .(ae_encrypted_hesid, apc_cips)][N > 1, .N] == 0 & ae_apc_data_link_unique[, .N, by = .(ae_encrypted_hesid, ae_aekey)][N > 1, .N] == 0)

  # Merge 1-to-1 attendances and CIPS - keeping unlinked attendances and CIPS also
  ae_apc_data <- merge(ae_data, ae_apc_data_link_unique[, .(ae_encrypted_hesid, ae_aekey, apc_cips)], by = c("ae_encrypted_hesid", "ae_aekey"), all.x = TRUE)
  ae_apc_data <- merge(ae_apc_data, apc_data, by.x = c("ae_encrypted_hesid", "apc_cips"), by.y = c("apc_encrypted_hesid", "apc_cips"), all = TRUE)

  # Tidy up memory
  rm(ae_data, apc_data, ae_apc_data_link, ae_apc_data_link_unique)
  gc()

  # Tidy up data
  setnames(ae_apc_data, "ae_encrypted_hesid", "encrypted_hesid")

  ae_apc_data[, ':=' (age = as.integer(ae_arrivalage),
                      sex = as.integer(ae_sex),
                      lsoa01 = ae_lsoa01)]
  ae_apc_data[is.na(ae_aekey), ':=' (age = as.integer(apc_startage),
                                     sex = as.integer(apc_sex),
                                     lsoa01 = apc_lsoa01)]

  # Sex is always consistent acros HES A&E and APC so remove dataset specific sex vars
  stopifnot(ae_apc_data[!is.na(ae_sex) & !is.na(apc_sex) & as.integer(ae_sex) != as.integer(apc_sex), .N] == 0)
  ae_apc_data[, c("ae_sex", "apc_sex") := NULL]

  # age is usually consistent, when not this probably due to birthday lying on the admission day and that being the day after the attendance
  #ggplot(ae_apc_data[!is.na(ae_arrivalage) & !is.na(apc_startage) & ae_arrivalage != apc_startage, .(apc_startage, ae_arrivalage)], aes(x = ae_arrivalage, y = apc_startage)) + geom_point()

  # For all attendances, find the last attendance a at Type 1 A&E
  last_ae_attendance <- merge(ae_apc_data[!is.na(ae_aekey), .(encrypted_hesid, ae_aekey, ae_arrivaltime)], ae_apc_data[ae_aedepttype == "01", .(encrypted_hesid, ae_arrivaltime)], by = "encrypted_hesid", allow.cartesian = TRUE)
  last_ae_attendance <- last_ae_attendance[ae_arrivaltime.x > ae_arrivaltime.y]
  last_ae_attendance[, ae_duration_since_previous_ae_attendance_days := (as.double(ae_arrivaltime.x) - as.double(ae_arrivaltime.y)) / 86400]
  # SLOW!!
  last_ae_attendance[, prev_ae_rank := frank(ae_duration_since_previous_ae_attendance_days, ties.method = "random"), by = ae_aekey]
  last_ae_attendance <- last_ae_attendance[prev_ae_rank == 1]

  # Attach last attendance, for those that exist in data
  ae_apc_data <- merge(ae_apc_data, last_ae_attendance[, .(ae_aekey, ae_duration_since_previous_ae_attendance_days)], by = "ae_aekey", all.x = TRUE)

  # Look at consistentcy of match with regard to attendance disposal and admission method
  # ae_apc_data[!is.na(ae_aekey) & !is.na(apc_cips), .N, by = .(ae_aeattenddisp, apc_admimeth)][order(-N)]

  # ensure all A&E attendances and all CIPS records are distinct (no dups!)
  stopifnot(ae_apc_data[!is.na(apc_cips), .N, by = .(encrypted_hesid, apc_cips)][N > 1, .N] == 0 & ae_apc_data[!is.na(ae_aekey), .N, by = .(encrypted_hesid, ae_aekey)][N > 1, .N] == 0)

  # save - SLOW!!
  #saveRDS(ae_apc_data, file = createDataFilename("linked hes ae and apc data"), compress = "xz")

  # ~3.5mins
  ae_apc_data <- readRDS("output-data/linked hes ae and apc data - 2017-05-17 14.30.Rds")

  # ~4.5mins
  ae_apc_data <- classifyACSC3.1(ae_apc_data)

  # save - SLOW!! ~50mins
  saveRDS(ae_apc_data, file = createDataFilename("linked hes ae and apc data"), compress = "xz")

  return(TRUE)
}
