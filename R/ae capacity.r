library(data.table)
library(doParallel)
library(ggplot2)
source("r/auxillary functions.R")
source("r/database functions.R")

hes_data <- readRDS("output-data/linked hes ae and apc data - 2017-05-22 16.13.Rds")
ae_data <- copy(hes_data[!is.na(ae_aekey)])

ae_data[, ae_endtime := lubridate::fast_strptime(paste(format(ae_arrivaldate, format = "%Y-%m-%d"), ae_deptime), format = "%Y-%m-%d %H:%M:%S", tz = "UTC", lt = FALSE)]
ae_data[, ':=' (ae_duration = (as.integer(ae_endtime) - as.integer(ae_arrivaltime)) / 60,
                ae_end_hour = as.integer(format(ae_endtime, format = "%H")),
                age_group = cut(age, c(0, 1, seq(5, 100, by = 5), 115), right = FALSE))]

ae_data[ae_duration <= 0, ':=' (ae_duration = ae_duration + 1440L,
                                ae_endtime = as.POSIXct(as.integer(ae_endtime) + 86400L, origin = "1970-01-01 00:00:00 UTC"))]

ggplot(ae_data, aes(x = ae_arrivaldate)) +
  scale_x_date(date_label = "%d-%m-%Y") +
  stat_count() +
  geom_line(aes(x = ae_arrivaldate, y = ae_duration),
            stat = "summary",
            fun.y = "quantile",
            fun.args = list(probs = 0.5, na.rm = TRUE, names = FALSE, type = 8),
            colour = "grey") +
  geom_line(aes(x = ae_arrivaldate, y = ae_duration),
            stat = "summary",
            fun.y = "quantile",
            fun.args = list(probs = 0.9, na.rm = TRUE, names = FALSE, type = 8),
            colour = "blue") +
  geom_line(aes(x = ae_arrivaldate, y = ae_duration),
            stat = "summary",
            fun.y = "quantile",
            fun.args = list(probs = 0.95, na.rm = TRUE, names = FALSE, type = 8),
            colour = "red") +
  geom_hline(yintercept = 14400, colour = "black")

capacity <- copy(ae_data[ae_arrivaltime < as.POSIXct("2012-04-01") & !is.na(ae_procode3) & !is.na(ae_arrivaltime) & !is.na(ae_duration) & !is.na(age_group), .N, by = .(ae_procode3, ae_arrivaltime, age_group, ae_duration)])
rm(ae_data, hes_data)

setorder(capacity, ae_arrivaltime, -ae_duration, age_group, ae_procode3)
capacity[, ae_arrivaltime := as.integer(ae_arrivaltime)]

items <- 12
splits <- as.integer(seq(min(ae_data$ae_arrivaltime), max(ae_data$ae_arrivaltime), length.out = items + 1))
years_capacity <- vector("list", length = items)
years_capacity[[1]] <- capacity[ae_arrivaltime < splits[2]]
for(i in 2:(items-1)) years_capacity[[i]] <- capacity[ae_arrivaltime >= splits[i] & ae_arrivaltime < splits[i+1]]
years_capacity[[items]] <- capacity[ae_arrivaltime >= splits[items]]

cl <- makePSOCKcluster(4)

registerDoParallel(cl)
capacity_summation_list <- foreach(i = 1:length(years_capacity),
                                   .combine = list,
                                   .multicombine = TRUE,
                                   .packages = c("data.table")) %dopar% {
                                     return(years_capacity[[i]][, id := 1:.N][rep(id, ae_duration)][, time_point := ae_arrivaltime + 0L:(.N - 1) * 60, by = id][, .(N = sum(N)), by = .(ae_procode3, age_group, time_point)])
                                   }
stopCluster(cl)

capacity_summation1 <- rbindlist(capacity_summation_list[1:min(items/2)])[, .(N = sum(N)), by = .(ae_procode3, age_group, time_point)][, time_point := as.POSIXct(time_point, origin = "1970-01-01", tz = "UTC")]
capacity_summation2 <- rbindlist(capacity_summation_list[(min(items/2)+1):items])[, .(N = sum(N)), by = .(ae_procode3, age_group, time_point)][, time_point := as.POSIXct(time_point, origin = "1970-01-01", tz = "UTC")]

rm(capacity, years_capacity, capacity_summation_list)
gc()

capacity_summation <- rbindlist(list(capacity_summation1, capacity_summation2))[, .(N = sum(N)), by = .(ae_procode3, age_group, time_point)][, time_point := as.POSIXct(time_point, origin = "1970-01-01", tz = "UTC")]

rm(capacity_summation1, capacity_summation2)
gc()

nrow(capacity_summation)
60*24*365*14*22


capacity_summation[, ':=' (yearmonth = format(time_point, "%Y-%m (%b)"),
                           day = format(time_point, "%w - %a"),
                           hour = as.integer(format(time_point, "%H")),
                           ten_minute_period = floor(as.integer(format(time_point, "%M")) / 10))]

capacity_summation[, ten_minute_period := hour * 60 + ten_minute_period * 10]

capacity_summation[, day_time := paste(day, ten_minute_period, sep = ", ")]

cap <- capacity_summation[, .(N = mean(N)), by = .(age_group, day_time, yearmonth)]

save(capacity_summation, file = "data/capacity summation.rda")

#ggplot(capacity_summation, aes(x = time, weight = N)) +
ggplot(cap[yearmonth < "2012-04 (Apr)"], aes(x = day_time, y = N, fill = age_group)) +
  geom_bar(stat = "identity", position = "fill") +
  facet_wrap(~yearmonth, ncol = 6)


ggplot(cap, aes(x = time_point, weight = N)) +
  geom_density() +
  scale_x_datetime() +
  facet_wrap(~ae_procode3, ncol = 7)




bla <- ae_data[!is.na(ae_endtime) & !is.na(ae_arrivaltime), .(.N, neg = sum(ae_endtime < ae_arrivaltime)), by = ae_arrivaldate]

bla <- melt(bla, id.vars = "ae_arrivaldate", variable.factor = FALSE)

ggplot(bla[ae_arrivaldate > as.Date("2012-10-26") & ae_arrivaldate < as.Date("2012-10-31")], aes(x = ae_arrivaldate, y = value, colour = variable)) +
  geom_line(size = 1) +
  scale_x_date(date_label = "%d-%m-%Y")
