db_config <- c(host = "",
               port = "",
               db_name = "",
               user = "",
               pass = "")

save(db_config, file = paste0("data/DB settings - ", db_config["db_name"], ", ", db_config["user"], ".rda"))
