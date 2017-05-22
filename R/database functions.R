connect2DB <- function() {
  load(file = "data/DB settings - clahrc, read.only.rda")

  # Form the URL from the above set of params; ssl=true means we connect over SSL/TLS; Java, by default, checks server TLS/SSL certificate is valid
  db_url <- paste0("jdbc:postgresql://", db_config["host"], ":", db_config["port"], "/", db_config["db_name"], "?user=", db_config["user"])

  # Specify the driver, the Java driver (JAR) file, that Postgres uses double quotes (") to escape identifiers (keywords)
  db_drvr <- RJDBC::JDBC("org.postgresql.Driver", "C:/Program Files/PostgreSQL/postgresql-42.1.1.jar", "\"")

  # Set up our DB connection
  return(RJDBC::dbConnect(db_drvr, db_url, password = db_config["pass"]))
}




getDBTableName <- function(shorthand) {
  if(missing(shorthand) | is.na(shorthand)) stop("Undefined source table name.")
  shorthand <- tolower(shorthand)
  if(shorthand == "ae") {
    rt_val <- "relevant_ae_attendances"
  } else if(shorthand == "apc") {
    rt_val <- "relevant_apc_cips_data"
  } else {
    stop("Undefined source table name.")
  }
  return(rt_val)
}




deleteDBTable <- function(db_conn, table) {
  if(RJDBC::dbExistsTable(db_conn, table) == TRUE) {
    tryCatch(invisible(RJDBC::dbRemoveTable(db_conn, table)), error = function(e) { stop(paste0("Could not remove DB table: ", table)) }, finally = NULL)
  }
}




getDistinctVals <- function(db_conn, field_name, indexes = "", src = "ae") {
  src_tbl <- getDBTableName(src)

  distinct_vals_list <- lapply(indexes, function(x) {
    return(DBI::dbGetQuery(db_conn, paste0("SELECT ", field_name, ", COUNT(*) AS n FROM ", src_tbl, " GROUP BY ", field_name, ";")))
  })

  distinct_vals <- data.table::rbindlist(distinct_vals_list)
  data.table::setnames(distinct_vals, c("value", "n"))
  distinct_vals <- distinct_vals[, .(n = sum(n)), by = value]
  data.table::setorder(distinct_vals, value)
  return(distinct_vals)
}




getCounts <- function(db_conn, where = "1 = 1", src = "ae") {
  src_tbl <- getDBTableName(src)
  DBI::dbGetQuery(db_conn, paste0("SELECT COUNT(*) FROM ", src_tbl, " WHERE ", where, ";"))
}


getQueryResults <- function(table, fields, logic = NA, group.over = NA, limit = 500000) {
  if(is.na(table) | length(table) != 1 | trimws(table) == "") stop("Must specify table in vector of length 1.")
  if(any(is.na(fields)) | length(fields) < 1 | any(trimws(fields) == "")) stop("Must specify field(s) in vector form.")
  if(!is.na(logic) & (length(logic) != 1 | trimws(logic) == "")) stop("If supplied, logic must be passed as a character vector of length 1.")
  if(!is.na(group.over) & (length(group.over) < 1 | trimws(group.over) == "")) stop("If supplied, group.over field(s) must not be blank.")

  fields <- paste0(fields, collapse = ", ")
  temp.table.name <- paste0("temp_rs_", table)

  sql <- paste("CREATE TEMP TABLE", temp.table.name, "AS SELECT row_number() OVER () AS row,", fields, "FROM (SELECT", fields, "FROM", table)
  if(!is.na(logic)) sql <- paste(sql, "WHERE", logic)
  if(!is.na(group.over)) {
    group.over <- paste0(group.over, collapse = ", ")
    sql <- paste(sql, "GROUP BY", group.over)
  }
  sql <- paste0(sql, ") AS st;")

  db_conn <- connect2DB()

  # Fairly quick, depends on table indexing, etc.
  resource <- RJDBC::dbSendUpdate(db_conn, sql)

  # Insignificant
  nrows <- DBI::dbGetQuery(db_conn, paste0("SELECT COUNT(*) FROM ", temp.table.name, ";"))[1, 1]

  offsets <- 0:(floor(nrows / limit) - as.integer(nrows %% limit == 0)) * limit
  sql_queries <- paste("SELECT", fields, "FROM", temp.table.name, "WHERE row >", offsets, "AND row <=", offsets + limit, ";")

  # Bet on 10s per iteration, max limit as much as possible
  result_sets <- lapply(sql_queries, function(sql_query, conn) {
    rs <- DBI::dbGetQuery(conn, sql_query)
    gc()
    return(rs)
  }, conn = db_conn)

  RJDBC::dbDisconnect(db_conn)
  db_conn <- NULL

  return(data.table::rbindlist(result_sets))
}


getAdHocQueryResults <- function(db_conn, sql_query, limit = 500000) {

  sql <- paste("CREATE TEMP TABLE temp_rs_adhoc AS SELECT row_number() OVER () AS row, st.* FROM (", sql_query, ") AS st;")

  # Fairly quick, depends on table indexing, etc.
  resource <- RJDBC::dbSendUpdate(db_conn, sql)

  # Insignificant
  nrows <- DBI::dbGetQuery(db_conn, paste0("SELECT COUNT(*) FROM temp_rs_adhoc;"))[1, 1]

  offsets <- 0:(floor(nrows / limit) - as.integer(nrows %% limit == 0)) * limit
  sql_queries <- paste("SELECT * FROM temp_rs_adhoc WHERE row >", offsets, "AND row <=", offsets + limit, ";")

  # Bet on 10s per iteration, max limit as much as possible
  result_sets <- lapply(sql_queries, function(sql_query, conn) {
    rs <- DBI::dbGetQuery(conn, sql_query)
    gc()
    return(rs)
  }, conn = db_conn)

  data <- data.table::rbindlist(result_sets)
  data[, row := NULL]

  return(data)
}
