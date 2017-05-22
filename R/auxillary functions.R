createDataFilename <- function(m_name, quiet = TRUE) {
  fn <- paste0("output-data/", m_name, " - ", format(Sys.time(), "%Y-%m-%d %H.%M"), ".Rds")
  if(!quiet) print(fn)
  return(fn)
}


getOptimalCompress <- function(fname) {
  tools::resaveRdaFiles(fname)
  return(tools::checkRdaFiles(fname))
}


classifySECs <- function(data_in) {
# need data with fields:
#   startage - (integer)
#   diag_01 - primary diagnosis (character)
#   diag_02 - primary diagnosis (character)
#   cause - external cause ICD-10 code (character)
#   The returned value is as per data but removes diag_0[1,2] and cause and adds condition (character)

  # Take a copy (becuase of doing all this by ref)
  data <- copy(data_in)

  ## Deal with error codes in the diag_01 field
  # cause code in primary diagnosis field
  data[diag_01 == "R69X3", diag_01 := diag_02]

  data[, ':=' (diag_4char = toupper(substr(diag_01, 1, 4)),
    cause_4char = toupper(substr(cause, 1, 4)),
    condition = "other")]

  # Create 1 character, 3 character and 4 character codes diagnosis codes and 3 character cause code
  data[, ':=' (diag_3char = substr(diag_4char, 1, 3),
    diag_1char = substr(diag_4char, 1, 1),
    cause_3char = substr(cause_4char, 1, 3))]

  # Septic shock
  data[condition == "other" & diag_3char %in% paste0("A", 40:41), condition := "septic shock"]

  # Meningitis
  data[condition == "other" & (diag_3char %in% c(paste0("G0", 1:3), "A39") | diag_4char == "A321"), condition := "meningitis"]

  # Myocardial Infarction
  data[condition == "other" & diag_3char %in% paste0("I", 21:23), condition := "myocardial infarction"]

  # Cardiac arrest
  data[condition == "other" & diag_3char == "I46", condition := "cardiac arrest"]

  # Acute heart failure
  data[condition == "other" & diag_3char == "I50", condition := "acute heart failure"]

  # Stroke/CVA
  data[condition == "other" & (diag_3char %in% paste0("I", c(61, 63:64)) | diag_4char == "I629"), condition := "stroke cva"]

  # Ruptured aortic aneurysm
  data[condition == "other" & diag_4char %in% paste0("I", c(710, 711, 713, 715, 718)), condition := "ruptured aortic aneurysm"]

  # Asthma
  data[condition == "other" & diag_3char %in% paste0("J", 45:46), condition := "asthma"]

  # Pregnancy and birth related
  data[condition == "other" & diag_1char == "O", condition := "pregnancy and birth related"]


  # External causes
  # Road traffic accidents
  data[condition == "other" & (cause_3char %in% c(paste0("V0", 0:9), paste0("V", 10:79)) | cause_4char %in% paste0("V", c(802:805, 821, 892, 830, 832, 833, 840:843, 850:853, 860:863, 870:878))), condition := "road traffic accident"]

  # Falls
  data[condition == "other" & startage < 75 & cause_3char %in% c(paste0("W0", 0:9), paste0("W", 10:19)), condition := "falls"]

  # Self harm
  data[condition == "other" & cause_3char %in% paste0("X", 60:84), condition := "self harm"]


  # The following must appear after the (3) external cause conditions
  # Anaphylaxis
  data[condition == "other" & diag_4char %in% paste0("T", c(780, 782, 805, 886)), condition := "anaphylaxis"]

  # Asphyxiation
  data[condition == "other" & (diag_3char %in% paste0("T", c(17, 71)) | diag_4char == "R090"), condition := "asphyxiation"]

  # Fractured neck of femur
  data[condition == "other" & diag_3char == "S72", condition := "fractured neck of femur"]

  # Serious head injuries
  data[condition == "other" & diag_3char %in% paste0("S0", 2:9), condition := "serious head injury"]

  # format data
  data[, c("diag_01", "diag_02", "cause", "diag_4char", "cause_4char", "diag_3char", "diag_1char", "cause_3char") := NULL]

  return(data)
}



classifyUCCs <- function(data_in) {
  # need data with fields:
  #   startage - (integer)
  #   diag_01 - primary diagnosis (character)
  #   diag_02 - primary diagnosis (character)
  #   cause - external cause ICD-10 code (character)
  #   The returned value is as per data but removes diag_0[1,2] and cause and adds condition (character)

  # Take a copy (becuase of doing all this by ref)
  data <- copy(data_in)

  ## Deal with error codes in the diag_01 field
  # cause code in primary diagnosis field
  data[diag_01 == "R69X3", diag_01 := diag_02]

  data[, ':=' (diag_4char = toupper(substr(diag_01, 1, 4)),
    diag_cause = toupper(substr(cause, 1, 3)),
    condition = "other")]

  # Create 1 character, 3 character and 4 character codes diagnosis codes and 3 character cause code
  data[, ':=' (diag_3char = substr(diag_4char, 1, 3),
    diag_1char = substr(diag_4char, 1, 1))]


  # Hypoglycemia
  data[condition == "other" & (diag_3char %in% paste0("E", 10:15) | diag_4char %in% paste0("E", 161:162)), condition := "hypoglycaemia"]

  # Acute mental health crisis
  data[condition == "other" & diag_1char == "F", condition := "acute mental health crisis"]

  # Epileptic fit
  data[condition == "other" & diag_3char %in% paste0("G", 40:41), condition := "epileptic fit"]

  # Angina
  data[condition == "other" & diag_3char == "I20", condition := "angina"]

  # DVT
  data[condition == "other" & diag_3char %in% paste0("I", 80:82), condition := "dvt"]

  # COPD
  data[condition == "other" & diag_3char %in% paste0("J", 40:44), condition := "copd"]

  # Cellulitis
  data[condition == "other" & diag_3char == "L03", condition := "cellulitis"]

  # Urinary tract infection
  data[condition == "other" & diag_4char == "N390", condition := "urinary tract infection"]

  # Non-specific chest pain
  data[condition == "other" & diag_4char %in% paste0("R0", 72:74), condition := "non-specific chest pain"]

  # Non-specific abdominal pain
  data[condition == "other" & diag_3char == "R10", condition := "non-specific abdominal pain"]

  # Pyrexial child (<6 years)
  data[condition == "other" & diag_3char == "R50" & startage < 6L, condition := "pyrexial child (<6 years)"]

  # Minor head injuries
  data[condition == "other" & diag_3char == "S00", condition := "minor head injuries"]

  # Blocked catheter
  data[condition == "other" & diag_4char == "T830", condition := "blocked catheter"]

  # Falls (75+ years)
  falls_codes_digits <- expand.grid(d1 = 0:1, d2 = 0:9, stringsAsFactors = FALSE)
  falls_codes <- paste0("W", falls_codes_digits$d1, falls_codes_digits$d2)
  data[condition == "other" & diag_cause %in% falls_codes & startage >= 75L, condition := "falls (75+ years)"]

  # format data
  data[, c("diag_01", "diag_02", "cause", "diag_4char", "diag_cause", "diag_3char", "diag_1char") := NULL]

  return(data)
}


searchLong <- function(data, idvars, chars = NA, needles) {
  dt <- copy(data)

  dt_long <- data.table::melt(dt, id.vars = idvars, variable.factor = FALSE)

  if(!missing(chars)) dt_long[, value := substr(value, 1, chars)]
  dt_long <- unique(dt_long[value %in% needles, (idvars), with = FALSE])

  return(dt_long)
}

classifyACSC3.1 <- function(data_in) {
  # CCG Outcome Indicator Set, Indicator 3.1
  # Method as per https://indicators.hscic.gov.uk/download/Clinical%20Commissioning%20Group%20Indicators/Specification/CCG_3.1_I00759_S.pdf

  # Take a copy (becuase of doing all this by ref)
  data <- copy(data_in)

  data[, ':=' (diag_01_3char = substr(apc_diag_01, 1, 3),
                        diag_01_4char = substr(apc_diag_01, 1, 4))]

  # More data is reqd for conditions identified in subsets a, b, and f

  acsc_additional_data_ids <- data[diag_01_3char %in% c("A36", "A37", "B05", "B06", "B26", "J10", "J11", "J14") | # (a)
                             diag_01_4char %in% c("B161", "B169", "J13X", "J153", "J154", "J157", "J159", "J168", "J181", "J188", "M014") | # (a)
                             diag_01_4char %in% paste0("I24", c(0, 8, 9)) | # (b)
                             diag_01_3char %in% c(paste0("L0", 1:4), "L88)") | # (f)
                             diag_01_4char %in% c("I891", "L980", paste0("L08", c(0, 8, 9))), # (f)
                           .(encrypted_hesid, apc_cips, diag_01_4char, diag_01_3char)]

  db_conn <- connect2DB()

  stopifnot(DBI::dbWriteTable(db_conn, "temp_apc_ids", acsc_additional_data_ids[, .(encrypted_hesid, apc_cips)], temporary = TRUE) == TRUE)

  apc_acsc_additional_fields <- c("encrypted_hesid",
                             "cips",
                             paste0("diag_" , c(paste0("0", 1:9), 10:20)),
                             paste0("opertn_" , c(paste0("0", 1:9), 10:24)))

  sql_get_apc_acsc_additional_data <- paste("SELECT",
                                       paste0(apc_acsc_additional_fields, collapse = ", "),
                                       "FROM (",
                                       "SELECT admiepis.epikey, admiepis.cips FROM",
                                       "(SELECT encrypted_hesid, cips, epikey FROM relevant_apc_cips_episode_data WHERE cips_episode = 1) AS admiepis",
                                       "INNER JOIN temp_apc_ids AS ids ON ",
                                       "admiepis.encrypted_hesid = ids.encrypted_hesid AND admiepis.cips = ids.apc_cips",
                                       ") AS link INNER JOIN relevant_apc_episodes AS epidata ON",
                                       "(link.epikey = epidata.epikey)")

  apc_acsc_additional_data <- getAdHocQueryResults(db_conn, sql_get_apc_acsc_additional_data, 500000)[, present := TRUE]

  RJDBC::dbDisconnect(db_conn)
  db_conn <- NULL

  apc_acsc_additional_data <- merge(acsc_additional_data_ids, apc_acsc_additional_data, by.x = c("encrypted_hesid", "apc_cips"), by.y = c("encrypted_hesid", "cips"), all.x = TRUE)

  # Check numbers
#  stopifnot(apc_acsc_additional_data[is.na(present), .N] == 0)
  apc_acsc_additional_data[, present := NULL]

  subset_a_exclusions <- searchLong(apc_acsc_additional_data[diag_01_3char %in% c("A36", "A37", "B05", "B06", "B26", "J10", "J11", "J14") |
                               diag_01_4char %in% c("B161", "B169", "J13X", "J153", "J154", "J157", "J159", "J168", "J181", "J188", "M014"),
                             (c("encrypted_hesid", "apc_cips", paste0("diag_", c(paste0(0, 1:9), 10:20)))), with = FALSE],
                    c("encrypted_hesid", "apc_cips"),
                    3,
                    "D57")

  subset_b.1_exclusions <- searchLong(apc_acsc_additional_data[diag_01_4char %in% paste0("I24", c(0, 8, 9)),
                                    (c("encrypted_hesid", "apc_cips", paste0("opertn_", c(paste0(0, 1:9), 10:20)))), with = FALSE],
                     c("encrypted_hesid", "apc_cips"),
                     1,
                     LETTERS[c(1:20, 22:23)])

  subset_b.2_exclusions <- searchLong(apc_acsc_additional_data[diag_01_4char %in% paste0("I24", c(0, 8, 9)),
                                    (c("encrypted_hesid", "apc_cips", paste0("opertn_", c(paste0(0, 1:9), 10:20)))), with = FALSE],
                     c("encrypted_hesid", "apc_cips"),
                     2,
                     paste0("X", c(0:2, 4:5)))

  subset_f.1_exclusions <- searchLong(apc_acsc_additional_data[diag_01_3char %in% c(paste0("L0", 1:4), "L88)") | # (f)
                                diag_01_4char %in% c("I891", "L980", paste0("L08", c(0, 8, 9))), (c("encrypted_hesid", "apc_cips", paste0("opertn_", c(paste0(0, 1:9), 10:20)))), with = FALSE],
                     c("encrypted_hesid", "apc_cips"),
                     1,
                     LETTERS[c(1:18, 20, 22:23)])

  subset_f.2_exclusions <- searchLong(apc_acsc_additional_data[diag_01_3char %in% c(paste0("L0", 1:4), "L88)") | # (f)
                                diag_01_4char %in% c("I891", "L980", paste0("L08", c(0, 8, 9))), (c("encrypted_hesid", "apc_cips", paste0("opertn_", c(paste0(0, 1:9), 10:20)))), with = FALSE],
                     c("encrypted_hesid", "apc_cips"),
                     2,
                     c(paste0("S", 1:3), paste0("X", c(0:2, 4:5))))

  subset_f.3_exclusions <- searchLong(apc_acsc_additional_data[diag_01_3char %in% c(paste0("L0", 1:4), "L88)") | # (f)
                                diag_01_4char %in% c("I891", "L980", paste0("L08", c(0, 8, 9))), (c("encrypted_hesid", "apc_cips", paste0("opertn_", c(paste0(0, 1:9), 10:20)))), with = FALSE],
                     c("encrypted_hesid", "apc_cips"),
                     3,
                     paste0("S", c(41:45, 48:49)))

  # merge in exclusion data
  acsc_additional_data_exclusion_ids <- unique(rbind(subset_a_exclusions,
                                              subset_b.1_exclusions,
                                              subset_b.2_exclusions,
                                              subset_f.1_exclusions,
                                              subset_f.2_exclusions,
                                              subset_f.3_exclusions))[, exclude := TRUE]

  # now find all cips with an ACSC (based on primary diag code only)
  acsc_identified_conditions <- data[(diag_01_3char %in% c("A36", "A37", "B05", "B06", "B26", "J10", "J11", "J14") | # (a)
                                        diag_01_4char %in% c("B161", "B169", "J13X", "J153", "J154", "J157", "J159", "J168", "J181", "J188", "M014") | # (a)
                                        diag_01_4char %in% paste0("I24", c(0, 8, 9)) | # (b)
                                        diag_01_3char %in% c("A04", "A08", "A09", "E86", "K52") | # (c)
                                        diag_01_4char %in% c("A020", "A059", "A072") | # (c)
                                        diag_01_3char %in% paste0("N", 10:12) | # (d)
                                        diag_01_4char %in% c("N136", "N159", "N300", "N308", "N309", "N390") | # (d)
                                        diag_01_3char %in% paste0("K", 20:21) | # (e)
                                        diag_01_4char %in% paste0("K", rep(25:28, each = 6), c(0:2, 4:6)) | # (e)
                                        diag_01_3char %in% c(paste0("L0", 1:4), "L88)") | # (f)
                                        diag_01_4char %in% c("I891", "L980", paste0("L08", c(0, 8, 9))) | # (f)
                                        diag_01_3char %in% c(paste0("H", 66:67), paste0("J0", c(2:3, 6))) | # (g)
                                        diag_01_4char %in% c("J312", "J040") | # (g)
                                        diag_01_3char %in% paste0("K", c(paste0(0, c(2:6, 8)), as.character(12:13))) | # (h)
                                        diag_01_4char %in% c("A690", "K098", "K099") | # (h)
                                        diag_01_3char %in% c("O15", "R56") | # (i)
                                        diag_01_4char == "G253"), # (i)
                                     .(encrypted_hesid, apc_cips, diag_01_3char, diag_01_4char)]

  # exclude relevent cips from subgroups a, b, and f
  acsc_identified_conditions <- merge(acsc_identified_conditions, acsc_additional_data_exclusion_ids, by = c("encrypted_hesid", "apc_cips"), all.x = TRUE)[is.na(exclude)][, exclude := NULL]

  # categorise ACSC cips (mutually exclusive groups)
  acsc_identified_conditions[diag_01_3char %in% paste0("J", c(10:11, 14)) |
                               diag_01_4char %in% c("J13X", paste0("J15", c(3:4, 7, 9)), "J168", "J181", "J188"),
                             acsc3.1_condition := "influenza, pneumonia"]

  acsc_identified_conditions[diag_01_3char %in% c("A36", "A37", "B05", "B06", "B26") |
                               diag_01_4char %in% c("B161", "B169", "M014"),
                             acsc3.1_condition := "other vaccine preventable"]

  acsc_identified_conditions[diag_01_4char %in% paste0("I24", c(0, 8:9)),
                             acsc3.1_condition := "angina"]

  acsc_identified_conditions[diag_01_3char %in% c("E86", "K52", "A04", "A08", "A09") |
                               diag_01_4char %in% c("A020", "A059", "A072"),
                             acsc3.1_condition := "dehydration and gastroenteritis"]

  acsc_identified_conditions[diag_01_3char %in% paste0("N", 10:12) |
                               diag_01_4char %in% c("N136", "N159", "N390", "N300", "N308", "N309"),
                             acsc3.1_condition := "pyelonephritis and kidney/urinary tract infections"]

  acsc_identified_conditions[diag_01_3char %in% paste0("K", 20:21) |
                               diag_01_4char %in% paste0("K", rep(25:28, each = 6), c(0:2, 4:6)),
                             acsc3.1_condition := "perforated/bleeding ulcer"]

  acsc_identified_conditions[diag_01_3char %in% paste0("L", c(paste0(0, 1:4), "88")) |
                               diag_01_4char %in% c(paste0("L08", c(0, 8:9)), "L980", "I891"),
                             acsc3.1_condition := "cellulitis"]

  acsc_identified_conditions[diag_01_3char %in% c(paste0("H", 66:67), paste0("J0", c(2:3, 6))) |
                               diag_01_4char %in% c("J040", "J312"),
                             acsc3.1_condition := "ear, nose and throat infections"]

  acsc_identified_conditions[diag_01_3char %in% paste0("K", c(paste0(0, c(2:6, 8)), 12:13)) |
                               diag_01_4char %in% c("A690", paste0("K09", 8:9)),
                             acsc3.1_condition := "dental conditions"]

  acsc_identified_conditions[diag_01_3char %in% c("R56", "O15") |
                               diag_01_4char == "G253",
                             acsc3.1_condition := "convulsions and epilepsy"]

  acsc_identified_conditions[is.na(acsc3.1_condition), acsc3.1_condition := "unclassified acsc3.1 condition"]

  data[, c("diag_01_3char", "diag_01_4char") := NULL]

  data <- merge(data, acsc_identified_conditions[, .(encrypted_hesid, apc_cips, acsc3.1_condition)], by = c("encrypted_hesid", "apc_cips"), all.x = TRUE)

  return(data)
}
