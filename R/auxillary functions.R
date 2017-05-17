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
