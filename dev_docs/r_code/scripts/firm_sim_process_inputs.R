
# This function loads all necessary inputs into envir, after any needed transformations
firm_sim_process_inputs <- function(envir) {
  
  ### Load and process project input files
  project.files <- c(c_naics_industry           = file.path(SYSTEM_DATA_PATH, "corresp_naics_industry.csv"),
                     c_naics_empcat             = file.path(SYSTEM_DATA_PATH, "corresp_naics_empcat.csv"),
                     c_lehd_naics_empcat        = file.path(SYSTEM_DATA_PATH, "lehd_naics_empcat.csv"),
                     c_taz1_taz2                = file.path(SYSTEM_DATA_PATH, "corresp_taz1_taz2.csv"),
                     c_taz1_fips                = file.path(SYSTEM_DATA_PATH, "corresp_taz_fips.csv"),
                     c_taz1_faf4                = file.path(SYSTEM_DATA_PATH, "corresp_taz_faf4.csv"),
                     unitcost                   = file.path(SYSTEM_DATA_PATH, "data_unitcost.csv"),
                     prefweights                = file.path(SYSTEM_DATA_PATH, "data_firm_pref_weights.csv"),
                     EmploymentCategories       = file.path(SYSTEM_DATA_PATH, "EmploymentCategories.rds"),
                     EstablishmentsIndustryTAZ  = file.path(SYSTEM_DATA_PATH, "EstablishmentsIndustryTAZ.rds"),
                     InputOutputValues          = file.path(SYSTEM_DATA_PATH, "InputOutputValues.rds"),
                     firm_sim_enumerate         = file.path(SYSTEM_SCRIPTS_PATH, "firm_sim_enumerate.R"),
                     firm_sim_tazallocation     = file.path(SYSTEM_SCRIPTS_PATH, "firm_sim_tazallocation.R"),
                     firm_sim_scale_employees   = file.path(SYSTEM_SCRIPTS_PATH, "firm_sim_scale_employees.R"),
                     firm_sim_sctg              = file.path(SYSTEM_SCRIPTS_PATH, "firm_sim_sctg.R"),
                     firm_sim_types             = file.path(SYSTEM_SCRIPTS_PATH, "firm_sim_types.R"),
                     firm_sim_iopairs           = file.path(SYSTEM_SCRIPTS_PATH, "firm_sim_iopairs.R"),
                     firm_sim_producers         = file.path(SYSTEM_SCRIPTS_PATH, "firm_sim_producers.R"),
                     firm_sim_consumers         = file.path(SYSTEM_SCRIPTS_PATH, "firm_sim_consumers.R"),
                     firm_sim_naics_set         = file.path(SYSTEM_SCRIPTS_PATH, "firm_sim_naics_set.R")
                     )
  
  loadInputs(files = project.files, envir = envir,
             
             # Some datasets require processing upon loading
             fread.args = list(c_naics_industry = list(stringsAsFactors = TRUE, colClasses = c(NAICS3 = "factor")),
                               c_naics_empcat = list(stringsAsFactors = TRUE, colClasses = c(NAICS2n3 = "factor"))
                               )
  )
  
  # Clean up TAZ field naming to consistent names used in model components
  # TAZ1 for large scale/national zones
  # TAZ2 for smaller scale/regional zones
  # This section of code will be updated if zone system changes/input naming changes
  setnames(envir[["c_taz1_faf4"]], "TAZ", "TAZ1")
  setnames(envir[["c_taz1_fips"]], "TAZ", "TAZ1")
  # setnames(envir[["c_taz1_taz2"]], c("TAZ_Nat","TAZ_Reg"), c("TAZ1","TAZ2"))
  setnames(envir[["EstablishmentsIndustryTAZ"]], "TAZ", "TAZ1")
  
  ### Load and process scenario input files
  scenario.files <- c(SocioEconomicsTAZ2 = file.path(SCENARIO_INPUT_PATH, "lehdtazm.csv"),
                      SocioEconomicsTAZ1 = file.path(SCENARIO_INPUT_PATH, "lehdtazm.csv"))
  loadInputs(files = scenario.files, envir = envir,
             fread.args = list(SocioEconomicsTAZ2 = list(select = c("TAZ", "HH", "aer","amf", "con",	"edu",
                                                                    "fsd",	"gov",	"hss",	"mfg",	"mht",
                                                                    "osv",	"pbs",	"rcs",	"twu",	"wt")),
                               SocioEconomicsTAZ1 = list(select = c("TAZ", "HH", "aer","amf", "con",	"edu",
                                                                    "fsd",	"gov",	"hss",	"mfg",	"mht",
                                                                    "osv",	"pbs",	"rcs",	"twu",	"wt")))
             )
  # loadInputs(files = scenario.files, envir = envir)
  
  # All socio-economic variables should be integers
  envir[["SocioEconomicsTAZ1"]] <- as.data.table(lapply(X = envir[["SocioEconomicsTAZ1"]], FUN = as.integer))
  setnames(envir[["SocioEconomicsTAZ1"]], "TAZ", "TAZ1")
  envir[["SocioEconomicsTAZ2"]] <- as.data.table(lapply(X = envir[["SocioEconomicsTAZ2"]], FUN = as.integer))
  setnames(envir[["SocioEconomicsTAZ2"]], "TAZ", "TAZ2")
  
  ### Return the cbp table
  EstablishmentsIndustryTAZ <- envir[["EstablishmentsIndustryTAZ"]]
  rm(EstablishmentsIndustryTAZ, envir = envir)
  return(EstablishmentsIndustryTAZ)
  
}
