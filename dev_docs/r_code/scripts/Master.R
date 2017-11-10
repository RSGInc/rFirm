
### Initialize Application -------------------------------------------------------------------

# Load global variables
source(file.path("lib", "scripts", "_SYSTEM_VARIABLES.R"))
source(file.path("lib", "scripts", "_BASE_VARIABLES.R"))
source(file.path("lib", "scripts", "_SCENARIO_VARIABLES.R"))
source(file.path("lib", "scripts", "_USER_VARIABLES.R"))

# Load current rFreight installation
suppressWarnings(suppressMessages(library(rFreight, lib.loc = SYSTEM_PKGS_PATH)))

# Check for new rFreight version, load rFreight and other packages, create output folder
initializeApp(rFreight.path = SYSTEM_RFREIGHT_PATH,
              output.path = SCENARIO_OUTPUT_PATH,
              lib = SYSTEM_PKGS_PATH,
              packages = SYSTEM_PKGS,
              reload.rFreight = FALSE)

# Load any other scripts of general functions for use in the model
#source(file.path("lib", "scripts", "omx.R"))

cat("Running the", SCENARIO_NAME, "scenario for", SCENARIO_YEAR)

### 1. Firm Synthesis ------------------------------------------------------------------------

if (SCENARIO_RUN_FIRMSYN) {
  
  # Load executive functions (process inputs and simulation)
  source(file = file.path(SYSTEM_SCRIPTS_PATH, "firm_sim_process_inputs.R"))
  source(file = file.path(SYSTEM_SCRIPTS_PATH, "firm_sim.R"))
  
  # Process inputs
  firm_inputs <- new.env()
  EstablishmentsIndustryTAZ <- firm_sim_process_inputs(envir = firm_inputs)
  Firms <- readRDS("./dev/Data/FirmsandEstablishments.rds")
  # Run simuation
  firm_sim_results <- run_sim(FUN = firm_sim, data = Firms, packages = SYSTEM_PKGS,
                              lib = SYSTEM_PKGS_PATH, inputEnv = firm_inputs)
  
  # Save raw results
  if (USER_SAVE_INTERMEDIATEFILES) {
    save(firm_sim_results, firm_inputs, file = file.path(SCENARIO_OUTPUT_PATH, SYSTEM_FIRMSYN_OUTPUTNAME))
  }
  
} else {
  
  load(file.path(SCENARIO_OUTPUT_PATH, SYSTEM_FIRMSYN_OUTPUTNAME))
  
}

### 2. Simulate Supply Chain ------------------------------------------------------------------------

if (SCENARIO_RUN_SCM) {

  # Load executive functions (process inputs and simulation)
  source(file = file.path(SYSTEM_SCRIPTS_PATH, "sc_sim_process_inputs.R"))
  source(file = file.path(SYSTEM_SCRIPTS_PATH, "sc_sim.R"))

  # Process inputs
  sc_inputs <- new.env()
  sc_sim_process_inputs(envir = sc_inputs, AllFirms = firm_sim_results[["AllFirms"]])

  # Run simuation
  sc_sim_results <- run_sim(FUN = sc_sim, data = firm_sim_results[["naics_set"]],
                            packages = SYSTEM_PKGS, lib = SYSTEM_PKGS_PATH, inputEnv = sc_inputs)

  # Save raw results
  if (USER_SAVE_INTERMEDIATEFILES) {
    save(sc_sim_results, sc_inputs, file = file.path(SCENARIO_OUTPUT_PATH, SYSTEM_SCM_OUTPUTNAME))
  }


} else {

  load(file.path(SCENARIO_OUTPUT_PATH, SYSTEM_SCM_OUTPUTNAME))

}

# ### 3. Simulate Freight Truck Movements  ------------------------------------------------------------------------
# 
# if (SCENARIO_RUN_FTTM) {
#   
#   # Load executive functions (process inputs and simulation)
#   source(file = file.path(SYSTEM_SCRIPTS_PATH, "ft_sim.R"))
#   source(file = file.path(SYSTEM_SCRIPTS_PATH, "ft_sim_process_inputs.R"))
#   
#   # Process inputs
#   ft_inputs <- new.env()
#   ft_sim_process_inputs(envir = ft_inputs, TAZSocioEconomics = firm_inputs[["SocioEconomicsTAZ2"]], AllFirms = firm_sim_results[["AllFirms"]], InternationalThru = sc_sim_results[["InternationalThru"]] )
#   
#   # Run simulation
#   ft_trips <- run_sim(FUN = ft_sim, data = sc_sim_results[["FreightShipments"]], packages = SYSTEM_PKGS,
#                       lib = SYSTEM_PKGS_PATH, inputEnv = ft_inputs)
#   
#   # Save raw results
#   if (USER_SAVE_INTERMEDIATEFILES) {
#     save(ft_trips, ft_inputs, file = file.path(SCENARIO_OUTPUT_PATH, SYSTEM_FTTM_OUTPUTNAME))
#   }
#   
# } else {
#   
#   load(file.path(SCENARIO_OUTPUT_PATH, SYSTEM_FTTM_OUTPUTNAME))
#   
# }
# 
# ### 4. Simulate Commercial Vehicle Movements -------------------------------------------------
# 
# if (SCENARIO_RUN_CVTM) {
#   
#   # Load executive functions (process inputs and simulation)
#   source(file = file.path(SYSTEM_SCRIPTS_PATH, "cv_sim_process_inputs.R"))
#   source(file = file.path(SYSTEM_SCRIPTS_PATH, "cv_sim.R"))
#   
#   # Process inputs
#   cv_inputs <- new.env()
#   cv_sim_process_inputs(envir = cv_inputs, TAZSocioEconomics = firm_inputs[["SocioEconomicsTAZ2"]])
#   
#   # Run simuation
#   RegionalFirms <- firm_sim_results[["AllFirms"]][!is.na(TAZ2)]
#   cv_trips <- run_sim(FUN = cv_sim, data = RegionalFirms[sample(1:nrow(RegionalFirms), size = nrow(RegionalFirms))],
#                       k = USER_PROCESSOR_CORES, packages = SYSTEM_PKGS, lib = SYSTEM_PKGS_PATH,
#                       inputEnv = cv_inputs, exclude = "pfm.shp")
#   cv_trips[, TourID := as.integer(factor(paste(BusID, Vehicle, TourID)))]
#   
#   # Save raw results
#   if (USER_SAVE_INTERMEDIATEFILES) {
#     save(cv_trips, cv_inputs, file = file.path(SCENARIO_OUTPUT_PATH, SYSTEM_CVTM_OUTPUTNAME))
#   }
#   
# } else {
#   
#   load(file.path(SCENARIO_OUTPUT_PATH, SYSTEM_CVTM_OUTPUTNAME))
#   
# }
# 
# ### 5. Produce Regional Trip Tables -------------------------------------------------------------------------
# 
# # Create trip tables for assignment
# if (SCENARIO_RUN_TT) {
#   
#   source(file.path(SYSTEM_SCRIPTS_PATH, "trip_tables.R"))
#   all_trips <- trip_tables(cv_trips = cv_trips, ft_trips = ft_trips)
#   
#   # Save raw results
#   if (USER_SAVE_INTERMEDIATEFILES) {
#     save(all_trips, file = file.path(SCENARIO_OUTPUT_PATH, SYSTEM_TT_OUTPUTNAME))
#   }
#   
# } else {
#   
#   load(file.path(SCENARIO_OUTPUT_PATH, SYSTEM_TT_OUTPUTNAME))
#   
# }
# 
# ### Produce Outputs -------------------------------------------------------------------------
# 
# SCENARIO_RUN_DURATION <- Sys.time() - SCENARIO_RUN_START
# 
# # Generate HTML Dashboard Report
# if (USER_GENERATE_DASHBOARD) {
# 
#   # Process inputs
#   loadPackages(SYSTEM_REPORT_PKGS, lib = SYSTEM_PKGS_PATH)
#   loadInputs(files = c(TAZ_System = file.path(SYSTEM_DATA_PATH, "TAZ_System.rds"),
#                        shp = file.path(SYSTEM_DATA_PATH, "TAZ.rds"),
#                        db_outputs = file.path(SYSTEM_SCRIPTS_PATH, "db_outputs.R")))
#   #shp@data$Organization <- TAZ_System[["Organization"]][match(shp@data[["TAZ"]], TAZ_System[["TAZ"]])]
#   shp@data$Organization <- shp$CountyState
#   skims <- cv_inputs[["skims_tod"]]
#   Facilities <- ft_inputs[["Facilities"]]
#   if (!"TAZ_Layer.html" %in% dir(SYSTEM_TEMPLATES_PATH)) { # Creates the HTML TAZ layer used by all maps
#     basemapWriter(shp, subset = "Organization", dir = SYSTEM_TEMPLATES_PATH)
#   }
#   
#   # Generate dashboard
#   rmarkdown::render(file.path(SYSTEM_TEMPLATES_PATH, "ReportDashboard.Rmd"),
#                     output_dir = SCENARIO_OUTPUT_PATH,
#                     intermediates_dir = SCENARIO_OUTPUT_PATH, quiet = TRUE)
#   ReportDashboard.html <- readLines(file.path(SCENARIO_OUTPUT_PATH, "ReportDashboard.html"))
#   idx <- which(ReportDashboard.html == "window.FlexDashboardComponents = [];")[1]
#   ReportDashboard.html <- append(ReportDashboard.html, "L_PREFER_CANVAS = true;", after = idx)
#   writeLines(ReportDashboard.html, file.path(SCENARIO_OUTPUT_PATH, "ReportDashboard.html"))
#   
# }
