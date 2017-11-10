
# This function loads all necessary inputs into envir, after any needed transformations
sc_sim_process_inputs <- function(envir, AllFirms) {
  
  ### Load and process project input files
  project.files <- c(#AnnualFactor                = file.path(SYSTEM_DATA_PATH, "AnnualFactor.rds"),
                     c_taz1_fips                 = file.path(SYSTEM_DATA_PATH, "corresp_taz_fips.csv"),
                     c_taz1_faf4                 = file.path(SYSTEM_DATA_PATH, "corresp_taz_faf4.csv"),
                     c_taz1_taz2                = file.path(SYSTEM_DATA_PATH, "corresp_taz1_taz2.csv"),
                     # c_sctg_cat                 = file.path(SYSTEM_DATA_PATH, "corresp_sctg_category.csv"),
                     # CommodityFlows             = file.path(SYSTEM_DATA_PATH, "CommodityFlows.rds"),
                     # Payload                    = file.path(SYSTEM_DATA_PATH, "data_payload.csv"),
                     # EmptyTruck                 = file.path(SYSTEM_DATA_PATH, "data_emptytruck.csv"),
                     # TruckType                  = file.path(SYSTEM_DATA_PATH, "data_trucktype.csv"),
                     ModeChoiceParameters       = file.path(SYSTEM_DATA_PATH, "ModeChoiceParameters.rds"),
                     # ModeChoiceConstants        = file.path(SYSTEM_DATA_PATH, "ModeChoiceConstants.rds"),
                     PMGParameters              = file.path(SYSTEM_DATA_PATH, "PMGParameters.rds"), # Input parameters to PMG
                     sctg                       = file.path(SYSTEM_DATA_PATH, "corresp_sctg_category.csv"),
                     mode_description           = file.path(SYSTEM_DATA_PATH,"data_mode_description.csv"),
                     FAF_DISTANCE               = file.path(SYSTEM_DATA_PATH, "data_faf_distance.csv"),
                     Supplier_Selection_Distribution = file.path(SYSTEM_DATA_PATH, "data_faf_ton_distribution.csv"),
                     mesozone_gcd               = file.path(SYSTEM_DATA_PATH, "data_mesozone_gcd.csv"),
                     distchannel_calibration    = file.path(SYSTEM_DATA_PATH, "model_distchannel_calibration.csv"),
                     distchannel_food           = file.path(SYSTEM_DATA_PATH, "model_distchannel_food.csv"),
                     # distchannel_food_cal       = file.path(SYSTEM_DATA_PATH, "model_distchannel_food_cal.csv"),
                     distchannel_mfg            = file.path(SYSTEM_DATA_PATH, "model_distchannel_mfg.csv"),
                     # distchannel_mfg_cal        = file.path(SYSTEM_DATA_PATH, "model_distchannel_mfg_cal.csv"),
                     # shipsize_calibration       = file.path(SYSTEM_DATA_PATH, "model_shipsize_calibration.csv"),
                     # shipsize_food              = file.path(SYSTEM_DATA_PATH, "model_shipsize_food.csv"),
                     # shipsize_food_cal          = file.path(SYSTEM_DATA_PATH, "model_shipsize_food_cal.csv"),
                     # shipsize_mfg               = file.path(SYSTEM_DATA_PATH, "model_shipsize_mfg.csv"),
                     # shipsize_mfg_cal           = file.path(SYSTEM_DATA_PATH, "model_shipsize_mfg_cal.csv"),
                     shipsize                   = file.path(SYSTEM_DATA_PATH, "data_commodity_shipmentsizes.csv"),
                     # DistFacilitiesTAZ          = file.path(SYSTEM_DATA_PATH, "DistributionFacilitiesTAZ.rds"),
                     AirportZones               = file.path(SYSTEM_DATA_PATH, "AirportZones.csv"),
                     SeaportZones               = file.path(SYSTEM_DATA_PATH, "SeaportZones.csv"),
                     # sc_sim_fafextract          = file.path(SYSTEM_SCRIPTS_PATH, "sc_sim_fafextract.R"),
                     # sc_sim_supplierselection   = file.path(SYSTEM_SCRIPTS_PATH, "sc_sim_supplierselection.R"),
                     sc_sim_pmg                 = file.path(SYSTEM_SCRIPTS_PATH, "sc_sim_pmg.R"),
                     sc_sim_distchannel         = file.path(SYSTEM_SCRIPTS_PATH, "sc_sim_distchannel.R"),
                     # sc_sim_shipmentsize        = file.path(SYSTEM_SCRIPTS_PATH, "sc_sim_shipmentsize.R"),
                     sc_sim_traveltimedistance  = file.path(SYSTEM_SCRIPTS_PATH, "sc_sim_traveltimedistance.R"),
                     sc_sim_modalcosts          = file.path(SYSTEM_SCRIPTS_PATH, "sc_sim_modalcosts.R")
                     # sc_sim_shipmentmode        = file.path(SYSTEM_SCRIPTS_PATH, "sc_sim_shipmentmode.R"),
                     # sc_sim_transferlocations   = file.path(SYSTEM_SCRIPTS_PATH, "sc_sim_transferlocations.R"),
                     # sc_sim_internationalthru   = file.path(SYSTEM_SCRIPTS_PATH, "sc_sim_internationalthru.R"),
                     # sc_sim_trips               = file.path(SYSTEM_SCRIPTS_PATH, "sc_sim_trips.R")
  )
  
  loadInputs(files = project.files, envir = envir,
             fread.args = list(c_sctg_cat = list(stringsAsFactors = TRUE)))
  
  # Clean up TAZ field naming to consistent names used in model components
  # TAZ1 for large scale/national zones
  # TAZ2 for smaller scale/regional zones
  setnames(envir[["c_taz1_faf4"]], "TAZ", "TAZ1")
  setnames(envir[["c_taz1_fips"]], "TAZ", "TAZ1")
  # setnames(envir[["c_taz1_taz2"]], c("TAZ_Nat","TAZ_Reg"), c("TAZ1","TAZ2"))
  
  # Load model regions shape file
  Taz.shp <- readOGR(SYSTEM_DATA_PATH, layer = "NUMA_Polygon", verbose = FALSE)
  Taz.shp$TAZ1 <- Taz.shp$FID_1
  
  TazGCD <- cbind(data.table(TAZ1 = Taz.shp$TAZ1), data.table(coordinates(Taz.shp)))
  setnames(TazGCD, c("V1", "V2"), c("LONG", "LAT"))
  setkey(TazGCD, "TAZ1")

  envir[["TazGCD"]] <- TazGCD
  
  
  ### Load and process scenario input files
  
  # Load modal skims
  WaterSkimDeep <- fread(file.path(SCENARIO_INPUT_PATH, "WaterwayDeep_Skim.csv"), verbose = FALSE, showProgress = FALSE)
  WaterSkimShal <- fread(file.path(SCENARIO_INPUT_PATH, "WaterwayShallow_Skim.csv"), verbose = FALSE, showProgress = FALSE)
  envir[["WaterSkim"]] <- rbind(WaterSkimDeep, WaterSkimShal)
  envir[["WaterSkim"]] <- envir[["WaterSkim"]][,1:3,with=FALSE]
  
  
  PipePetrolSkim <- fread(file.path(SCENARIO_INPUT_PATH, "Petrol_Skim.csv"), verbose = FALSE, showProgress = FALSE)
  PipeCrudeSkim <- fread(file.path(SCENARIO_INPUT_PATH, "Crude_Skim.csv"), verbose = FALSE, showProgress = FALSE)
  PipeCoalNECSkim <- fread(file.path(SCENARIO_INPUT_PATH, "CoalNEC_Skim.csv"), verbose = FALSE, showProgress = FALSE)
  PipePetrolSkim[, commodity := "Petrol"]
  PipeCrudeSkim[, commodity := "Crude"]
  PipeCoalNECSkim[, commodity := "CoalNEC"]
  envir[["PipeSkim"]] <- rbind(PipePetrolSkim, PipeCrudeSkim, PipeCoalNECSkim)
  
  
  scenario.files <- c(TruckSkim = file.path(SCENARIO_INPUT_PATH, "Highway_Skim.csv"),
                      RailSkim = file.path(SCENARIO_INPUT_PATH, "Railway_Skim.csv"))
  loadInputs(files = scenario.files, envir = envir)
  
  
  # Units: Time is in minutes, distance is in miles
  setnames(envir[["TruckSkim"]], c("oTAZ1", "dTAZ1", "trucktime", "truckdist"))
  setnames(envir[["RailSkim"]], c("oTAZ1", "dTAZ1", "railtime", "raildist"))
  setnames(envir[["WaterSkim"]], c("oTAZ1", "dTAZ1", "waterdist"))
  setnames(envir[["PipeSkim"]], c("oTAZ1", "dTAZ1", "pipedist", "commodity"))
  
  
  # For testing purposes only, mimics above code to prepare environment for
  # sc_sim_traveltimedistance.R and sc_sim_modalcosts.R without using the envir
  # variable.
  ModeChoiceParameters <- readRDS(file.path(SYSTEM_DATA_PATH, "ModeChoiceParameters.rds"))
  AirportZones <- fread(file.path(SYSTEM_DATA_PATH, "AirportZones.csv"), verbose = FALSE, showProgress = FALSE)
  SeaportZones <- fread(file.path(SYSTEM_DATA_PATH, "SeaportZones.csv"), verbose = FALSE, showProgress = FALSE)
  
  
  # WaterSkim <- rbind(WaterSkimDeep, WaterSkimShal)
  # 
  # PipeSkim <- rbind(PipePetrolSkim, PipeCrudeSkim, PipeCoalNECSkim)
  # 
  # TruckSkim <- fread(file.path(SCENARIO_INPUT_PATH, "Highway_Skim.csv"), verbose = FALSE, showProgress = FALSE)
  # RailSkim <- fread(file.path(SCENARIO_INPUT_PATH, "Railway_Skim.csv"), verbose = FALSE, showProgress = FALSE)
  # 
  # setnames(TruckSkim, c("oTAZ1", "dTAZ1", "trucktime", "truckdist"))
  # setnames(RailSkim, c("oTAZ1", "dTAZ1", "railtime", "raildist"))
  # setnames(WaterSkim, c("oTAZ1", "dTAZ1", "waterdist"))
  # setnames(PipeSkim, c("oTAZ1", "dTAZ1", "pipedist", "commodity"))
  
  #Prepare SCTG specific input file
  
  setnames(envir[["sctg"]],"Commodity_SCTG","SCTG")
  setkey(envir[["sctg"]],SCTG)
  
  #Assign values for B2,B3,B5,a, and sdQ paramaters in logistics cost equation
  
  envir[["sctg"]][,c("B2","B3","B5"):=c(envir[["ModeChoiceParameters"]][Variable=="LowDiscRate",Value],envir[["ModeChoiceParameters"]][Variable=="MedDiscRate",Value],envir[["ModeChoiceParameters"]][Variable=="MedDiscRate",Value],envir[["ModeChoiceParameters"]][Variable=="MedDiscRate",Value],envir[["ModeChoiceParameters"]][Variable=="HighDiscRate",Value])[match(Category,c("Bulk natural resource (BNR)","Animals","Intermediate processed goods (IPG)","Other","Finished goods (FG)"))]]
  
  envir[["sctg"]][,a:=c(envir[["ModeChoiceParameters"]][Variable=="LowMultiplier",Value],envir[["ModeChoiceParameters"]][Variable=="MediumMultiplier",Value],envir[["ModeChoiceParameters"]][Variable=="HighMultiplier",Value])[match(Category2,c("Functional","Functional/Innovative","Innovative"))]]
  
  envir[["sctg"]][,sdQ:=c(envir[["ModeChoiceParameters"]][Variable=="LowVariability",Value],envir[["ModeChoiceParameters"]][Variable=="MediumVariability",Value],envir[["ModeChoiceParameters"]][Variable=="HighVariability",Value])[match(Category2,c("Functional","Functional/Innovative","Innovative"))]]
  
  
  
  #For each of the 21 possible paths, define the B0 (logistics cost equation constant) and the ls (log savings) value
  
  #These vary depending on the commodity and mode; extremely high values of B0 are set to indicate a non-available mode-path
  
  envir[["sctg"]][,paste0("B0",1:21):=1E4]
  
  envir[["sctg"]][,paste0("ls",1:21):=1]
  
  
  
  #Category='Bulk natural resource (BNR)'
  
  # envir[["sctg"]][Category=="Bulk natural resource (BNR)",paste0("B0",7) := 0]
  # 
  # envir[["sctg"]][Category=="Bulk natural resource (BNR)",paste0("ls",7) := 0.5]
  # 
  # envir[["sctg"]][Category=="Bulk natural resource (BNR)",paste0("B0",c(4,12)) := envir[["sctg"]][Category=="Bulk natural resource (BNR)",paste0("B0",c(4,12)), with = FALSE] * 0.1]
  # 
  # envir[["sctg"]][Category=="Bulk natural resource (BNR)",paste0("B0",c(5:6,13:14,15:18)) := envir[["sctg"]][Category=="Bulk natural resource (BNR)",paste0("B0",c(5:6,13:14,15:18)), with = FALSE] * 10.0]
  # 
  # # envir[["sctg"]][Category=="Bulk natural resource (BNR)",ls3 := 0.5]
  # 
  # ### Set the pipeline (55,56,57) for 16:19 envir[["sctg"]]
  # 
  # envir[["sctg"]][SCTG %in% c(16:19), paste0("B0",19:21) := 0]
  # envir[["sctg"]][SCTG %in% c(16:19), paste0("ls",19:21) := 0.5]
  # 
  # 
  # 
  # #Category='Animals'
  # 
  # envir[["sctg"]][Category=="Animals",paste0("B0",c(4,12)) := envir[["sctg"]][Category=="Animals",paste0("B0",c(4,12)), with = FALSE] * 0.75]
  # 
  # envir[["sctg"]][Category=="Animals",paste0("B0",c(3,5:6,13:14)) := envir[["sctg"]][Category=="Animals",paste0("B0",c(1:2,13:30)), with = FALSE] * 10.0]
  # 
  # envir[["sctg"]][Category=="Animals",B01 := B031*0.25]
  # 
  # 
  # 
  # #Category='Intermediate processed goods (IPG)'
  # 
  # envir[["sctg"]][Category=="Intermediate processed goods (IPG)" | Category=="Other",paste0("B0",1:12) := envir[["sctg"]][Category=="Intermediate processed goods (IPG)" | Category=="Other",paste0("B0",1:12), with = FALSE]* 2.0]
  # 
  # envir[["sctg"]][Category=="Finished goods (FG)",paste0("B0",1:12) := envir[["sctg"]][Category=="Finished goods (FG)",paste0("B0",1:12), with = FALSE] * 10.0]
  # 
  # envir[["sctg"]][Category=="Intermediate processed goods (IPG)" | Category=="Other",paste0("B0",14:30) := envir[["sctg"]][Category=="Intermediate processed goods (IPG)" | Category=="Other",paste0("B0",14:30), with = FALSE] * 0.9]
  # 
  # envir[["sctg"]][Category=="Intermediate processed goods (IPG)" | Category=="Other",paste0("B0",31:46) := envir[["sctg"]][Category=="Intermediate processed goods (IPG)" | Category=="Other",paste0("B0",31:46), with = FALSE] * 0.5]
  # 
  # 
  # 
  # #Category='Finished goods (FG)'
  # 
  # envir[["sctg"]][Category=="Finished goods (FG)",paste0("B0",31:46) := envir[["sctg"]][Category=="Finished goods (FG)",paste0("B0",31:46), with = FALSE] * 0.9]
  # 
  # envir[["sctg"]][Category=="Finished goods (FG)",paste0("B0",c(32:45,47:50)) := sctg[Category=="Finished goods (FG)",paste0("B0",c(32:45,47:50)), with = FALSE] * 0.25]
  # 
  
  
  ####
  envir[["mode_description"]] <- envir[["mode_description"]][,.(path=ModeNumber,Mode.Domestic=LinehaulMode)]
  setkey(envir[["mode_description"]],"path")
  
  envir[["famesctg"]] <- c(rep("A",3),rep("E",4),rep("K",2),rep("F",3),rep("C",7),rep("B",5),rep("J",6),"C",rep("G",3),"D",rep("I",2),"G","J",rep("H",4))
  
  envir[["distchan_calcats"]] <- data.table(CHOICE=c("0","1","2+","2+"),CHID=1:4)
  
  envir[["FUTURE_MAX_SIZE"]] <- USER_FUTRE_MAX_SIZE*(1024**3)
  envir[["AVAILABLE_RAM"]] <- USER_MAX_RAM*(1024**3) #50 GB
  
  envir[["NSupplierPerBuyer"]] <- 20
  envir[["FAF_DISTANCE"]][,Distance_Bin:=findInterval(distance,seq(0,150000,100))]
  setkey(envir[["FAF_DISTANCE"]],oFAFZONE,dFAFZONE)
  envir[["CALIBRATEMODECHOICE"]] <- FALSE
  envir[["CALIBRATEDISTCHANNEL"]] <- FALSE
  envir[["RUNSENSITIVITYANALYSIS"]] <- FALSE
  envir[["RUNPARAMETERS"]] <- FALSE
  
  if(USER_RUN_MODE != "Application"){
    if(USER_CALIBRATION_MODE == "Distchannel"){
      envir[["CALIBRATEDISTCHANNEL"]] <- TRUE
    } else {
      envir[["CALIBRATEMODECHOICE"]] <- TRUE
    }
  }
  
  if(USER_RUN_TEST == "Sensitivity"){
    envir[["RUNSENSITIVITYANALYSIS"]] <- TRUE
  } else if(USER_RUN_TEST == "Parameters"){
    envir[["RUNPARAMETERS"]] <- TRUE
  }
  
  if((envir[["CALIBRATEDISTCHANNEL"]] | envir[["CALIBRATEMODECHOICE"]]) & (envir[["RUNSENSITIVITYANALYSIS"]] | envir[["RUNSENSITIVITYANALYSIS"]])) stop("Please set either calibration mode or test mode off by going to _USER_Variables.R")
  
  
  
  ### Return TRUE
  gc()
  invisible(TRUE)
  
}
