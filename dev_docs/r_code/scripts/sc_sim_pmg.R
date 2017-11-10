sc_sim_pmg <- function(naics_set, ShipmentRoutesCosts){
  # browser()
  # Prepare future processors
  debugFuture <- TRUE
  source(file.path(SYSTEM_SCRIPTS_PATH,"FutureTaskProcessor.R"))
  #only allocate as many workers as we need (including one for future itself) or to the specified maximum
  plan(multiprocess, workers = USER_COST_CORES) #### Needs to be defined
  
  naicsInProcess <- list()
  
  # Prepare the inputs to PMG
  for(naics_run_number in 1:nrow(naics_set)){
    naics <- as.character(naics_set$NAICS[naics_run_number])
    groups <- naics_set$groups[naics_run_number]
    sprod <- ifelse(naics_set$Split_Prod[naics_run_number], 1, 0)
    
    # Create a log file for PMG Setup
    log_file_path <- file.path(SCENARIO_OUTPUT_PATH, paste0(naics, "_PMGSetup_Log.txt"))
    file.create(log_file_path)
    
    #create place to accumulate group results
    naicsInProcess[[paste0("naics-", naics)]] <- list()
    
    write(print(paste0(Sys.time(), ": Starting naics: ", naics, " with ", groups, " groups. sprod: ", sprod)), file = log_file_path, append = TRUE)
    
    #load the workspace for this naics code
    load(file.path(SCENARIO_OUTPUT_PATH, paste0(naics, ".Rdata")))
    # browser()
    # Assign groups to the consumers and producers
    if (!"group" %in% names(prodc)){
      conprodlist <- create_sample_groups(naics, groups, sprod, consc, prodc)
      consc <- conprodlist[[1]]
      prodc <- conprodlist[[2]]
    }
    
    # Start creating input files for pmg for each group in the naics
    for(g in 1:groups){
      
      taskName <- paste0( "Supplier_to_Buyer_Costs_makeInputs_naics-", naics, "_group-", g, "_of_", groups, "_sprod-", sprod)
      
      write(print(paste0(Sys.time(), ": Submitting task '", taskName, "' to join ", getNumberOfRunningTasks(), " currently running tasks")), file = log_file_path, append = TRUE)
      
      startAsyncTask(
        taskName, # Name of the task running
        future({ # Start the future processor
          pmgs_inputs_file_path <- file.path(SCENARIO_OUTPUT_PATH, paste0(naics, "_g", g, ".sell.csv"))
          
          if (file.exists(pmgs_inputs_file_path)) {
            file.remove(pmgs_inputs_file_path)
          }
          
          # Write message to the console
          msg <- write(print(paste0(Sys.time(), " Starting creating inputs for: ", naics, ",  Group: ", g)), file = log_file_path, append = TRUE)
          
          # Run pmgs input script
          output <- capture.output(create_inputs(naics, g, sprod, consc[group==g], prodc[group==g], ShipmentRoutesCosts))
          return(output) #no need to return anything to future task handler
        }),
        callback = function(asyncResults) {
          # asyncResults is: list(asyncTaskName,
          #                        taskResult,
          #                        startTime,
          #                        endTime,
          #                        elapsedTime,
          #                        caughtError,
          #                        caughtWarning)
          
          #check that cost files was create
          taskName <- asyncResults[["asyncTaskName"]]
          taskInfo <- data.table::data.table(namedCapture::str_match_named(taskName, 
                                                                           "^.*naics[-](?<taskNaics>[^_]+)_group-(?<taskGroup>[^_]+)_of_(?<taskGroups>[^_]+)_sprod-(?<sprod>.*)$"))[1,]
          task_log_file_path <- file.path(SCENARIO_OUTPUT_PATH, 
                                          paste0(taskInfo$taskNaics, "_PMGRun_Log.txt"))
          
          taskResult <- asyncResults[["taskResult"]]
          write(print("=================== START output captured from call to create_pmg_inputs ================================"), file = task_log_file_path, append = TRUE)
          write(print(taskResult), file = task_log_file_path, append = TRUE)
          write(print("=================== END output captured from call to create_pmg_inputs ================================"), file = task_log_file_path, append = TRUE)
          
          naicsKey <- paste0("naics-", taskInfo$taskNaics)
          groupoutputs <- naicsInProcess[[naicsKey]]
          if (is.null(groupoutputs)) {
            stop(
              paste0(
                "for taskInfo$taskNaics ",
                taskInfo$taskNaics,
                " naicsInProcess[[taskInfo$taskNaics]] (groupoutputs) is NULL! "
              )
            )
          }
          
          groupKey <- paste0("group-", taskInfo$taskGroup)
          groupoutputs[[groupKey]] <-
            paste0(Sys.time(), ": Finished!")
          
          #don't understand why this is necessary but apparently have to re-store list
          naicsInProcess[[naicsKey]] <<- groupoutputs
          
          costs_file_path <- file.path(SCENARIO_OUTPUT_PATH,
                                      paste0(taskInfo$taskNaics,"_g",
                                             taskInfo$taskGroup,".costs.csv"))
          cost_file_exists <- file.exists(costs_file_path)
          
          write(print(paste0(Sys.time(),": Finished ",taskName,
                             ", Elapsed time since submitted: ",
                             asyncResults[["elapsedTime"]],
                             ", cost_file_exists: ",cost_file_exists,
                             " # of group results so far for this naics=",
                             length(groupoutputs))),
                file = task_log_file_path,append = TRUE)
          
          if (!cost_file_exists) {
            msg <- paste("***ERROR*** Did not find expected costs file '",
                         costs_file_path,"'.")
            
            write(print(msg), file = task_log_file_path, append = TRUE)
            stop(msg)
          }
          if (length(groupoutputs) == taskInfo$taskGroups) {
            #delete naics from tracked outputs
            naicsInProcess[[naicsKey]] <<- NULL
            write(print(paste0(Sys.time(),": Completed Processing Outputs of all ",
                               taskInfo$taskGroups," groups for naics ",
                               taskInfo$taskNaics,". Remaining naicsInProcess=",
                               paste0(collapse = ", ", names(naicsInProcess)))), 
                  file = task_log_file_path, append = TRUE)
            } #end if all groups in naic are finished
        },
        debug = FALSE
      ) #end call to startAsyncTask
      processRunningTasks(wait = FALSE, debug = TRUE, maximumTasksToResolve = 1)
    }
  } # Finished making inputs to the PMG
  
  # Wait until all tasks are finished
  processRunningTasks(wait = TRUE, debug = TRUE)
  
  if (length(naicsInProcess) != 0) {
    stop(paste(
      "At end of 03_0a_Supplier_to_Buyer_Costs.R there were still some unfinished naics! Unfinished: ", 
      paste0(collapse = ", ", names(naicsInProcess))))
  }
  
  
  # Save some results if running in calibration mode
  if(CALIBRATEDISTCHANNEL){
    filelistmfg <- list.files(SYSTEM_DATA_PATH,pattern = "_g\\d+_model_distchannel_mfg",full.names = TRUE,recursive = FALSE)
    if(length(filelistmfg)>0){
      mfg_cal <- rbindlist(lapply(filelistmfg, fread, stringsAsFactors=FALSE))
      saveRDS(mfg_cal,file=file.path(SYSTEM_DATA_PATH,"model_distchannel_mfg_cal.rds"))
      mfg_calreduced <- mfg_cal[,.(COEFF=weighted.mean(COEFF,weight)),by=.(CATEGORY,CHID,CHDESC,VAR,TYPE)]
      fwrite(mfg_calreduced,file=file.path(SYSTEM_DATA_PATH,"model_distchannel_mfg_cal.csv"))
      # file.remove(filelistmfg)
    }
    filelistfood <- list.files(SYSTEM_DATA_PATH,pattern = "_g\\d_model_distchannel_food")
    if(length(filelistfood)>0){
      food_cal <- rbindlist(lapply(filelistfood, fread, stringsAsFactors=FALSE))
      saveRDS(food_cal,file=file.path(SYSTEM_DATA_PATH,"model_distchannel_mfg_cal.rds"))
      food_calreduced <- food_cal[,.(COEFF=weighted.mean(COEFF,weight)),by=.(CATEGORY,CHID,CHDESC,VAR,TYPE)]
      fwrite(food_calreduced,file=file.path(SYSTEM_DATA_PATH,"model_distchannel_food_cal.csv"))
    }
  } else if (CALIBRATEMODECHOICE){
    filelist <- list.files(SYSTEM_DATA_PATH,pattern = "_g\\d+_modechoiceconstants.rds",full.names = TRUE,recursive = FALSE)
    modeChoiceConstants <- rbindlist(lapply(filelist,readRDS))
    saveRDS(modeChoiceConstants,file = file.path(SYSTEM_DATA_PATH,"allModeChoiceConstants.rds"))
    modeChoiceConstants <- modeChoiceConstants[,.(Constant=mean(Constant)),by=.(Commodity_SCTG,ODSegment,Mode.Domestic)]
    saveRDS(modeChoiceConstants,file = file.path(SYSTEM_DATA_PATH,"ModeChoiceConstants.rds"))
  } # Finished saving the files for calibration
  
  # Save some results if running in test mode
  # Need to implement
  
  # Run the PMGs
  
  fwrite(PMGParameters[variable != "pmglogging"], file = file.path(SYSTEM_PMG_PATH,"PMG.ini"), 
         sep = "=", row.names = FALSE, col.names = FALSE)
  
  plan(multiprocess, workers = USER_PMG_CORES)
  
  naicsInProcess <- list()
  
  for(naics_run_number in 1:nrow(naics_set)){
    naics <- naics_set$NAICS[naics_run_number]
    groups <- naics_set$groups[naics_run_number]
    sprod <- ifelse(naics_set$Split_Prod[naics_run_number], 1, 0)
    
    log_file_path <- file.path(SCENARIO_OUTPUT_PATH, paste0(naics, "_PMGRun_Log.txt"))
    file.create(log_file_path)
    
    write(print(paste0(Sys.time(), ": Starting naics: ", naics, " with ",
                       groups, " groups. sprod: ", sprod)),file = log_file_path,
          append = TRUE)
    
    naicsInProcess[[paste0("naics-",naics)]] <- list() #create place to accumulate group results
    
    #loop over the groups and run the games, and process outputs
    for (g in 1:groups) {
      taskName <- paste0("RunPMG_naics-", naics, "_group-", g, "_of_", groups,
                         "_sprod-", sprod)
      
      write(print(paste0(Sys.time(), ": Submitting task '", taskName, "' to join ",
                         getNumberOfRunningTasks(), " currently running tasks")), 
            file = log_file_path, append = TRUE)
      
      startAsyncTask(taskName,
                     future({
                       #call to runPMG to run the game
                       runPMG(naics, g, writelog = ifelse(PMGParameters[variable=="pmglogging",value==1],TRUE,FALSE), wait = TRUE, pmgexe = file.path(SYSTEM_PMG_PATH,"pmg.exe"),
                              inipath = file.path(SYSTEM_PMG_PATH,"PMG.ini"), 
                              inpath = SCENARIO_OUTPUT_PATH, outpath = SCENARIO_OUTPUT_PATH,
                              logpath = SCENARIO_OUTPUT_PATH)
        }),
        callback = function(asyncResults) {
          # asyncResults is: list(asyncTaskName,
          #                        taskResult,
          #                        startTime,
          #                        endTime,
          #                        elapsedTime,
          #                        caughtError,
          #                        caughtWarning)
          
          taskName <- asyncResults[["asyncTaskName"]]
          
          taskInfo <- data.table::data.table(namedCapture::str_match_named(
            taskName, "^.*naics[-](?<taskNaics>[^_]+)_group-(?<taskGroup>[^_]+)_of_(?<taskGroups>[^_]+)_sprod-(?<sprod>.*)$"))[1, ]
          
          task_log_file_path <- file.path(SCENARIO_OUTPUT_PATH, paste0(taskInfo$taskNaics, "_PMGRun_Log.txt"))
          
          write(print(paste0(Sys.time(), ": task '", taskName, 
                             "' finished. Elapsed time since submitted: ", 
                             asyncResults[["elapsedTime"]])), 
                file = task_log_file_path, append = TRUE)
          
          expectedOutputFile <- file.path(SCENARIO_OUTPUT_PATH, 
                                          paste0(taskInfo$taskNaics, 
                                                 "_g", taskInfo$taskGroup, ".out.csv"))
          if (!file.exists(expectedOutputFile)) {
            msg <- paste0(Sys.time(), ": Expected PMG output file '", expectedOutputFile,
                          "' does not exist!")
            write(print(msg), file = task_log_file_path, append = TRUE)
            stop(msg)
          }
          
          pmgout <- fread(expectedOutputFile)
          
          setnames(pmgout, c("BuyerId", "SellerId"), c("BuyerID", "SellerID"))
          
          #get just the results from the final iteration
          
          pmgout <- pmgout[Last.Iteration.Quantity > 0]
          
          #apply fix for bit64/data.table handling of large integers in rbindlist
          
          pmgout[, Quantity.Traded := as.character(Quantity.Traded)]
          
          pmgout[, Last.Iteration.Quantity := as.character(Last.Iteration.Quantity)]
          
          load(file.path(SCENARIO_OUTPUT_PATH, paste0(taskInfo$taskNaics, "_g", taskInfo$taskGroup, ".Rdata")))
          
          naicsKey <- paste0("naics-",taskInfo$taskNaics)
          groupoutputs <- naicsInProcess[[naicsKey]]
          groupKey <- paste0("group-",taskInfo$taskGroup)
          groupoutputs[[groupKey]] <- merge(pc, pmgout, by = c("BuyerID", "SellerID"))
          
          #don't understand why this is necessary but apparently have to re-store list
          naicsInProcess[[naicsKey]] <<- groupoutputs
          
          rm(pmgout, pc)
          write(print(paste0(Sys.time(), ": Deleting Inputs: ", taskInfo$taskNaics,
                             " Group: ", taskInfo$taskGroup)), 
                file=task_log_file_path, append=TRUE)
          
          file.remove(file.path(SCENARIO_OUTPUT_PATH, paste0(taskInfo$taskNaics, "_g", taskInfo$taskGroup, ".costs.csv")))
          file.remove(file.path(SCENARIO_OUTPUT_PATH, paste0(taskInfo$taskNaics, "_g", taskInfo$taskGroup, ".buy.csv")))
          file.remove(file.path(SCENARIO_OUTPUT_PATH, paste0(taskInfo$taskNaics, "_g", taskInfo$taskGroup, ".sell.csv")))
          
          if (length(groupoutputs) == taskInfo$taskGroups) {
            #convert output list to one table, add to workspace, and save
            #apply fix for bit64/data.table handling of large integers in rbindlist
            naicsRDataFile <- file.path(SCENARIO_OUTPUT_PATH, paste0(taskInfo$taskNaics, ".Rdata"))
            load(naicsRDataFile)
            pairs <- rbindlist(groupoutputs)
            pairs[, Quantity.Traded := as.integer64.character(Quantity.Traded)]
            
            pairs[, Last.Iteration.Quantity := as.integer64.character(Last.Iteration.Quantity)]
            
            write(print(paste0(Sys.time(), ": loaded and then re-saved with'", naicsRDataFile,
                               "'", " nrow(consc)=", nrow(consc), " nrow(prodc)=", nrow(prodc),
                               " nrow(pairs)=", nrow(pairs))))
            
            save(consc, prodc, pairs, file = naicsRDataFile)
            rm(consc, prodc, pairs)
            
            #delete naics from tracked outputs
            naicsInProcess[[naicsKey]] <<- NULL
            
            write(print(paste0(Sys.time(), ": Completed Processing Outputs of all ", 
                               taskInfo$taskGroups, " groups for naics ", taskInfo$taskNaics, 
                               ". Remaining naicsInProcess=", 
                               paste0(collapse=", ", names(naicsInProcess)))), 
                  file = task_log_file_path, append = TRUE)
          } #end if all groups in naics are finished
        },
        #end callback
        debug = FALSE
      ) #end call to startAsyncTask
      processRunningTasks(wait = FALSE, debug = TRUE, maximumTasksToResolve=1)
    }#end loop over groups
  }
  
  # Wait until all tasks are finished
  processRunningTasks(wait = TRUE, debug = TRUE)
  
  if (length(naicsInProcess) != 0) {
    stop(paste(
      "At end of Run_PMG there were still some unfinished naics! Unfinished: ", 
      paste0(collapse = ", ", names(naicsInProcess))))
  }
  
  
  # Save some results if running in test mode
  # Need to implement
  
  # Combine all the outputs
  
  naicspairs <- list() #list to hold the summarized outputs
  
  for (naics_run_number in 1:nrow(naics_set)) {
    
    naics <- naics_set$NAICS[naics_run_number]
    
    #check if the next naics market is ready
    naicsLogPath <- file.path(SCENARIO_OUTPUT_PATH, paste0(naics, "_PMGRun_Log.txt"))
    if (!file.exists(naicsLogPath)) {
      stop(paste0("Expected file does not exist: '", naicsLogPath, "'!"))
    }
    naicslog <- readLines(naicsLogPath)
    if(length(naicslog) == 0) {
      stop(paste0("'", naicsLogPath, "' has zero lines!"))
    }
    lastLineOfLog <- naicslog[length(naicslog)]
    targetPhrase <- "Completed Processing Outputs"
    if (!grepl(targetPhrase, lastLineOfLog, fixed = TRUE)) {
      stop(paste0("last line of '", naicsLogPath, "' does not contain '",
                  targetPhrase, "' but is instead: '", lastLineOfLog, 
                  "'!"))
    }
    
    print(paste0("Reading Outputs from Industry ", naics_run_number, ": ", 
                 naics))
    load(file.path(SCENARIO_OUTPUT_PATH, paste0(naics, ".Rdata")))
    #apply fix for bit64/data.table handling of large integers in rbindlist
    pairs[, Quantity.Traded := as.character(Quantity.Traded)]
    pairs[, Last.Iteration.Quantity := as.character(Last.Iteration.Quantity)]
    #Temporary addition. Think of a permanent fix
    pairs[FAF4.supplier==FAF4.buyer,ODSegment:="I"]
    pairs[FAF4.supplier!=FAF4.buyer,ODSegment:="X"]
    pairs[,c("emple49", "emp50t199", "empge200", "mfgind", "trwind", "whind", "Seller.Size", "Buyer.Size","Buyer.NAICS2","Seller.NAICS2","lssbd","k") := NULL]
    naicspairs[[naics_run_number]] <- pairs
  } #end for (naics_run_number in 1:nrow(naics_set))
  
  pairs <- rbindlist(naicspairs)
  rm(naicspairs)
  gc()
  #apply fix for bit64/data.table handling of large integers in rbindlist
  pairs[, Quantity.Traded := as.integer64.character(Quantity.Traded)]
  pairs[, Last.Iteration.Quantity := as.integer64.character(Last.Iteration.Quantity)]
  
  pairs[, AnnualValue := (Last.Iteration.Quantity / PurchaseAmountTons) *
          ConVal]
  
  setkey(pairs, Production_zone, Consumption_zone, MinPath)
  
  # Stop the future processors
  future:::ClusterRegistry("stop")
  
  return(pairs)
}


# Working on this
create_sample_groups <- function(naics, groups, sprod, consc, prodc){
  # Assign group number to producers and consumers
  consc[InputCommodity==naics,numgroups:=groups]
  prodc[NAICS==naics,numgroups:=groups]
  suppressWarnings(consc[!is.na(numgroups),group:=1:numgroups,.(NAICS)])
  suppressWarnings(prodc[!is.na(numgroups),group:=1:numgroups,.(NAICS)])
  
  #Check that for all groups the capacity > demand
  #to much demand overall?
  # prodconsratio <- sum(producers$OutputCapacityTons,na.rm = TRUE)/sum(consumers$PurchaseAmountTons,na.rm = TRUE)
  return(list(consc,prodc))
  
}

create_inputs <- function(naics, g, sprod, consc, prodc, ShipmentRoutesCosts){
  
  ####Notes: Things that are missing:
  ### OutputCommodity in consumer data is missing
  
  #All consumers for this group write PMG input and create table for merging
  fwrite(consc[,.(InputCommodity,BuyerID,FAF4,Zone=TAZ1,NAICS,Size,OutputCommodity=NAICS,PurchaseAmountTons,PrefWeight1_UnitCost,PrefWeight2_ShipTime,SingleSourceMaxFraction)], file = file.path(SCENARIO_OUTPUT_PATH,paste0(naics, "_g", g, ".buy.csv")))
  
  conscg <- consc[,.(InputCommodity,SCTG,NAICS,FAF4,Zone=TAZ1,BuyerID,Size,PurchaseAmountTons,ConVal)]
  
  print(paste(Sys.time(), "Finished writing buy file for ",naics,"group",g))
  
  #If splitting producers, write out each group and else write all with output capacity reduced
  if(sprod==1){
    
    fwrite(prodc[,.(OutputCommodity,SellerID,FAF4,Zone=TAZ1,NAICS,Size,OutputCapacityTons,NonTransportUnitCost)], file = file.path(SCENARIO_OUTPUT_PATH,paste0(naics, "_g", g, ".sell.csv")))
    
    prodcg <- prodc[,.(OutputCommodity,NAICS,SCTG,SellerID,Size,FAF4,Zone=TAZ1,OutputCapacityTons)]
    
  } else {
    
    #reduce capacity based on demand in this group
    
    consamount <- sum(conscg$PurchaseAmountTons)/sum(consc$PurchaseAmountTons)
    
    prodc[,OutputCapacityTonsG:= OutputCapacityTons * consamount]
    
    fwrite(prodc[,.(OutputCommodity,SellerID,FAF4,Zone=TAZ1,NAICS,Size,OutputCapacityTons=OutputCapacityTonsG,NonTransportUnitCost)], file = file.path(SCENARIO_OUTPUT_PATH,paste0(naics, "_g", g, ".sell.csv")))
    
    prodcg <- prodc[,.(OutputCommodity,NAICS,SCTG,SellerID,Size,FAF4,Zone=TAZ1,OutputCapacityTons=OutputCapacityTonsG)]
  }
  
  print(paste(Sys.time(), "Applying distribution, shipment, and mode-path models to",naics,"group",g))
  
  # Rename ready to merge
  
  setnames(conscg, c("InputCommodity", "NAICS", "Zone","Size"), c("NAICS", "Buyer.NAICS", "Consumption_zone","Buyer.Size"))
  
  setnames(prodcg, c("OutputCommodity", "NAICS", "Zone","Size"),c("NAICS", "Seller.NAICS", "Production_zone","Seller.Size"))
  
  pc <- sample_prod_cons(conscg, prodcg)
  
  pc <- pc_sim_distchannel(pc, distchannel_food = distchannel_food, distchannel_mfg = distchannel_mfg)
  
  ### Prepare for mode choice model
  size_per_row <- 12010 #bytes
  npct <- as.numeric(pc[,.N])
  nShipSizet <- as.numeric(shipsize[SCTG %in% pc[,unique(SCTG)],.N])
  n_splits <- ceiling(npct*nShipSizet*size_per_row/(AVAILABLE_RAM/USER_COST_CORES))
  suppressWarnings(pc[,':='(n_split=1:n_splits,k=1)])
  
  # Mode choice
  
  findMinLogisticsCost <- function(split_number,ShipmentRoutesCosts,modeChoiceConstants=NULL){
    pc_split <- pc[n_split==split_number]
    pc_split <- merge(pc_split,shipsize[,.(SCTG,weight=meanWeight)],by=c("SCTG"),allow.cartesian=TRUE,all.x=TRUE)
    setkey(pc_split, Production_zone, Consumption_zone)
    if(!is.null(modeChoiceConstants)){
      df_fin <- minLogisticsCost(pc_split, ShipmentRoutesCosts, modeChoiceConstants = modeChoiceConstants)
    } else {
      df_fin <- minLogisticsCost(pc_split, ShipmentRoutesCosts, modeChoiceConstants=NULL)
    }
    setnames(df_fin,c("time","path","minc"),c("Attribute2_ShipTime","MinPath","MinGmnql"))
    commonNames <- intersect(colnames(pc_split),colnames(df_fin))
    return(pc_split[df_fin,on=commonNames])
  }
  
  # pc[FAF4.buyer==FAF4.supplier,ODSegment:="I"]
  # pc[FAF4.buyer!=FAF4.supplier,ODSegment:="X"]
  iter <- 1
  
  if(CALIBRATEMODECHOICE){
    iter <- 4
    ModeChoiceConstants <- mode_description[][,k:=1][data.table(SCTG=unique(pc$SCTG))[,k:=1],on="k"][,k:=NULL]
    ModeChoiceConstants[,Constant:=0]
  }
  
  for(iters in iter){
    if(exists("ModeChoiceConstants")){
      df_min <- rbindlist(lapply(1:n_splits,findMinLogisticsCost,ShipmentRoutesCosts,ModeChoiceConstants))
    } else {
      df_min <- rbindlist(lapply(1:n_splits,findMinLogisticsCost,ShipmentRoutesCosts))
    }
    if(iter > 1){
      # Do something with targets here
    }
  }
  
  pc <- df_min[,c("n_split","Mode.Domestic","CATEGORY","Distance","Distance_Bin","avail","Constant","Prob","MinCost"):=NULL]
  gc()
  if(CALIBRATEMODECHOICE){
    saveRDS(ModeChoiceConstants[,':='(NAICS=naics,Group=g)], file=file.path(SCENARIO_INPUT_PATH, paste0(naics,"_g",g,"_","modechoiceconstants.rds")))
  }
  
  setkey(pc,Production_zone,Consumption_zone)	### return to original sort order
  pc[, Attribute1_UnitCost := MinGmnql / PurchaseAmountTons]
  pc[, Attribute2_ShipTime := Attribute2_ShipTime / 24] #Convert from hours to days
  
  save(pc, file = file.path(SCENARIO_OUTPUT_PATH,paste0(unique(pc$NAICS), "_g", g, ".Rdata")))
  
  pc[, c("Production_zone", "Consumption_zone", "SCTG", "NAICS","Seller.NAICS", "Seller.Size", "Buyer.NAICS","Buyer.Size","ConVal","PurchaseAmountTons", "OutputCapacityTons","weight", "distchannel","lssbd", "MinGmnql", "MinPath","FAF4.supplier","FAF4.buyer", "Buyer.NAICS2","Seller.NAICS2") := NULL]
  
  fwrite(pc, file = file.path(SCENARIO_OUTPUT_PATH,paste0(naics, "_g", g, ".costs.csv")))
  
  
  if(file.exists(file.path(SCENARIO_OUTPUT_PATH,paste0(naics,"_g", g, ".out.csv")))){
    
    file.remove(file.path(SCENARIO_OUTPUT_PATH,paste0(naics,"_g", g, ".out.csv")))
    
  }
  
  ## -- Delete NAICS_gX.txt file if exists from prior run as well
  
  if(file.exists(file.path(SCENARIO_OUTPUT_PATH,paste0(naics,"_g", g, ".txt")))){
    
    file.remove(file.path(SCENARIO_OUTPUT_PATH,paste0(naics,"_g", g, ".txt")))
    
  }
  
  rm(pc)
  gc()
  return(TRUE)
}


sample_prod_cons <- function(conscg,prodcg){
  ###Note: Need to define:
  ## AVAILABLE_RAM
  ## FAF_DISTANCE
  ## Supplier_Selection_Distribution
  ## NSupplierPerBuyer
  set.seed(151)
  n_splits <- 1L
  nconst <- as.numeric(conscg[,.N])
  nprodt <- as.numeric(prodcg[,.N])
  SIZE_PER_ROW <- 104
  RAM_TO_USE <- 0.85*(AVAILABLE_RAM) # Approximately 85% of available RAM
  n_splits <- ceiling(nconst*nprodt*SIZE_PER_ROW/(RAM_TO_USE/(USER_COST_CORES)))
  suppressWarnings(conscg[,':=' (n_split=1:n_splits,DoSample=TRUE)])
  prodcg[,AvailableForSample:=TRUE]
  
  ### Temporary solution to NA in purchaseamounts and outputcapacitytons
  prodcg <- prodcg[!is.na(OutputCapacityTons)]
  conscg <- conscg[!is.na(PurchaseAmountTons)]
  ### Temporary solution ends here.
  
  sample_data <- function(split_number,fractionOfSupplierperBuyer=1.0,samplingforSeller=FALSE){
    pc_split <- merge(prodcg[AvailableForSample==TRUE][,AvailableForSample:=NULL],conscg[n_split==split_number & DoSample==TRUE][,c("n_split","DoSample"):=NULL],by = c("NAICS","SCTG"), allow.cartesian = TRUE,suffixes = c(".supplier",".buyer"))
    pc_split <- pc_split[Production_zone<=273 | Consumption_zone<=273]	### -- Heither, 10-14-2015: potential foreign flows must have one end in U.S. so drop foreign-foreign
    if((pc_split[,.N]>0)){
      # pc_split <- conscg[pc_split,.(BuyerID,weights=seq(5,nSupplierPerBuyer,length.out=4)[findInterval(prop.table(PurchaseAmountTons),quantile(prop.table(PurchaseAmountTons),c(0.25,0.5,0.75)))]),on="BuyerID"]
      pc_split[,Distance_Bin:=FAF_DISTANCE[.(FAF4.supplier,FAF4.buyer),Distance_Bin]]
      pc_split[Supplier_Selection_Distribution,Proportion:=i.Proportion, on = .(SCTG,Distance_Bin==DistanceGroup)]
      pc_split[,RemainingProp:=max(1e-12,1-sum(Proportion,na.rm=TRUE)),by=.(BuyerID,SCTG)]
      pc_split[is.na(Proportion),Proportion:=(RemainingProp/.N),by=.(BuyerID,SCTG,Distance_Bin)]
      pc_split[,RemainingProp:=NULL]
      pc_split[,BSRatio:=OutputCapacityTons/PurchaseAmountTons]
      pc_split[,Proportion:=Proportion*(BSRatio/sum(BSRatio,na.rm=TRUE)),by=.(BuyerID,SCTG,Distance_Bin)]
      pc_split[,BSRatio:=NULL]
      min_proportion <- pc_split[Proportion>0,min(Proportion)]*1E-3
      pc_split[,Proportion:=Proportion+min_proportion]
      pc_split[conscg[n_split==split_number & DoSample==TRUE,.(BuyerID,weights=seq(5,NSupplierPerBuyer,length.out=4)[findInterval(PurchaseAmountTons,quantile(PurchaseAmountTons,c(0.25,0.5,0.75)))+1])],weights:=i.weights,on="BuyerID"]
      resample <- TRUE # Check to see all the sellers can meet the buyers demand.
      sampleIter <- 1
      while(resample & sampleIter<6) {
        buyersWithMultipleSellers <- pc_split[,.N,by=BuyerID]
        if(buyersWithMultipleSellers[N==1,.N]>0){
          sampled_pairs <- rbindlist(list(if(pc_split[BuyerID %in% buyersWithMultipleSellers[N>1,BuyerID],length(BuyerID)]>0) pc_split[BuyerID %in% buyersWithMultipleSellers[N>1,BuyerID], .(SellerID = sample(SellerID, min(.N, ceiling(weights*fractionOfSupplierperBuyer)), replace = FALSE, prob = Proportion)), by = .(BuyerID)],pc_split[BuyerID %in% buyersWithMultipleSellers[N==1,BuyerID],.(BuyerID,SellerID)]))
        } else {
          sampled_pairs <- pc_split[, .(SellerID = sample(SellerID, min(.N, ceiling(weights*fractionOfSupplierperBuyer)), replace = FALSE, prob = Proportion)), by = .(BuyerID)]
        }
        setkey(sampled_pairs, BuyerID, SellerID)
        resample <- pc_split[sampled_pairs,on=c("BuyerID","SellerID")][,sum(OutputCapacityTons)<unique(PurchaseAmountTons),by=.(BuyerID)][,sum(V1)] > ceiling(0.01*length(pc_split[,unique(BuyerID)])) # Make sure that the buyers demand could be fulfilled.
        resample  <- resample&!samplingforSeller
        # pc_split_partial <- pc_split[sampled_pairs,on=c("BuyerID","SellerID")]
        # resample <- pc_split_partial[,sum(OutputCapacityTons)/.N,by=SellerID][,sum(V1)]<pc_split_partial[,sum(PurchaseAmountTons)/.N,by=BuyerID][,sum(V1)]
        sampleIter <- sampleIter+1
      }
      # print(paste0("Number of sampling iterations for Sellers: ",sampleIter-1))
      # print(sprintf("Split Number: %d/%d",split_number,n_splits))
      # print(sprintf("Number of pc_split rows: %d", pc_split[,.N]))
      # print(paste0(object.size(pc_split)/(1024**3)," Gb"))
      # print(paste0(object.size(pc_split[sampled_pairs,on=c("BuyerID","SellerID")])/(1024**3)," Gb"))
      return(pc_split[sampled_pairs,on=c("BuyerID","SellerID")][,c("Proportion","weights"):=NULL])
    } else {
      return(NULL)
    }
  }
  
  pc <- rbindlist(lapply(1:n_splits,sample_data))
  
  # Sample again to verify that the production meets all the consumption
  resampleBuyers <- TRUE # Check to make sure that all buyers can meet the sellers Capacity.
  sampleBuyersIter <- 1L
  sellersMaxedOut <- pc[,sum(PurchaseAmountTons),by=.(SellerID,OutputCapacityTons)][V1>OutputCapacityTons,SellerID]
  buyersOfSellers <- pc[SellerID %in% sellersMaxedOut,unique(BuyerID)]
  resampleBuyers <- (length(sellersMaxedOut)>0)
  conscg[!(BuyerID %in% buyersOfSellers), DoSample:=FALSE]
  sellDifference <- as.numeric(pc[SellerID%in%sellersMaxedOut,sum(PurchaseAmountTons),by=.(SellerID,OutputCapacityTons)][,sum(V1-OutputCapacityTons)])
  print(paste0("Original Number of Combinations: ", pc[,.N]))
  limitBuyersResampling <- 10
  
  ## While the condition of production not meeting the consumption is satisfied repeat the sampling process
  while(resampleBuyers & (sampleBuyersIter<(limitBuyersResampling+1)) & (prodcg[AvailableForSample==TRUE,.N]>0)){
    new_pc <- rbindlist(lapply(1:n_splits,sample_data,fractionOfSupplierperBuyer=1.0,samplingforSeller=resampleBuyers)) #(limitBuyersResampling/model$scenvars$nSuppliersPerBuyer)
    validIndex <- pc[new_pc[,.(BuyerID,SellerID,SCTG)],.I[is.na(NAICS)],on=c("BuyerID","SellerID","SCTG")]
    pc <- rbindlist(list(pc,new_pc[validIndex]))
    rm(new_pc)
    sellersMaxedOut <- pc[,sum(PurchaseAmountTons),by=.(SellerID,OutputCapacityTons)][V1>OutputCapacityTons,SellerID]
    buyersOfSellers <- pc[SellerID %in% sellersMaxedOut,unique(BuyerID)]
    new_sellDifference <- as.numeric(pc[SellerID%in%sellersMaxedOut,sum(PurchaseAmountTons),by=.(SellerID,OutputCapacityTons)][,sum(V1-OutputCapacityTons)])
    resampleBuyers <- (new_sellDifference < (2*sellDifference)) # Increased in the maxed out capacity of the sellers should be at least 10%.
    # prodcg[SellerID %in% sellersMaxedOut,availableForSample:=FALSE]
    conscg[!(BuyerID %in% buyersOfSellers), DoSample:=FALSE]
    sampleBuyersIter <- sampleBuyersIter + 1
    print(paste0("Number of Combinations after ", sampleBuyersIter-1, " iteration: ",pc[,.N]))
  }
  
  return(pc)
}


pc_sim_distchannel <- function(pc, distchannel_food, distchannel_mfg, calibration = NULL){
  # Define distchannel_food
  # Define distchannel_mfg
  
  
  ### Create variables used in the distribution channel model
  # Create a new table so the preceding table isn't modified
  pcFlows <- copy(pc)
  
  # Create employment and industry dummy variables
  pcFlows[, c("emple49", "emp50t199", "empge200", "mfgind", "trwind", "whind") := 0L]
  pcFlows[Buyer.Size <= 49, emple49 := 1]
  pcFlows[Buyer.Size >= 50 & Buyer.Size <= 199, emp50t199 := 1]
  pcFlows[Buyer.Size >= 200, empge200 := 1]
  
  pcFlows[,Seller.NAICS2:=substr(Seller.NAICS,1,2)]
  pcFlows[Seller.NAICS2 %in% 31:33, mfgind := 1]
  pcFlows[Seller.NAICS2 %in% 48:49, trwind := 1]
  pcFlows[Seller.NAICS2 %in% c(42, 48, 49), whind := 1]
  
  pcFlows[,Buyer.NAICS2:=substr(Buyer.NAICS,1,2)]
  pcFlows[Buyer.NAICS2 %in% 31:33, mfgind := 1]
  pcFlows[Buyer.NAICS2 %in% 48:49, trwind := 1]
  pcFlows[Buyer.NAICS2 %in% c(42, 48, 49), whind := 1]
  
  
  # Add the FAME SCTG category for comparison with calibration targets
  pcFlows[, CATEGORY := famesctg[SCTG]]
  setkey(pcFlows, Production_zone, Consumption_zone)
  
  # Add zone to zone distances
  pcFlows <- merge(pcFlows, mesozone_gcd, c("Production_zone", "Consumption_zone")) # append distances
  setnames(pcFlows, "GCD", "Distance")
  
  # Update progress log
  # Missing for now
  
  
  print(paste(Sys.time(), "Applying distribution channel model"))
  
  #Apply choice model of distribution channel and iteratively adjust the ascs
  
  #The model estimated for mfg products was applied to all other SCTG commodities
  
  
  inNumber <- nrow(pc[SCTG %in% c(1:9)])
  
  if (inNumber > 0) {
    
    # Sort on vars so simulated choice is ordered correctly
    model_vars_food <- c("CATEGORY", distchannel_food[TYPE == "Variable", unique(VAR)])
    model_ascs_food <- distchannel_food[TYPE == "Constant", unique(VAR)]
    setkeyv(pcFlows, model_vars_food) #sorted on vars, calibration coefficients, so simulated choice is ordered correctly
    
    pcFlows_food <- pcFlows[SCTG %in% c(1:9),model_vars_food,with=FALSE]
    pcFlows_food_weight <- pcFlows[SCTG %in% 1:9,PurchaseAmountTons]
    
    df <- pcFlows_food[, list(Start = min(.I), Fin = max(.I)), by = model_vars_food] #unique combinations of model coefficients
    
    df[, (model_ascs_food) := 1] #add 1s for constants to each group in df
    
    print(paste(Sys.time(), nrow(df), "unique combinations"))
    
    if(!is.null(calibration)){
      
      pcFlows[SCTG %in% c(1:9), distchannel := predict_logit(df, distchannel_food, cal = distchan_cal, calcats = distchan_calcats, weight = pcFlows_food_weight, path = file.path(model$inputdir,paste0(naics,"_g",g,"_model_distchannel_food_cal.csv")), iter = 4)]
      
    } else {
      
      pcFlows[SCTG %in% 1:9, distchannel := predict_logit(df, distchannel_food,cal=distchan_cal,calcats=distchan_calcats)]
      
    }
    
  }
  
  # Update progress log
  
  
  print(paste(Sys.time(), "Finished ", inNumber, " for SCTG %in% c(1:9)"))
  
  ### Apply choice model of distribution channel for other industries
  
  # The model estimated for mfg products is applied to all other SCTG commoditie
  
  outNumber <- nrow(pcFlows[!SCTG %in% c(1:9)])
  
  if (outNumber > 0) {
    
    # Sort on vars so simulated choice is ordered correctly
    model_vars_mfg <- c("CATEGORY", distchannel_mfg[TYPE == "Variable", unique(VAR)])
    model_ascs_mfg <- distchannel_mfg[TYPE == "Constant", unique(VAR)]
    
    setkeyv(pcFlows, model_vars_mfg) #sorted on vars so simulated choice is ordered correctly
    
    pcFlows_mfg <- pcFlows[!SCTG %in% c(1:9),model_vars_mfg,with=FALSE]
    pcFlows_mfg_weight <- pcFlows[!SCTG %in% c(1:9),PurchaseAmountTons]
    
    df <- pcFlows_mfg[, list(Start = min(.I), Fin = max(.I)), by = model_vars_mfg] #unique combinations of model coefficients
    
    ####do this in the function (seems unecessary here)?
    
    df[, (model_ascs_mfg) := 1] #add 1s for constants to each group in df
    
    print(paste(Sys.time(), nrow(df), "unique combinations"))
    
    # Simulate choice -- with calibration if calibration targets provided
    if(!is.null(calibration)){
      
      pcFlows[!SCTG %in% c(1:9), distchannel := predict_logit(df, distchannel_mfg, cal = distchannel_calibration, calcats = distchan_calcats, weight = pcFlows_mfg_weight, path = file.path(model$inputdir,paste0(naics,"_g",g,"_model_distchannel_mfg_cal.csv")), iter=4)]
      
    } else {
      
      pcFlows[!SCTG %in% 1:9, distchannel := predict_logit(df, distchannel_mfg,cal = distchannel_calibration,calcats = distchan_calcats)]
      
    }
    
  }
  
  rm(df)
  print(paste(Sys.time(), "Finished ", outNumber, " for !SCTG %in% c(1:9)"))
  
  
  # Update progress log
  setkey(pcFlows, Production_zone, Consumption_zone)
  
  pcFlows[Seller.Size>5 & Buyer.Size < 3 & Distance > 300, lssbd:= 1]
  pcFlows[!(Seller.Size>5 & Buyer.Size < 3 & Distance > 300),lssbd:=0]
  return(pcFlows)
  
  
}

minLogisticsCost <- function(BuyerSupplierPairs, ShipmentRoutesCosts, modeChoiceConstants=NULL, runmode = 0){
  
  
  
  pass <- 0			### counter for number of passes through function
  
  for (iSCTG in unique(BuyerSupplierPairs$SCTG)){
    
    if(runmode==0)	{
      
      #Direct (Limited to US Domestic) (even if flagged as indirect): 4 direct mode-paths in the direct path choiceset: c(1,2)
      
      DirectPairs <- BuyerSupplierPairs[(distchannel==1) & !((Production_zone>9999 | Consumption_zone>9999)) & SCTG==iSCTG]
      if(!is.null(modeChoiceConstants) & nrow(DirectPairs) > 0){
        DirectPairs <- minLogisticsCostSctgPaths(DirectPairs,iSCTG,c(1,2), ShipmentRoutesCosts,  modeChoiceConstants = modeChoiceConstants)
      } else if(nrow(DirectPairs) > 0) {
        DirectPairs <- minLogisticsCostSctgPaths(DirectPairs,iSCTG,c(1,2), ShipmentRoutesCosts, modeChoiceConstants = NULL)
      } else {
        DirectPairs <- DirectPairs[BuyerID < 0]
      }
      
      if(nrow(DirectPairs)>0) {
        if(pass==0) {
          df_out <- copy(DirectPairs)			### make an actual copy, not just a reference to df2
          pass <- pass + 1
        } else {
          df_out <- rbind(df_out,DirectPairs)
        }
      }
      
      
      
      #Indirect and International: 50 indirect mode-paths in the path choiceset: c(3:21)
      
      
      
      IndirectPairs <- BuyerSupplierPairs[(distchannel > 1) & ((Production_zone>9999 | Consumption_zone>9999)) & SCTG==iSCTG]
      
      if(!is.null(modeChoiceConstants) & nrow(IndirectPairs) > 0){
        IndirectPairs <- minLogisticsCostSctgPaths(IndirectPairs,iSCTG,c(3:21), ShipmentRoutesCosts, modeChoiceConstants=modeChoiceConstants)
      } else if (nrow(IndirectPairs) > 0) {
        IndirectPairs <- minLogisticsCostSctgPaths(IndirectPairs,iSCTG,c(3:21), ShipmentRoutesCosts, modeChoiceConstants=NULL)
      } else {
        IndirectPairs <- IndirectPairs[BuyerID < 0]
      }
      
      if(nrow(IndirectPairs)>0) {
        if(pass==0) {
          df_out <- copy(IndirectPairs)
          pass <- pass + 1
          } else {
            df_out <- rbind(df_out,IndirectPairs)
          }
      }
      
    } else {
      
      ###### Runmode!=0 - use for shipments between domestic zone and domestic port --
      
      DomesticShipmentPairs <- BuyerSupplierPairs[SCTG==iSCTG]
      if(is.null(modeChoiceConstants) & nrow(DomesticShipmentPairs) > 0){
        DomesticShipmentPairs <- minLogisticsCostSctgPaths(DomesticShipmentPairs,iSCTG,c(4:7,15:21), ShipmentRoutesCosts, modeChoiceConstants = NULL)			## include inland water
      } else if(nrow(DomesticShipmentPairs) > 0) {
        DomesticShipmentPairs <- minLogisticsCostSctgPaths(DomesticShipmentPairs,iSCTG,c(4:7,15:21), ShipmentRoutesCosts, modeChoiceConstants = modeChoiceConstants)
      } else {
        DomesticShipmentPairs <- DomesticShipmentPairs[BuyerID < 0]
      }
      if(nrow(DomesticShipmentPairs)>0) {if(pass==0) {
        
        df_out <- copy(DomesticShipmentPairs)
        
        pass <- pass + 1
        
      } else {
        
        df_out <- rbind(df_out,DomesticShipmentPairs)
        
      }
        
      }
      
    }
    
  }
  
  return(df_out)
  
}

minLogisticsCostSctgPaths <- function(BuyerSupplierPairs, iSCTG, paths, ShipmentRoutesCosts, modeChoiceConstants=NULL){
  
  # callIdentifier <- paste0("minLogisticsCostSctgPaths(iSCTG=",iSCTG, ", paths=", paste0(collapse=", ", paths), " naics=", naics, ")")
  
  startTime <- Sys.time()
  
  # print(paste0(startTime, " Entering: ", callIdentifier, " nrow(dfsp)=", nrow(dfsp)))
  
  s <- sctg[iSCTG]
  
  BuyerSupplierPairs <- merge(BuyerSupplierPairs,ShipmentRoutesCosts[path %in% paths,.(Production_zone=oTAZ1,Consumption_zone=dTAZ1,Mode.Domestic,path,time,cost)], by=c("Production_zone","Consumption_zone"), all.x = TRUE, allow.cartesian = TRUE)
  
  
  CAP1Carload  <- ModeChoiceParameters[Variable=="CAP1Carload",Value]    #Capacity of 1 Full Railcar
  
  CAP1FTL      <- ModeChoiceParameters[Variable=="CAP1FTL",Value]		#Capacity of 1 Full Truckload
  
  CAP1Airplane <- ModeChoiceParameters[Variable=="CAP1Airplane",Value]	#Capacity of 1 Airplane Cargo Hold
  
  
  
  BuyerSupplierPairs[,avail:=TRUE]
  
  BuyerSupplierPairs[path %in% c(4,7:12) & weight<CAP1Carload,avail:=FALSE] #Eliminate Water and Carload from choice set if Shipment Size < 1 Rail Carload
  
  BuyerSupplierPairs[path %in% c(1,6,14) & weight<CAP1FTL,avail:=FALSE] #Eliminate FTL and FTL-IMX combinations from choice set if Shipment Size < 1 FTL
  
  # dfsp[path %in% c(32:38) & weight<CAP1FTL,avail:=FALSE] #Eliminate FTL Linehaul with FTL external drayage choice set if Shipment Size < 1 FTL (Heither, 11-06-2015)
  
  BuyerSupplierPairs[path %in% c(2,5,11,13,15,16) & weight>CAP1FTL,avail:=FALSE] #Eliminate LTL and its permutations from choice set if Shipment Size > FTL (Heither, 11-06-2015)
  
  BuyerSupplierPairs[path %in% c(3) & weight>CAP1Airplane,avail:=FALSE] #Eliminate Air from choice set if Shipment Size > Air Cargo Capacity
  
  BuyerSupplierPairs[path %in% c(8:9) & weight<(0.75*CAP1FTL),avail:=FALSE] #Eliminate Container-Direct from choice set if Shipment Size < 1 40' Container
  
  BuyerSupplierPairs[path %in% c(10:11) & weight<CAP1FTL,avail:=FALSE] #Eliminate International Transload-Direct from choice set if Shipment Size < 1 FTL
  
  BuyerSupplierPairs[!(SCTG %in% c(16:19)) & (path %in% c(19:21)), avail:=FALSE]
  
  BuyerSupplierPairs[avail==TRUE,minc:=calcLogisticsCost(.SD[,list(PurchaseAmountTons,weight,ConVal,lssbd,time,cost)],s,unique(path)),by=path] # Faster implementation of above code
  
  BuyerSupplierPairs <- BuyerSupplierPairs[avail==TRUE & !is.na(minc)]		## limit to only viable choices before finding minimum path
  
  # Select the modes with minimum cost
  BuyerSupplierPairs <- BuyerSupplierPairs[BuyerSupplierPairs[,.I[which.min(minc)],.(BuyerID,SellerID,Mode.Domestic)][,V1]]
  
  # Convert the logistics costs to a probability, adjust using constants
  # iter <- 1 # just once through if in application
  # Add the constant to the mode choice table
  if(!is.null(modeChoiceConstants)){
    BuyerSupplierPairs[modeChoiceConstants,Constant:=i.Constant, on=c("SCTG","ODSegment","path")]
  } else {
    BuyerSupplierPairs[,Constant:=0]
  }
  
  # Calculate Probability
  BuyerSupplierPairs[, Prob:= exp(-(minc/(10**floor(log10(max(minc))))) + Constant) / sum(exp(-(minc/(10**floor(log10(max(minc))))) + Constant)), by = .(BuyerID, SellerID)]
  BuyerSupplierPairs[is.nan(Prob),Prob:=1]
  BuyerSupplierPairs[is.na(Prob),Prob:=1]
  
  #
  BuyerSupplierPairs[,MinCost:=0L]
  BuyerSupplierPairs[BuyerSupplierPairs[,.I[which.max(Prob)],by=list(SellerID,BuyerID)][,V1],MinCost:=1]
  
  
  BuyerSupplierPairs <- BuyerSupplierPairs[MinCost==1]
  gc()
  
  
  
  endTime <- Sys.time()
  
  # print(paste0(endTime, " Exiting after elapsed time ", format(endTime-startTime), ", ", callIdentifier, " nrow(dfsp)=", nrow(dfsp)))
  
  return(BuyerSupplierPairs)
  
} #minLogisticsCostSctgPaths

calcLogisticsCost <- function(dfspi,s,path){
  set.seed(151)
  setnames(dfspi,c("pounds","weight","value","lssbd","time","cost"))
  
  ## variables from model$scenvars
  
  B1   <- ModeChoiceParameters[Variable=="B1",Value]		#Constant unit per order
  
  B4   <- ModeChoiceParameters[Variable=="B4",Value]		#Storage costs per unit per year
  
  j    <- ModeChoiceParameters[Variable=="j",Value]		#Fraction of shipment that is lost/damaged
  
  sdLT <- ModeChoiceParameters[Variable=="sdLT",Value]	#Standard deviation in lead time
  
  LT_OrderTime <- ModeChoiceParameters[Variable=="LT_OrderTime",Value]	#Expected lead time for order fulfillment (to be added to in-transit time)
  
  #vars from S
  
  B2 <- s$B2
  
  B3 <- s$B3
  
  B5 <- s$B5
  
  a  <- s$a
  
  sdQ<- s$sdQ
  
  B0 <- s[[paste0("B0",path)]]
  
  ls <- s[[paste0("ls",path)]]
  
  
  
  #adjust factors for B0 and ls based on small buyer,
  
  if(s$Category=="Bulk natural resource (BNR)" & path %in% 5:12) B0 <- B0 - 0.25*B0*dfspi$lssbd
  
  if(s$Category=="Finished goods (FG)" & path %in% c(14:30,32:38)) ls <- ls - 0.25*ls*dfspi$lssbd
  
  if(s$Category=="Finished goods (FG)" & path %in% 39:45) ls <- ls - 0.5*ls*dfspi$lssbd
  
  
  
  #Calculate annual transport & logistics cost
  
  
  
  # NEW EQUATION
  
  
  
  dfspi[,minc:= B0 * runif(length(pounds)) + #changed scale of B0 -- too large for small shipment flows
          
          B1*pounds/weight +
          
          ls*(pounds/2000)*cost + #since cost is per ton
          
          B2*j*value +
          
          B3*time/24*value/365 +
          
          (B4 + B5*value/(pounds/2000))*weight/(2*2000) +
          
          (B5*(value/(pounds/2000))*a)*sqrt((LT_OrderTime+time/24)*((sdQ*pounds/2000)^2) + (pounds/2000)^2*(sdLT*LT_OrderTime)^2)]
  

  return(dfspi$minc)
  
}

# Override the predict_logit function from rFreight package as there is problem of loading the reshape package in the future processors
predict_logit <- function (df, mod, cal = NULL, calcats = NULL, iter = 1) {
  alts <- max(mod$CHID)
  ut <- diag(alts)
  ut[upper.tri(ut)] <- 1
  if (is.numeric(df$CATEGORY)) 
    df[, `:=`(CATEGORY, paste0("x", CATEGORY))]
  cats <- unique(df$CATEGORY)
  mod <- data.table(reshape::expand.grid.df(mod, data.frame(CATEGORY = cats)))
  if (is.numeric(cal$CATEGORY)) 
    cal[, `:=`(CATEGORY, paste0("x", CATEGORY))]
  for (iters in 1:iter) {
    if (iters > 1 & !is.null(cal) & !is.null(calcats)) {
      sim <- sapply(cats, function(x) tabulate(simchoice[min(df$Start[df$CATEGORY == 
                                                                        x]):max(df$Fin[df$CATEGORY == x])], nbins = alts))
      sim <- sim/colSums(sim)
      if (length(unique(calcats$CHOICE)) < length(unique(calcats$CHID))) {
        sim <- cbind(calcats, sim)
        sim <- melt(sim, id.vars = c("CHOICE", "CHID"), 
                    variable.name = "CATEGORY")
        sim <- sim[, list(MODEL = sum(value)), by = list(CHOICE, 
                                                         CATEGORY)]
        sim <- merge(sim, cal, c("CATEGORY", "CHOICE"))
        sim[, `:=`(ascadj, log(TARGET/MODEL))]
        adj <- merge(sim, calcats, "CHOICE", allow.cartesian = TRUE)[, 
                                                                     list(CATEGORY, CHID, ascadj)]
      }
      if (length(unique(calcats$CHOICE)) > length(unique(calcats$CHID))) {
        caldat <- merge(cal[CATEGORY %in% cats], calcats, 
                        "CHOICE")
        caldat <- caldat[, list(TARGET = sum(TARGET)), 
                         by = list(CATEGORY, CHID)]
        sim <- data.table(CHID = 1:nrow(sim), sim)
        sim <- melt(sim, id.vars = c("CHID"), variable.name = "CATEGORY")
        sim <- merge(sim, caldat, c("CATEGORY", "CHID"))
        sim[, `:=`(ascadj, log(TARGET/value))]
        adj <- sim[, list(CATEGORY, CHID, ascadj)]
      }
      if (length(unique(calcats$CHOICE)) == length(unique(calcats$CHID))) {
        stop("Need to implment calibration for same calcats as choice alts")
      }
      mod <- merge(mod, adj, c("CATEGORY", "CHID"))
      mod[TYPE == "Constant", `:=`(COEFF, COEFF + ascadj)]
      mod[, `:=`(ascadj, NULL)]
    }
    utils <- lapply(cats, function(y) sapply(1:alts, function(x) exp(rowSums(sweep(df[CATEGORY == 
                                                                                        y, mod[CHID == x & CATEGORY == y, VAR], with = F], 
                                                                                   2, mod[CHID == x & CATEGORY == y, COEFF], "*")))))
    utils <- lapply(1:length(cats), function(x) (utils[[x]]/rowSums(utils[[x]])) %*% 
                      ut)
    utils <- do.call("rbind", utils)
    temprand <- runif(max(df$Fin))
    simchoice <- unlist(lapply(1:nrow(df), function(x) 1L + 
                                 findInterval(temprand[df$Start[x]:df$Fin[x]], utils[x, 
                                                                                     ])))
  }
  return(simchoice)
}