sc_run_pmg_inputs <- function(naics_set){
  # Prepare future processors
  debugFuture <- TRUE
  source("./scripts/FutureTaskProcessor.R")
  #only allocate as many workers as we need (including one for future itself) or to the specified maximum
  plan(multiprocess,
       workers = model$scenvars$maxcostrscripts)
  
  naicsInProcess <- list()
  
  for(naics_run_number in 1:nrow(naics_set)){
    naics <- naics_set$NAICS[naics_run_number]
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
    
    # Assign groups to the consumers and producers
    if (!"group" %in% names(prodc)){
      sc_create_pmg_sample_groups(naics, groups, sprod)
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
          output <- capture.output(sc_create_pmg_inputs(naics, g, sprod))
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
          taskInfo <-
            data.table::data.table(
              namedCapture::str_match_named(
                taskName,
                "^.*naics[-](?<taskNaics>[^_]+)_group-(?<taskGroup>[^_]+)_of_(?<taskGroups>[^_]+)_sprod-(?<sprod>.*)$"
              )
            )[1,]
          task_log_file_path <-
            file.path(outputdir,
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
          
          costs_file_path <-
            file.path(
              outputdir,
              paste0(
                taskInfo$taskNaics,
                "_g",
                taskInfo$taskGroup,
                ".costs.csv"
              )
            )
          cost_file_exists <- file.exists(costs_file_path)
          write(print(
            paste0(
              Sys.time(),
              ": Finished ",
              taskName,
              ", Elapsed time since submitted: ",
              asyncResults[["elapsedTime"]],
              ", cost_file_exists: ",
              cost_file_exists,
              " # of group results so far for this naics=",
              length(groupoutputs)
            )
          ),
          file = task_log_file_path,
          append = TRUE)
          if (!cost_file_exists) {
            msg <- paste("***ERROR*** Did not find expected costs file '",
                         costs_file_path,
                         "'.")
            write(print(msg), file = task_log_file_path, append = TRUE)
            stop(msg)
          }
          if (length(groupoutputs) == taskInfo$taskGroups) {
            #delete naic from tracked outputs
            naicsInProcess[[naicsKey]] <<- NULL
            write(print(
              paste0(
                Sys.time(),
                ": Completed Processing Outputs of all ",
                taskInfo$taskGroups,
                " groups for naics ",
                taskInfo$taskNaics,
                ". Remaining naicsInProcess=",
                paste0(collapse = ", ", names(naicsInProcess))
              )
            ), file = task_log_file_path, append = TRUE)
          } #end if all groups in naic are finished
        },
        debug = FALSE
      ) #end call to startAsyncTask
      processRunningTasks(
        wait = FALSE,
        debug = TRUE,
        maximumTasksToResolve = 1
      )
    }
  }
}


# Working on this
create_pmg_groups <- function(naics, groups, sprod){
  # Assign group number to producers and consumers
  consumers[naics_set[,.(NAICS,groups)],numgroups:=groups,on="NAICS"]
  producers[naics_set[,.(NAICS,groups)],numgroups:=groups,on="NAICS"]
  suppressWarnings(consumers[!is.na(numgroups),group:=1:numgroups,.(NAICS)])
  suppressWarnings(producers[!is.na(numgroups),group:=1:numgroups,.(NAICS)])
  
  #Check that for all groups the capacity > demand
  #to much demand overall?
  prodconsratio <- sum(producers$OutputCapacityTons,na.rm = TRUE)/sum(consumers$PurchaseAmountTons,na.rm = TRUE)
  
}

create_inputs <- function(naics, g, sprod){
  
  #All consumers for this group write PMG input and create table for merging
  fwrite(consumers[group==g,.(InputCommodity,BuyerID,FAF4,Zone=TAZ1,NAICS,Size,OutputCommodity,PurchaseAmountTons,PrefWeight1_UnitCost,PrefWeight2_ShipTime,SingleSourceMaxFraction)], file = file.path(SCENARIO_OUTPUT_PATH,paste0(naics, "_g", g, ".buy.csv")))
  
  conscg <- consumers[group==g,.(InputCommodity,SCTG,NAICS,FAF4,Zone=TAZ1,SCTG,BuyerID,Size,PurchaseAmountTons)]
  
  print(paste(Sys.time(), "Finished writing buy file for ",naics,"group",g))
  
  #If splitting producers, write out each group and else write all with output capacity reduced
  if(sprod==1){
    
    fwrite(producers[group==g,.(OutputCommodity,SellerID,FAF4,Zone=TAZ1,NAICS,Size,OutputCapacityTons,NonTransportUnitCost)], file = file.path(SCENARIO_OUTPUT_PATH,paste0(naics, "_g", g, ".sell.csv")))
    
    prodcg <- producers[group==g,.(OutputCommodity,NAICS,SCTG,SellerID,Size,FAF4,Zone=TAZ1,OutputCapacityTons)]
    
  } else {
    
    #reduce capacity based on demand in this group
    
    consamount <- sum(conscg$PurchaseAmountTons)/sum(consc$PurchaseAmountTons)
    
    prodc[,OutputCapacityTonsG:= OutputCapacityTons * consamount]
    
    fwrite(producers[group==g,.(OutputCommodity,SellerID,FAF4,Zone=TAZ1,NAICS,Size,OutputCapacityTons=OutputCapacityTonsG,NonTransportUnitCost)], file = file.path(SCENARIO_OUTPUT_PATH,paste0(naics, "_g", g, ".sell.csv")))
    
    prodcg <- producers[group==g,.(OutputCommodity,NAICS,SCTG,SellerID,Size,FAF4,Zone=TAZ1,OutputCapacityTons=OutputCapacityTonsG)]
  }
  
  print(paste(Sys.time(), "Applying distribution, shipment, and mode-path models to",naics,"group",g))
  
  # Rename ready to merge
  
  setnames(conscg, c("InputCommodity", "NAICS", "Zone","Size"), c("NAICS", "Buyer.NAICS", "Consumption_zone","Buyer.Size"))
  
  setnames(prodcg, c("OutputCommodity", "NAICS", "Zone","Size"),c("NAICS", "Seller.NAICS", "Production_zone","Seller.Size"))
  
  
}