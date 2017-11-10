sc_sim_distchannel <- function(pcpairs, mesozone_gcd, distchannel_food, distchannel_mfg, calibration = NULL){
  pcpairs
  
  distchan_calcats <- data.table(CHOICE=c("0","1","2+","2+"),CHID=1:4) #correspondence between choice model alts and target categories int he distchan model
  
  # Create employment and industry dummy variables
  pcpairs[, c("emple49", "emp50t199", "empge200", "mfgind", "trwind", "whind") := 0L]
  pcpairs[Buyer.Size <= 49, emple49 := 1]
  pcpairs[Buyer.Size >= 50 & Buyer.Size <= 199, emp50t199 := 1]
  pcpairs[Buyer.Size >= 200, empge200 := 1]
  
  pcpairs[,Seller.NAICS2:=substr(Seller.NAICS,1,2)]
  pcpairs[Seller.NAICS2 %in% 31:33, mfgind := 1]
  pcpairs[Seller.NAICS2 %in% 48:49, trwind := 1]
  pcpairs[Seller.NAICS2 %in% c(42, 48, 49), whind := 1]
  
  pcpairs[,Buyer.NAICS2:=substr(Buyer.NAICS,1,2)]
  pcpairs[Buyer.NAICS2 %in% 31:33, mfgind := 1]
  pcpairs[Buyer.NAICS2 %in% 48:49, trwind := 1]
  pcpairs[Buyer.NAICS2 %in% c(42, 48, 49), whind := 1]
  
  
  # Add the FAME SCTG category for comparison with calibration targets
  pcpairs[, CATEGORY := famesctg[Commodity_SCTG]]
  setkey(pcpairs, Production_zone, Consumption_zone)
  
  # Add zone to zone distances
  pcpairs <- merge(pcpairs, mesozone_gcd, c("Production_zone", "Consumption_zone")) # append distances
  setnames(pcpairs, "GCD", "Distance")
  
  # Update progress log
  # Missing for now
  
  
  print(paste(Sys.time(), "Applying distribution channel model"))
  
  #Apply choice model of distribution channel and iteratively adjust the ascs
  
  #The model estimated for mfg products was applied to all other SCTG commodities
  
  
  inNumber <- nrow(pcpairs[Commodity_SCTG %in% c(1:9)])
  
  if (inNumber > 0) {
    
    # Sort on vars so simulated choice is ordered correctly
    model_vars_food <- c("CATEGORY", distchannel_food[TYPE == "Variable", unique(VAR)])
    model_ascs_food <- distchannel_food[TYPE == "Constant", unique(VAR)]
    setkeyv(pcpairs, model_vars_food) #sorted on vars, calibration coefficients, so simulated choice is ordered correctly
    
    pcpairs_food <- pcpairs[Commodity_SCTG %in% c(1:9),model_vars_food,with=FALSE]
    pcpairs_food_weight <- pcpairs[Commodity_SCTG %in% 1:9,PurchaseAmountTons]
    
    df <- pcpairs_food[, list(Start = min(.I), Fin = max(.I)), by = model_vars_food] #unique combinations of model coefficients
    
    df[, (model_ascs_food) := 1] #add 1s for constants to each group in df
    
    print(paste(Sys.time(), nrow(df), "unique combinations"))
    
    if(!is.null(calibration)){
      
      pcpairs[Commodity_SCTG %in% c(1:9), distchannel := predict_logit(df, distchannel_food, cal = distchan_cal, calcats = distchan_calcats, weight = pcpairs_food_weight, path = file.path(model$inputdir,paste0(naics,"_g",g,"_model_distchannel_food_cal.csv")), iter = 4)]
      
    } else {
      
      pcpairs[Commodity_SCTG %in% 1:9, distchannel := predict_logit(df, distchannel_food,cal=distchan_cal,calcats=distchan_calcats)]
      
    }
    
  }
  
  # Update progress log
  
  
  print(paste(Sys.time(), "Finished ", inNumber, " for Commodity_SCTG %in% c(1:9)"))
  
  ### Apply choice model of distribution channel for other industries
  
  # The model estimated for mfg products is applied to all other SCTG commoditie
  
  outNumber <- nrow(pcpairs[!Commodity_SCTG %in% c(1:9)])
  
  if (outNumber > 0) {
    
    # Sort on vars so simulated choice is ordered correctly
    model_vars_mfg <- c("CATEGORY", distchannel_mfg[TYPE == "Variable", unique(VAR)])
    model_ascs_mfg <- distchannel_mfg[TYPE == "Constant", unique(VAR)]
    
    setkeyv(pcpairs, model_vars_mfg) #sorted on vars so simulated choice is ordered correctly
    
    pcpairs_mfg <- pcpairs[!Commodity_SCTG %in% c(1:9),model_vars_mfg,with=FALSE]
    pcpairs_mfg_weight <- pcpairs[!Commodity_SCTG %in% c(1:9),PurchaseAmountTons]
    
    df <- pcpairs_mfg[, list(Start = min(.I), Fin = max(.I)), by = model_vars_mfg] #unique combinations of model coefficients
    
    ####do this in the function (seems unecessary here)?
    
    df[, (model_ascs_mfg) := 1] #add 1s for constants to each group in df
    
    print(paste(Sys.time(), nrow(df), "unique combinations"))
    
    # Simulate choice -- with calibration if calibration targets provided
    if(!is.null(calibration)){
      
      pcpairs[!Commodity_SCTG %in% c(1:9), distchannel := predict_logit(df, distchannel_mfg, cal = distchan_cal, calcats = distchan_calcats, weight = pcpairs_mfg_weight, path = file.path(model$inputdir,paste0(naics,"_g",g,"_model_distchannel_mfg_cal.csv")), iter=4)]
      
    } else {
      
      pcpairs[!Commodity_SCTG %in% 1:9, distchannel := predict_logit(df, distchannel_mfg,cal = distchan_cal,calcats = distchan_calcats)]
      
    }
    
  }
  
  rm(df)
  print(paste(Sys.time(), "Finished ", outNumber, " for !Commodity_SCTG %in% c(1:9)"))
  
  
  # Update progress log
  
  return(pcpairs)
  
}