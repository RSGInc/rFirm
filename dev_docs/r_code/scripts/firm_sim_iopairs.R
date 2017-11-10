
# Sample producer and consumer firms to create buyer-supplier pairings
firm_sim_iopairs <- function(Firms, InputOutputValues) {
  
  # Identify consumer firms
  Consumers <- Firms[, .(BusID, TAZ1, CountyFIPS, StateFIPS, FAF4, NAICS6_Make, esizecat, Emp)]
  
  # In the consumers table NAICS Use codes are those of the consuming firm
  setnames(Consumers, old = "NAICS6_Make", new = "NAICS6_Use")
  
  # Create a flag to make sure at least one firm is used as a consumer for each zone and commodity combination
  Consumers[Consumers[, .I[sample(x = .N, size = 1)], by = .(TAZ1, FAF4, NAICS6_Use, esizecat)]$V1, MustKeep := 1L]
  Consumers[is.na(MustKeep), MustKeep := 0L]
  
  # Create a sample of consumer firms based on the following rules to form
  # the basis of the producer-consumer pairs to be simulated:
  # 1. Keep all individual businesses in the region and halo states.
  # 2. Also keep all large businesses throughout the U.S
  # 3. Keep those identified as "MustKeep" above
  # 4. Randomly sample an additional small percentage
  Consumers[, temprand := runif(.N)]
  Pairs <- Consumers[temprand > 0.95 | TAZ1 %in% BASE_TAZ1_HALO_STATES  | as.integer(esizecat) >= 5 | MustKeep == 1]
  
  # Remove extra fields
  Pairs[, c("MustKeep", "temprand") := NULL]
  
  # For each consuming firm generate a list of input commodities that need to be purchased
  # Filter to just transported commodities
  InputOutputValues <- InputOutputValues[NAICS6_Make %in% unique(Firms[Producer == 1,NAICS6_Make])]
  
  # Sort on NAICS_Use, ProVal
  setkey(InputOutputValues, NAICS6_Use, ProVal)
  
  # Calculate cumulative pct value of the consumption inputs
  InputOutputValues[, CumPctProVal := cumsum(ProVal)/sum(ProVal), by = NAICS6_Use]
  
  # Select suppliers including the first above the threshold value
  InputOutputValues <- InputOutputValues[CumPctProVal > 1 - BASE_PROVALTHRESHOLD]
  InputOutputValues[,NAICS6_Make:=factor(NAICS6_Make,levels = levels(Firms$NAICS6_Make))]
  InputOutputValues[,NAICS6_Use:=factor(NAICS6_Use,levels = levels(Firms$NAICS6_Make))]
  
  # Calcuate value per employee required (US domestic employment)
  Emp <- Firms[!is.na(StateFIPS), .(Emp = sum(Emp)), by = NAICS6_Make]
  setnames(Emp, old = "NAICS6_Make", new = "NAICS6_Use")
  InputOutputValues[Emp, Emp := i.Emp, on = "NAICS6_Use"]
  InputOutputValues[, ValEmp := ProVal/Emp]
  
  # Merge top k% suppliers with sample from consumers and add the first matching STCG code
  Pairs <- merge(InputOutputValues[, .(NAICS6_Use, NAICS6_Make, ValEmp)], Pairs, by = "NAICS6_Use", allow.cartesian = TRUE)
  
  # Merge in the first matching SCTG code
  Pairs[NAICS2007io_to_SCTG[,.(NAICS6_Make = NAICSio, SCTG)], SCTG := i.SCTG, on = "NAICS6_Make"]
  
  # Some NAICS6_Make industries make more than one SCTG
  # Account for this by simulating the SCTG commodity supplied by them based on probability thresholds
  mult_n6make <- unique(NAICS2007io_to_SCTG[!is.na(SCTG) & Proportion < 1, .(NAICS6_Make = as.character(NAICSio), SCTG, Proportion)])
  n6m_samp <- Pairs[as.character(NAICS6_Make) %in% unique(mult_n6make[["NAICS6_Make"]]), .N , by = NAICS6_Make][!is.na(NAICS6_Make), .(NAICS6_Make = as.character(NAICS6_Make), N)]
  
  assign_mult_sctg <- function(n6m) {
    
    sample(x = mult_n6make[["SCTG"]][mult_n6make[["NAICS6_Make"]] == n6m],
           size = n6m_samp[["N"]][n6m_samp[["NAICS6_Make"]] == n6m],
           replace = TRUE,
           prob = mult_n6make[["Proportion"]][mult_n6make[["NAICS6_Make"]] == n6m])
  }
  
  for (i in 1:nrow(n6m_samp)) {
    Pairs[as.character(NAICS6_Make) == n6m_samp[["NAICS6_Make"]][i], SCTG := assign_mult_sctg(n6m_samp[["NAICS6_Make"]][i])]
  }
  
  # Account for multiple SCTGs produced by certain naics codes (i.e. 211111 and 324110)
  Pairs[NAICS6_Make == "211000" & SCTG == 16L, SCTG := c(16L, 19L)[1 + findInterval(runif(.N), 0.45)]]
  Pairs[NAICS6_Make == "324110", SCTG := c(17L, 18L, 19L)[1 + findInterval(runif(.N), c(0.25, 0.50))]]
  
  # Assume a small chance that a consumer works with a wholesaler instead of a direct shipper for
  # a given shipment/commodity. Mutate some suppliers to wholesaler NAICS.
  sctg_whl <- c(rep("424500", 4), rep("424400" ,3), "424800", "424400",
                rep("423300", 3), rep("423500", 2), "423700",
                rep("424700", 4), "424600", "424200", rep("424600", 2),
                "423400", rep("423300", 2), rep("424100", 3),
                "424300", rep("423500", 2), "423700", "423800", "425100",
                rep("423100", 2), "425100", "423200", "423900")
  
  Pairs[, NAICS6_Use2 := substr(as.character(NAICS6_Use), 1, 2)]
  Pairs[, temprand := runif(.N)]
  Pairs[NAICS6_Use2 != "42" & temprand < 0.30 & SCTG < 41,     NAICS6_Make := sctg_whl[SCTG]]
  Pairs[NAICS6_Use2 != "42" & temprand < 0.15 & SCTG %in% c(35, 38), NAICS6_Make := "423600"]
  Pairs[NAICS6_Use2 != "42" & temprand < 0.15 & SCTG == 40,          NAICS6_Make := "424900"]
  
  # Accounting for multiple SCTGs from one NAICS and for wholesalers means that certain
  # users now have multiple identical inputs on a NAICS6_Make - SCTG basis
  # aggregate (summing over ValEmp) so that NAICS6_MAke - SCTG is unique for each user
  Pairs <- Pairs[, .(ValEmp = sum(ValEmp)), by = .(BusID, NAICS6_Make, SCTG, NAICS6_Use, TAZ1, FAF4, esizecat)]
  
  # Return results
  return(Pairs)
  
}
