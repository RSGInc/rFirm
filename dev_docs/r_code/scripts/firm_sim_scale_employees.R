
# Scale Firms to Employment Forecasts
firm_sim_scale_employees <- function(Firms, EmploymentCategories, c_naics_empcat, c_taz1_taz2, c_taz1_fips, c_taz1_faf4, SocioEconomicsTAZ2 = NULL, SocioEconomicsTAZ1 = NULL) {
 
  #list of employment categories used in the model
  ModelEmpCats <- as.character(unique(c_naics_empcat$Model_EmpCat))
  
  # Scale synthetic firms to employment forecasts
  # 1. For region with small zones use TAZ2 SE data
  
  if(!is.null(SocioEconomicsTAZ2)){
    
    SocioEconomicsTAZ <- SocioEconomicsTAZ2[, c("TAZ2", ModelEmpCats), with = FALSE]
    setnames(SocioEconomicsTAZ, "TAZ2", "TAZ")
  
    TAZ2RegionFirms <- firm_sim_scale_employees_taz(RegionFirms = Firms[!is.na(TAZ2), .(TAZ = TAZ2, BusID, CountyFIPS, StateFIPS, FAF4, naics, n4, n3, n2, NAICS6_Make, Industry10, Industry5, Model_EmpCat, esizecat, Emp)], SocioEconomicsTAZ = SocioEconomicsTAZ, MaxBusID = max(Firms$BusID))
   
    setnames(TAZ2RegionFirms,"TAZ","TAZ2")
    TAZ2RegionFirms[c_taz1_taz2, TAZ1 := i.TAZ1, on = "TAZ2"]
    
    # Define the TAZ1s to use: it is BASE_TAZ1_BUFFER_HALO_STATES with the TAZ2 zones removed
    # if these was TAZ2 SE data
    TAZ1BufferNotTAZ2 <- BASE_TAZ1_HALO_STATES[!BASE_TAZ1_HALO_STATES %in% (c_taz1_taz2$TAZ1)]
    
    # Define the TAZ1s used so far
    TAZ1Scaled <- unique(c_taz1_taz2$TAZ1)
     
  } else {
    
    TAZ2RegionFirms <- NULL
    TAZ1Scaled <- NULL
    TAZ1BufferNotTAZ2 <- BASE_TAZ1_HALO_STATES
    
  }
  
  # 2. For rest of area that has SE data and large zones, use TAZ1 SE data
  
  if(!is.null(SocioEconomicsTAZ1)){
    
    SocioEconomicsTAZ <- SocioEconomicsTAZ1[TAZ1 %in% TAZ1BufferNotTAZ2, c("TAZ1", ModelEmpCats), with = FALSE]
    setnames(SocioEconomicsTAZ, "TAZ1", "TAZ")
    
    TAZ1RegionFirms <- firm_sim_scale_employees_taz(RegionFirms = Firms[TAZ1 %in% TAZ1BufferNotTAZ2,
                                                                     .(TAZ = TAZ1, BusID, CountyFIPS, StateFIPS, 
                                                                     FAF4, naics, n4, n3, n2, NAICS6_Make, Industry10, 
                                                                     Industry5, Model_EmpCat, esizecat, Emp)], 
                                                 SocioEconomicsTAZ = SocioEconomicsTAZ, 
                                                 MaxBusID = max(max(TAZ2RegionFirms$BusID), max(Firms$BusID)))
    
    setnames(TAZ1RegionFirms,"TAZ","TAZ1")
    TAZ1RegionFirms[, TAZ2 := NA]
    TAZ1Scaled <- c(TAZ1Scaled, TAZ1BufferNotTAZ2)
    
  } else {
    
    TAZ1RegionFirms <- NULL
    
  }
  
  # Combine the two sections together 
  RegionFirms <- rbindlist(list(TAZ2RegionFirms, TAZ1RegionFirms), use.names = TRUE, fill = TRUE)
  
  # Update the County and State FIPS and FAF zones
  RegionFirms[c_taz1_fips, StateFIPS := i.StateFIPS, on = "TAZ1"]
  RegionFirms[c_taz1_fips, CountyFIPS := i.CountyFIPS, on = "TAZ1"]
  RegionFirms[c_taz1_faf4, FAF4 := i.FAF4, on = "TAZ1"]
  
  # Recode employee counts into categories
  RegionFirms[, Emp := as.integer(Emp)]
  RegionFirms[, esizecat := cut(x = Emp, breaks = c(as.numeric(gsub("^(\\d+)\\D.*","\\1",EmploymentCategories$Label)), Inf),
                          labels = EmploymentCategories[["Label"]], right = FALSE, ordered_result = TRUE)]
  
  #Combine the update regional firms with those from the rest of the country
  Firms <- rbind(RegionFirms, Firms[!TAZ1 %in% TAZ1Scaled], use.names = TRUE, fill = TRUE)
  
  # Set table structure
  setkey(Firms, BusID)
  
  # Return results
  return(Firms)
  
}

#Define a function that scales for a given zone system -- need to call more than once where multiple zone systems
firm_sim_scale_employees_taz <- function(RegionFirms, SocioEconomicsTAZ, MaxBusID){
  
  # Summarize employment of synthesized firms by TAZ and Model_EmpCat
  Employment.CBP <- RegionFirms[, .(Employees.CBP = sum(Emp)), by = .(TAZ, Model_EmpCat)]
  
  # Melt employment data by TAZ and employment category  
  Employment.SE <- melt(SocioEconomicsTAZ, id.vars = "TAZ", variable.name = "Model_EmpCat", value.name = "Employees.SE")
  
  # Compare employment between socio-economic input (SE) and synthetized firms (CBP)
  Employment.Compare <- merge(Employment.SE, Employment.CBP, by = c("TAZ", "Model_EmpCat"), all = TRUE)
  Employment.Compare[is.na(Employees.SE),   Employees.SE := 0]
  Employment.Compare[is.na(Employees.CBP), Employees.CBP := 0]
  
  # If both employment sources say there should be no employment, there's
  # nothing to do but drop those records to save on calculations
  Employment.Compare <- Employment.Compare[!(Employees.SE == 0 & Employees.CBP == 0)]
  Employment.Compare[, Employees.Difference := Employees.SE - Employees.CBP]
  
  # Three cases to deal with:
  # 1. Employment change where there is some CBP employment and some SE employment
  # 2. Employment in SE data but none from CBP
  # 3. Employment from CBP but none in SE data
  #
  # Note: if difference is 0, no action required --> Adjustment factor will be 1: 
  
  # Calculate employment scale factors to deal with Case 1 and 3 simultaneously
  Employment.Scaled <- Employment.Compare[Employees.CBP > 0]
  Employment.Scaled[, Adjustment := Employees.SE / Employees.CBP]
  
  # Scale employees in firms table and bucket round
  RegionFirms[Employment.Scaled, Adjustment := i.Adjustment, on = c("TAZ", "Model_EmpCat")]
  RegionFirms[, Emp := as.numeric(Emp)]
  RegionFirms[!is.na(Adjustment), Emp := as.numeric(bucketRound(Emp * Adjustment)), by = .(TAZ, Model_EmpCat)]
  RegionFirms[, Adjustment := NULL]
  RegionFirms <- RegionFirms[Emp > 0]
  
  # Add firms to empty TAZ-Employment category combinations to deal with Case 2
  FirmsNeeded <- Employment.Compare[Employees.CBP < 1]
  
  # For each combination:
  # 1. Select a number of firms max of 1 and EMPDIFF/average emp from all of the firms in that Model_EmpCat
  # 2. Add them to the firms table
  # 3. recalc the EMPADJ to refine the employment to match exactly the SE data employment
  
  # Calculate average employment by Model_EmpCat
  Employment.Avg <- RegionFirms[, .(Employees.Avg = mean(Emp)), by = Model_EmpCat]
  FirmsNeeded[Employment.Avg, Employees.Avg := i.Employees.Avg, on = "Model_EmpCat"]
  EmpCatAbsentCBP <- FirmsNeeded[is.na(Employees.Avg),unique(Model_EmpCat)] # Employment Category Absent from CBP
  FirmsNeeded[is.na(Employees.Avg),Employees.Avg:=10]
  
  # Calculate number of firms to be sampled
  FirmsNeeded[, N := round(pmax(1, Employees.Difference/Employees.Avg))]
  
  # Sample the N firms needed for each TAZ and Employment category
  NewFirms <- FirmsNeeded[, .(N = sum(N)), by = .(TAZ, Model_EmpCat)]
  NewFirms <- NewFirms[!Model_EmpCat %in% EmpCatAbsentCBP, .(BusID = sample(x = RegionFirms[Model_EmpCat == Model_EmpCat.temp, BusID], size = N, replace = TRUE)),
                       by = .(TAZ, Model_EmpCat.temp = Model_EmpCat)] # Firms that are present in RegionFirms
  #Note: Need to implement a method for Firms not present in Region Firms.
  setnames(NewFirms, old = "Model_EmpCat.temp", new = "Model_EmpCat")
  
  # Look up the firm attributes for these new firms (from the ones they were created from)
  NewFirms <- merge(NewFirms, RegionFirms[, !c("TAZ", "Model_EmpCat"), with = FALSE], by = "BusID")
  
  # Check that the employee counts of the new firms matches the SE data and scale/bucket round as needed
  Employment.New <- NewFirms[, .(Employees.NewFirms = sum(Emp)), by = .(TAZ, Model_EmpCat)]
  Employment.New[Employment.SE, Employees.SE := i.Employees.SE, on = c("TAZ", "Model_EmpCat")]
  Employment.New[, Adjustment := Employees.SE / Employees.NewFirms]
  NewFirms[Employment.New[, .(TAZ, Model_EmpCat, Adjustment)], Adjustment := i.Adjustment, on = c("TAZ", "Model_EmpCat")]
  NewFirms[, Emp := as.numeric(bucketRound(Emp * Adjustment)), by = .(TAZ, Model_EmpCat)]
  NewFirms[, Adjustment := NULL]
  NewFirms <- NewFirms[Emp >= 1]
  
  # Give the new firms new, unique business IDs
  NewFirms[, BusID := .I + MaxBusID]
  
  # Combine the original firms and the new firms
  RegionFirms <- rbindlist(list(RegionFirms, NewFirms), use.names = TRUE, fill = TRUE)
  
  return(RegionFirms)
  
}
