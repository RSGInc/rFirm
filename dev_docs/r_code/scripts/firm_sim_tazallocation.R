
# Allocate firms from national/large TAZs to regional/small TAZs
firm_sim_tazallocation <- function(Firms, SocioEconomicsTAZ2, c_taz1_taz2, c_naics_empcat) {
  # Reformat firms data
  # setnames(Firms,c("TAZ"),c("TAZ1"))
  # Firms[,c("n3","n4"):=.(substring(naics,1,3),substring(naics,1,4))]
  
  # Assign the model employment category to each firm
  Firms[c_naics_empcat[nchar(as.character(NAICS2n3)) == 2, .(n2 = NAICS2n3, Model_EmpCat)], Model_EmpCat := i.Model_EmpCat, on = "n2"]
  Firms[c_naics_empcat[nchar(as.character(NAICS2n3)) == 3, .(n3 = NAICS2n3, Model_EmpCat)], Model_EmpCat := i.Model_EmpCat, on = "n3"]
  
  # Is there a 1 to 1 or 1 to many relationship between TAZ1 and TAZ2?
  c_taz1_taz2[, Num := .N, by = TAZ1]
  
  if(nrow(c_taz1_taz2[Num == 1]) > 0){
    
    # Where there is a one to one correspondence between large and small TAZs, add the small TAZ number  
    Firms[c_taz1_taz2[Num == 1], TAZ2 := i.TAZ2, on = "TAZ1"]
    
  }
  
  if(nrow(c_taz1_taz2[Num > 1]) > 0){
    
    # For locations where there is more than 1 small TAZ per large TAZ, allocate based on employment share
    taz2_emp <- merge(c_taz1_taz2[Num > 1], SocioEconomicsTAZ2, by = "TAZ2", all.x = TRUE)
    taz2_emp[, c("Num", "HH") := NULL]
    taz2_emp <- melt(taz2_emp, id.vars = c("TAZ2", "TAZ1"), variable.name = "Model_EmpCat", value.name = "Employment")
                      
    # Calculate sector emp probabilities
    taz2_emp[, Probs := Employment/sum(Employment), by = .(TAZ1, Model_EmpCat)]
    taz2_emp[is.na(Probs), Probs := 0]
    
    # If all probabilities for a TAZ - employment category combination are 0, set them to 1
    taz2_emp[taz2_emp[, .(SumP = sum(Probs)), by = .(TAZ1, Model_EmpCat)][SumP == 0], Probs := 1, on = c("TAZ1", "Model_EmpCat")]
    
    # Expand unallocated firms by the zone options
    FirmsUnallocated <- merge(Firms[is.na(TAZ2), .(BusID, TAZ1, Model_EmpCat)], taz2_emp,
                              by = c("TAZ1", "Model_EmpCat"), allow.cartesian = TRUE)
    
    # For each firm, select a TAZ2 from the set within the TAZ1 using the probabilities for that Model_EmpCat
    AssignedTAZ2 <- FirmsUnallocated[, .(TAZ2 = sample(x = TAZ2, size = 1, prob = Probs)), by = BusID]
    Firms[AssignedTAZ2, TAZ2 := i.TAZ2, on = "BusID"]
  
  }
  
  # Set table structure
  return(Firms[, .(BusID, StateFIPS, FAF4, CountyFIPS, TAZ1, TAZ2,
                   naics, n4, n3, n2, NAICS6_Make,
                   Industry10, Industry5, Model_EmpCat, esizecat, Emp)])
  
}
