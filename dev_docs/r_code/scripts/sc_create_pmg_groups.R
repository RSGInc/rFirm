create_pmg_groups <- function(naics_set, consumers, producers){
  # Assign group number to producers and consumers
  consumers[naics_set[,.(NAICS,groups)],numgroups:=groups,on="NAICS"]
  producers[naics_set[,.(NAICS,groups)],numgroups:=groups,on="NAICS"]
  suppressWarnings(consumers[!is.na(numgroups),group:=1:numgroups,.(NAICS)])
  suppressWarnings(producers[!is.na(numgroups),group:=1:numgroups,.(NAICS)])
  
  #Check that for all groups the capacity > demand
  #to much demand overall?
  prodconsratio <- sum(producers$OutputCapacityTons,na.rm = TRUE)/sum(consumers$PurchaseAmountTons,na.rm = TRUE)
  
}