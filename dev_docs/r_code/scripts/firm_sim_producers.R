firm_sim_producers <- function(Firms,InputOutputValues,unitcost){
  # Create Producers
  producers <- Firms[Producer==1]
  # Foreign_Production <- fread("./dev_firm/Data/inputs/data_foreign_prod.csv")
  # makers <- Firms[Maker==1]
  
  
  InputOutputValues <- InputOutputValues[NAICS6_Make %in% unique(Firms[Producer == 1,NAICS6_Make])]
  setkey(InputOutputValues, NAICS6_Make, ProVal)
  
  # Calculate cumulative pct value of the consumption inputs
  InputOutputValues[, CumPctProVal := cumsum(ProVal)/sum(ProVal), by = NAICS6_Use]
  
  # Select suppliers including the first above the threshold value
  InputOutputValues <- InputOutputValues[CumPctProVal > 1 - BASE_PROVALTHRESHOLD]
  InputOutputValues[,NAICS6_Make:=factor(NAICS6_Make,levels = levels(Firms$NAICS6_Make))]
  InputOutputValues[,NAICS6_Use:=factor(NAICS6_Use,levels = levels(Firms$NAICS6_Make))]
  
  Emp <- producers[!is.na(StateFIPS), .(Emp = sum(Emp)), by = NAICS6_Make]
  InputOutputValues[Emp, Emp := i.Emp, on = "NAICS6_Make"]
  InputOutputValues <- InputOutputValues[,.(ProVal=sum(ProVal), Emp = sum(Emp)), .(NAICS6_Make)]
  InputOutputValues[, ValEmp := ProVal/Emp]
  
  producers <- merge(InputOutputValues[, .(NAICS6_Make, ValEmp)], producers, by = "NAICS6_Make", allow.cartesian = TRUE)
  
  # producers[FirmInputOutputPairs,ValEmp:=i.ValEmp,on=c("BusID","NAICS6_Make","SCTG")]
  producers[!is.na(ValEmp),ProdVal:=ValEmp*Emp]
  # unitcost <- fread("./dev_firm/Data/data_unitcost.csv")
  if("Commodity_SCTG" %in% names(unitcost)) setnames(unitcost,"Commodity_SCTG","SCTG")
  producers[unitcost,UnitCost:=i.UnitCost,on="SCTG"]
  producers[,ProdCap:=ProdVal*1E6/UnitCost]
  producers[,c("naics","ProdVal","esizecat","ValEmp","Model_EmpCat","Industry10","Industry5","Warehouse","Producer","Maker"):=NULL]
  setnames(producers,c("BusID","NAICS6_Make","UnitCost","ProdCap","Emp"),c("SellerID","NAICS","NonTransportUnitCost","OutputCapacityTons","Size"))
  producers[,OutputCommodity:=NAICS]
  return(producers)
}