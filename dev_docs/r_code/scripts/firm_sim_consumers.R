firm_sim_consumers <- function(Firms,FirmInputOutputPairs,unitcost,prefweights){
  # Create Consumers
  consumers <- FirmInputOutputPairs
  consumers[Firms,Emp:=i.Emp,on="BusID"]
  consumers[,ConVal:=Emp*1E6*ValEmp]
  # setnames(unitcost,"Commodity_SCTG","SCTG")
  consumers[unitcost,UnitCost:=i.UnitCost,on="SCTG"]
  consumers[,PurchaseAmountTons:=ConVal/UnitCost]
  consumers[,c("ValEmp"):=NULL]
  # prefweights <- fread("./dev_firm/Data/data_firm_pref_weights.csv")
  setnames(prefweights,c("Commodity_SCTG"),c("SCTG"))
  consumers[prefweights,":="(PrefWeight1_UnitCost=i.CostWeight,PrefWeight2_ShipTime=i.TimeWeight,SingleSourceMaxFraction=i.SingleSourceMaxFraction),on="SCTG"]
  setnames(consumers,c("BusID","NAICS6_Make","NAICS6_Use","Emp","UnitCost"),c("BuyerID","InputCommodity","NAICS","Size","NonTransportUnitCost"))
  # consumers[,InputCommodity:=NAICS]
  return(consumers)
}