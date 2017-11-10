
#Calculates modal costs and times for the shipment mode and transfer choice model
sc_sim_modalcosts <- function(ShipmentRoutes, ModeChoiceParameters) {
  
  # Update progress log
  progressUpdate(subtaskprogress = 0, subtask = "Calculate Modal Costs", prop = 1/6, dir = SCENARIO_LOG_PATH)
  
  ### Convert parameters from to individual objects for use in formulas
  
  for(i in 1:nrow(ModeChoiceParameters)) assign(ModeChoiceParameters$Variable[i], value = ModeChoiceParameters$Value[i])
  
  # Update progress log
  progressUpdate(subtaskprogress = 0.20, subtask = "Calculate Modal Costs", prop = 1/6, dir = SCENARIO_LOG_PATH)
  
  ### Times and Costs for Direct Truck and Truck-Air, Truck-Rail, and Truck-Water
  
  ShipmentRoutesCosts <- copy(ShipmentRoutes)
  
  # Truck Only (Direct)
  # Path for either FTL or LTL depending on shipment size
  # 1 FTL Direct 
  # 2 LTL Direct
  ShipmentRoutesCosts[, c("time1","time2") := trucktime]
  ShipmentRoutesCosts[, cost1 := truckdist * FTL53rate]
  ShipmentRoutesCosts[, cost2 := truckdist * LTL53rate]
  
  # Truck-Air-Truck
  # Use LTL rates as max air shipment weight is smaller than an FTL
  # TODO should we conside alternative airports or just use the closest from the skim?
  # 3 LTL-Air-LTL 
  ShipmentRoutesCosts[, time3 := tatrucktime + taairtime + AirTime * 60 * 3] #assumes a transfer that 1*airtime
  ShipmentRoutesCosts[, cost3 := tatruckdist * LTL53rate + taairdist * AirRate + AirHandFee * 2]
    
  # Truck-Rail-Truck
  # TODO what about direct by rail?
  # 4 FTL-Carload-FTL
  # 5 LTL-IMX-LTL
  # 6 FTL-IMX-FTL
  ShipmentRoutesCosts[, time4:= trtrucktime + trrailtime + BulkTime * 60 * 2]
  ShipmentRoutesCosts[, c("time5","time6") := trtrucktime + trrailtime + IMXTime * 60 * 2]
  ShipmentRoutesCosts[, cost4 := trtruckdist * FTL53rate + trraildist * CarloadRate + BulkHandFee * 2]
  ShipmentRoutesCosts[, cost5 := trtruckdist * LTL40rate + trraildist * IMXRate + IMXHandFee * 2]
  ShipmentRoutesCosts[, cost6 := trtruckdist * FTL40rate + trraildist * IMXRate + IMXHandFee * 2]
    
  # Truck-Water-Truck
  # TODO what about rail access or direct by water only?
  # Use FTL rates as water movements are large volumes
  # 7 FTL-Water-FTL
  ShipmentRoutesCosts[, time7 := twtrucktime + twwatertime + BulkTime * 60 * 2]
  ShipmentRoutesCosts[, cost7 := twtruckdist * FTL53rate + twwaterdist * WaterRate + BulkHandFee * 2]
  
  # Update progress log
  progressUpdate(subtaskprogress = 0.40, subtask = "Calculate Modal Costs", prop = 1/6, dir = SCENARIO_LOG_PATH)
  
  ### Times and Costs for Ports
  
  # Truck-Gateway Port
  # TODO what about port to intl. dest time on ship? sea lane distance? It will be approx the same for close ports
  # 8 FTL-NoTransload-Port (40' container)
  # 9 LTL-NoTranload-Port (40' container)
  # 10 FTL-Transload-Port (53' trailer)
  # 11 LTL-Transload-Port (53' trailer)
  ShipmentRoutesCosts[, c("time8","time9") := tptrucktime + tpwatertime]
  ShipmentRoutesCosts[, c("time10","time11") := tptrucktime + tpwatertime + TloadTime * 60 * 2]
  ShipmentRoutesCosts[, cost8 := tptruckdist * FTL40rate + tpwaterdist * WaterRate]
  ShipmentRoutesCosts[, cost9 := tptruckdist * LTL40rate + tpwaterdist * WaterRate]
  ShipmentRoutesCosts[, cost10 := tptruckdist * FTL53rate + tpwaterdist * WaterRate + TloadHandFee * 2]
  ShipmentRoutesCosts[, cost11 := tptruckdist * LTL53rate + tpwaterdist * WaterRate + TloadHandFee * 2]
  
  #Truck-Rail-Gateway Port
  #12 FTL-Carload-Port
  #13 LTL-IMX-Port
  #14 FTL-IMX-Port 
  ShipmentRoutesCosts[, time12 := trptrucktime + trprailtime + trpwatertime + BulkTime * 60 + TloadTime * 60 * 2]
  ShipmentRoutesCosts[, c("time13","time14") := trptrucktime + trprailtime + trpwatertime + IMXTime * 60]
  ShipmentRoutesCosts[, cost12 := trptruckdist * FTL53rate + trpraildist * CarloadRate + trpwaterdist * WaterRate + BulkHandFee + TloadHandFee * 2]
  ShipmentRoutesCosts[, cost13 := trptruckdist * LTL40rate + trpraildist * IMXRate + trpwaterdist * WaterRate + IMXHandFee]
  ShipmentRoutesCosts[, cost14 := trptruckdist * FTL40rate + trpraildist * IMXRate + trpwaterdist * WaterRate + IMXHandFee]
  
  # Update progress log
  progressUpdate(subtaskprogress = 0.60, subtask = "Calculate Modal Costs", prop = 1/6, dir = SCENARIO_LOG_PATH)
  
  ### Times and Costs for Truck via Distribution Center(s)
  
  #Truck Only (Indirect)
  #Paths for 1 and 2 two DC stops, for with LTL or FTL depending on shipment size
  #15 LTL-DC-LTL
  #16 LTL-DC-FTL-DC-LTL (use LTL rate)
  #17 FTL-DC-FTL
  #18 FTL-DC-FTL-DC-FTL
  ShipmentRoutesCosts[, c("time15","time17") := trucktime + WDCTime * 60]
  ShipmentRoutesCosts[, cost15:= truckdist * LTL53rate + WDCHandFee]
  ShipmentRoutesCosts[, cost17:= truckdist * FTL53rate + WDCHandFee]
  ShipmentRoutesCosts[, c("time16","time18") := trucktime + WDCTime * 60 * 2]
  ShipmentRoutesCosts[, cost16 := truckdist * LTL53rate + WDCHandFee * 2]
  ShipmentRoutesCosts[, cost18 := truckdist * FTL53rate + WDCHandFee * 2]

  # Update progress log
  progressUpdate(subtaskprogress = 0.80, subtask = "Calculate Modal Costs", prop = 1/6, dir = SCENARIO_LOG_PATH)
  
  # Truck-Pipeline-Truck
  # 19 FTL-PipePetrol-FTL
  # 20 FTL-PipeCrude-FTL
  # 21 FTL-PipeCoalNEC-FTL
  ShipmentRoutesCosts[, time19:= tpptrucktime + pptpipetime + PipeTime * 60 * 2]
  ShipmentRoutesCosts[, time20:= tpctrucktime + pctpipetime + PipeTime * 60 * 2]
  ShipmentRoutesCosts[, time21:= tpntrucktime + pntpipetime + PipeTime * 60 * 2]
  ShipmentRoutesCosts[, cost19 := tpptruckdist * FTL53rate + pptpipedist * PipeRate + PetroHandFee * 2]
  ShipmentRoutesCosts[, cost20 := tpctruckdist * FTL53rate + pctpipedist * PipeRate + CrudeHandFee * 2]
  ShipmentRoutesCosts[, cost21 := tpntruckdist * FTL53rate + pntpipedist * PipeRate + CoalNECHandFee * 2]
  
  # Update progress log
  progressUpdate(subtaskprogress = 1, subtask = "Calculate Modal Costs", prop = 1/6, dir = SCENARIO_LOG_PATH)
  
  # Return results
  ShipmentRoutesCosts <- ShipmentRoutesCosts[, c("oTAZ1","dTAZ1", "ODSegment", paste0("time",1:21), paste0("cost",1:21)), with = FALSE]
  nTAZPairs <- nrow(ShipmentRoutesCosts)
  ShipmentRoutesCosts <- cbind(melt.data.table(ShipmentRoutesCosts,id.vars = c("oTAZ1","dTAZ1","ODSegment"), measure.vars = paste0("time",1:21), variable.name = "timepath", value.name = "time"), melt.data.table(ShipmentRoutesCosts[,paste0("cost",1:21), with=FALSE], measure.vars = paste0("cost",1:21), variable.name = "costpath", value.name = "cost"))
  ShipmentRoutesCosts[, path:= as.numeric(rep(1:21,each=nTAZPairs))]
  ShipmentRoutesCosts <- ShipmentRoutesCosts[!(is.na(time) | is.na(cost))]
  ShipmentRoutesCosts[mode_description,Mode.Domestic:=i.Mode.Domestic,on="path"]
  return(ShipmentRoutesCosts[,c("timepath","costpath"):=NULL][,.(oTAZ1,dTAZ1,ODSegment,Mode.Domestic,path,time,cost)])
}
