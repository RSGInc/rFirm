
# Master function for executing the supply chain simulation.
sc_sim <- function(naics_set) {
  
  # Begin progress tracking
  progressStart(action = "Simulating...", task = "Supply Chain", dir = SCENARIO_LOG_PATH)
  
  # Process network skims into a set of mode choice model inputs describing modal travel times and distances
  ShipmentRoutes <- sc_sim_traveltimedistance(TruckSkim = TruckSkim, RailSkim = RailSkim, WaterSkim = WaterSkim, PipeSkim = PipeSkim,
                                              AirportZones = AirportZones, SeaportZones = SeaportZones, TazGCD = TazGCD, 
                                              ModeChoiceParameters = ModeChoiceParameters)
  gc()
  
  progressUpdate(prop = 0.33, dir = SCENARIO_LOG_PATH)
  # Calculate model costs and times for each shipment route - mode combination
  ShipmentRoutesCosts <- sc_sim_modalcosts(ShipmentRoutes = ShipmentRoutes,  ModeChoiceParameters = ModeChoiceParameters)
  rm(ShipmentRoutes)
  gc()
  
  progressUpdate(prop = 0.67, dir = SCENARIO_LOG_PATH)
  # Create groups of producers and consumers
  if(!USER_MARKETS_ALL) naics_set <- naics_set[as.character(NAICS) %in% NAICS2007io_to_SCTG[SCTG %in% USER_COMMODITIES_RUN,as.character(NAICSio)]]
  
  AllFirms <- sc_sim_pmg(naics_set, ShipmentRoutesCosts = ShipmentRoutesCosts)
  gc()

  # # Select supplier firms for consuming firms
  # BuyerSupplierPairs <- sc_sim_supplierselection(FirmInputOutputPairs = FirmInputOutputPairs, AllFirms = AllFirms,
  #                                                CommodityFlows = CommodityFlowsList$CommodityFlowsBuffer, 
  #                                                TazGCD = TazGCD)
  # gc()
  # 
  # # Apportion freight flows to buyer supplier pairs and weight to FAF data
  # 
  # BuyerSupplierFlows <- sc_sim_apportionment(BuyerSupplierPairs = BuyerSupplierPairs, FirmInputOutputPairs = FirmInputOutputPairs, 
  #                                            AllFirms = AllFirms, CommodityFlows = CommodityFlowsList$CommodityFlowsBuffer)
  # 
  # rm(FirmInputOutputPairs, BuyerSupplierPairs)
  # gc()
  # 
  # # Determine distribution channel
  # BuyerSupplierChannel <- sc_sim_distchannel(BuyerSupplierFlows = BuyerSupplierFlows, AllFirms = AllFirms, TazGCD = TazGCD,
  #                                            distchannel_food = distchannel_food_cal, distchannel_mfg = distchannel_mfg_cal, 
  #                                            c_sctg_cat = c_sctg_cat)
  # rm(BuyerSupplierFlows)
  # gc()
  # 
  # # Determine shipment size
  # BuyerSupplierShipmentSize <- sc_sim_shipmentsize(BuyerSupplierChannel = BuyerSupplierChannel,
  #                                                  shipsize_food = shipsize_food_cal, shipsize_mfg = shipsize_mfg_cal,
  #                                                  calibration = shipsize_calibration)
  # rm(BuyerSupplierChannel)
  # gc()
  # 
  # # Mode choice
  # # Process network skims into a set of mode choice model inputs describing modal travel times and distances
  # ShipmentRoutes <- sc_sim_traveltimedistance(TruckSkim = TruckSkim, RailSkim = RailSkim, WaterSkim = WaterSkim, PipeSkim = PipeSkim,
  #                                             AirportZones = AirportZones, SeaportZones = SeaportZones, TazGCD = TazGCD, 
  #                                             ModeChoiceParameters = ModeChoiceParameters)
  # gc()
  # 
  # # Calculate model costs and times for each shipment route - mode combination
  # ShipmentRoutesCosts <- sc_sim_modalcosts(ShipmentRoutes = ShipmentRoutes,  ModeChoiceParameters = ModeChoiceParameters)
  # gc()
  # 
  # # Determine Shipment mode
  # BuyerSupplierMode <- sc_sim_shipmentmode(BuyerSupplierShipmentSize = BuyerSupplierShipmentSize, ShipmentRoutesCosts = ShipmentRoutesCosts, 
  #                                          ModeChoiceParameters = ModeChoiceParameters, c_sctg_cat = c_sctg_cat,
  #                                          ModeChoiceConstants = ModeChoiceConstants)
  # gc()
  # 
  # # Complete shipment table with intermediate transfer locations added
  # BuyerSupplierModeTransfer <- sc_sim_transferlocations(BuyerSupplierMode = BuyerSupplierMode, ShipmentRoutes = ShipmentRoutes,
  #                                                       DistFacilitiesTAZ = DistFacilitiesTAZ, TruckSkim = TruckSkim, c_taz1_taz2)
  # rm(BuyerSupplierMode)
  # gc()
  # 
  # # Add through trips to and from Import/Export nodes in the region
  # InternationalThru <- sc_sim_internationalthru(CommodityFlows = CommodityFlowsList$CommodityFlowsInternationalThru,
  #                                               AirportZones = AirportZones, SeaportZones = SeaportZones, 
  #                                               c_taz1_faf4 = c_taz1_faf4, AvShipmentSizes = shipsize_calibration)
  # 
  # gc()
  # 
  # # Convert shipments to trips for aggregate assignment and for sampling for truck touring simulation
  # ShipmentTripList <- sc_sim_trips(BuyerSupplierModeTransfer = BuyerSupplierModeTransfer, InternationalThru = InternationalThru,
  #                                  ShipmentRoutes = ShipmentRoutes, TruckSkim = TruckSkim, AnnualFactor = AnnualFactor,
  #                                  Payload = Payload, EmptyTruck = EmptyTruck, TruckType = TruckType)
  # 
  # rm(BuyerSupplierModeTransfer)
  # gc()

  # End progress tracking
  progressEnd(dir = SCENARIO_LOG_PATH)
  
  # Return results
  # return(ShipmentTripList)
  return(list(BuyerSupplierPairs = AllFirms, ShipmentRoutesCosts=ShipmentRoutesCosts))
  
}
