
#Process the skims and other inputs required for the shipment mode and transfer choice model
sc_sim_traveltimedistance <- function(TruckSkim, RailSkim, WaterSkim, PipeSkim, AirportZones, SeaportZones, TazGCD, ModeChoiceParameters) {
  
  # Update progress log
  progressUpdate(subtaskprogress = 0, subtask = "Process Skim Data", prop = 1/6, dir = SCENARIO_LOG_PATH)
  
  ### Label and format the skims and other input files
  
  # Truck Skims : key
  setkey(TruckSkim, oTAZ1, dTAZ1)
  
  # Intermodal Skims: remove unavailable pairs, key and add speeds
  RailSkim <- RailSkim[raildist > 0 & raildist < 999999] 
  # railtime is already determined in skim
  setkey(RailSkim, oTAZ1, dTAZ1)
  
  WaterSkim <- WaterSkim[waterdist > 0 & waterdist < 999999] 
  WaterSkim[,watertime := waterdist * 60 / ModeChoiceParameters[Variable == "WaterSpeed", Value]]
  setkey(WaterSkim, oTAZ1, dTAZ1)
  
  PipeSkim <- PipeSkim[pipedist > 0 & pipedist < 999999] 
  PipeSkim[,pipetime := pipedist * 60 / ModeChoiceParameters[Variable == "PipeSpeed", Value]]
  # Split pipeline skims into one skim per commodity
  PipePetrolSkim <- PipeSkim[commodity == "Petrol", .(oTAZ1, dTAZ1, pipedist, pipetime)]
  PipeCrudeSkim <- PipeSkim[commodity == "Crude", .(oTAZ1, dTAZ1, pipedist, pipetime)]
  PipeCoalNECSkim <- PipeSkim[commodity == "CoalNEC", .(oTAZ1, dTAZ1, pipedist, pipetime)]
  setkey(PipePetrolSkim, oTAZ1, dTAZ1)
  setkey(PipeCrudeSkim, oTAZ1, dTAZ1)  
  setkey(PipeCoalNECSkim, oTAZ1, dTAZ1)
  
  
  # Update progress log
  progressUpdate(subtaskprogress = 0.10, subtask = "Process Skim Data", prop = 1/6, dir = SCENARIO_LOG_PATH)
  
  ### Process the shipment routes
  TAZ1GCD <- pairsGCD(TazGCD)
  ShipmentRoutes <- TAZ1GCD[, .(oTAZ1, dTAZ1)]
  
  # Add the OD segment to the list of shipment routes
  ShipmentRoutes[oTAZ1 %in% BASE_TAZ1_FOREIGN & !dTAZ1 %in% BASE_TAZ1_FOREIGN, ODSegment := "IM"]
  ShipmentRoutes[!oTAZ1 %in% BASE_TAZ1_FOREIGN & dTAZ1 %in% BASE_TAZ1_FOREIGN, ODSegment := "EX"]
  ShipmentRoutes[!oTAZ1 %in% BASE_TAZ1_FOREIGN & !dTAZ1 %in% BASE_TAZ1_FOREIGN, ODSegment := "II"]
  
  # Model just flows to, from, and within the US
  # Remove those shipments to and from the foreign countries
  # The import and export flows that use the ports of entry in the US will be dealt with seperately
  ShipmentRoutes <- ShipmentRoutes[!is.na(ODSegment)]
  
  # Key the routes on O and D zones
  setkey(ShipmentRoutes, oTAZ1, dTAZ1)
  
  # Join on the distance skims from trucks
  # No match for certain IM/EX movements -- outside North America
  ShipmentRoutes[TruckSkim, c("truckdist","trucktime") := .(i.truckdist, i.trucktime), on = c("oTAZ1", "dTAZ1")] 

  # Develop the remaining shortest paths for other modes for the unique set of TAZ1 pairs
  TAZ1Pairs <- unique(ShipmentRoutes[oTAZ1 > 0 & dTAZ1 > 0, .(oTAZ1,dTAZ1) ])[order(oTAZ1, dTAZ1)]
  
  # List of TAZ1 that have a direct truck path
  TruckTAZ1 <- unique(TruckSkim[,.(TAZ1 = oTAZ1)])[order(TAZ1)]
  # List of TAZ1 with rail access (via an intermodal)
  RailTAZ1 <- unique(RailSkim[,.(TAZ1 = oTAZ1)])[order(TAZ1)]
  # List of TAZ1 with water access (via an intermodal)
  WaterTAZ1 <- unique(WaterSkim[,.(TAZ1 = oTAZ1)])[order(TAZ1)]
  # List of TAZ1 for airports 
  AirportTAZ1 <- data.table(TAZ1 = unique(AirportZones$TAZ1))[order(TAZ1)]
  # List of TAZ1 for gateway ports
  PortTAZ1 <- data.table(TAZ1 = unique(SeaportZones$TAZ1))[order(TAZ1)]
  # List of TAZ1 with petrol pipeline access
  PipePetrolTAZ1 <- unique(PipePetrolSkim[,.(TAZ1 = oTAZ1)])[order(TAZ1)]
  # List of TAZ1 with crude pipeline access
  PipeCrudeTAZ1 <- unique(PipeCrudeSkim[,.(TAZ1 = oTAZ1)])[order(TAZ1)]
  # List of TAZ1 with coal NEC pipeline access
  PipeCoalNECTAZ1 <- unique(PipeCoalNECSkim[,.(TAZ1 = oTAZ1)])[order(TAZ1)]
  
  # Update progress log
  progressUpdate(subtaskprogress = 0.20, subtask = "Process Skim Data", prop = 1/6, dir = SCENARIO_LOG_PATH)
  
  ### Truck-Rail 
  
  # Identify closest rail access and egress connections
  RailAccess <- data.table(expand.grid(oTAZ1 = unique(TAZ1Pairs$oTAZ1), dTAZ1 = RailTAZ1$TAZ1), key = c("oTAZ1","dTAZ1"))
  RailAccess[TruckSkim, c("truckdist","trucktime") := list(i.truckdist, i.trucktime)]
  RailAccess <- RailAccess[RailAccess[,.I[which.min(truckdist)], by = oTAZ1]$V1]
  
  RailEgress <- data.table(expand.grid(oTAZ1 = RailTAZ1$TAZ1, dTAZ1 = unique(TAZ1Pairs$dTAZ1)), key = c("oTAZ1","dTAZ1"))
  RailEgress[TruckSkim, c("truckdist","trucktime") := list(i.truckdist, i.trucktime)]
  RailEgress <- RailEgress[RailEgress[,.I[which.min(truckdist)], by = dTAZ1]$V1]
  
  # Combine elements into oTAZ1-TRTAZ1-RTTAZ1-dRTAZ1 path
  TruckRail <- TAZ1Pairs[RailAccess[, .(oTAZ1, TRTAZ1 = dTAZ1, ratrucktime = trucktime, ratruckdist = truckdist)], on = "oTAZ1", nomatch = 0]
  TruckRail <- TruckRail[RailEgress[, .(dTAZ1, RTTAZ1 = oTAZ1, retrucktime = trucktime, retruckdist = truckdist)], on = "dTAZ1", nomatch = 0]
  TruckRail <- TruckRail[RailSkim[, .(TRTAZ1 = oTAZ1, RTTAZ1 = dTAZ1, trrailtime = railtime, trraildist = raildist)], on = c("TRTAZ1","RTTAZ1"), nomatch = 0]
  setkey(TruckRail, oTAZ1, dTAZ1)
  TruckRail[, trtrucktime := ratrucktime + retrucktime]
  TruckRail[, trtruckdist := ratruckdist + retruckdist]
  TruckRail[,c("ratrucktime","ratruckdist","retrucktime","retruckdist"):=NULL]
  
  # Join on the truck rail paths
  ShipmentRoutes <- merge(ShipmentRoutes, TruckRail, by = c("oTAZ1", "dTAZ1"), all.x = TRUE)
  
  # Update progress log
  progressUpdate(subtaskprogress = 0.30, subtask = "Process Skim Data", prop = 1/6, dir = SCENARIO_LOG_PATH)
  
  ### Truck-Water
  
  # Identify closest water access and egress connections
  WaterAccess <- data.table(expand.grid(oTAZ1 = unique(TAZ1Pairs$oTAZ1), dTAZ1 = WaterTAZ1$TAZ1), key = c("oTAZ1","dTAZ1"))
  WaterAccess[TruckSkim, c("truckdist","trucktime") := list(i.truckdist, i.trucktime)]
  WaterAccess <- WaterAccess[WaterAccess[,.I[which.min(truckdist)], by = oTAZ1]$V1]
  
  WaterEgress <- data.table(expand.grid(oTAZ1 = WaterTAZ1$TAZ1, dTAZ1 = unique(TAZ1Pairs$dTAZ1)), key = c("oTAZ1","dTAZ1"))
  WaterEgress[TruckSkim, c("truckdist","trucktime") := list(i.truckdist, i.trucktime)]
  WaterEgress <- WaterEgress[WaterEgress[,.I[which.min(truckdist)], by = dTAZ1]$V1]
  
  # Combine elements into oTAZ1-TRTAZ1-RTTAZ1-dRTAZ1 path
  TruckWater <- TAZ1Pairs[WaterAccess[, .(oTAZ1, TWTAZ1 = dTAZ1, watrucktime = trucktime, watruckdist = truckdist)], on = "oTAZ1", nomatch = 0]
  TruckWater <- TruckWater[WaterEgress[, .(dTAZ1, WTTAZ1 = oTAZ1, wetrucktime = trucktime, wetruckdist = truckdist)], on = "dTAZ1", nomatch = 0]
  TruckWater <- TruckWater[WaterSkim[, .(TWTAZ1 = oTAZ1, WTTAZ1 = dTAZ1, twwaterdist = waterdist, twwatertime = watertime)], on = c("TWTAZ1","WTTAZ1"), nomatch = 0]
  setkey(TruckWater, oTAZ1, dTAZ1)
  TruckWater[, twtrucktime := watrucktime + wetrucktime]
  TruckWater[, twtruckdist := watruckdist + wetruckdist]
  TruckWater[,c("watrucktime","watruckdist","wetrucktime","wetruckdist"):=NULL]
  
  # Join on the truck rail paths
  ShipmentRoutes <- merge(ShipmentRoutes, TruckWater,  by = c("oTAZ1", "dTAZ1"), all.x = TRUE)
  
  # Update progress log
  progressUpdate(subtaskprogress = 0.40, subtask = "Process Skim Data", prop = 1/6, dir = SCENARIO_LOG_PATH)
  
  ### Truck-Air
  
  # Inside the US, build connections to the set of airports
  # Outside the US, just use TAZ1 centroids as dummy airports
  # THe mode choice model adds signficant transfer time which is sufficent for access/egress time
  # identify closest oRNODE- Air RNODE and Air RNODE-dRNODE connections
  AllAirportTAZ1 <- rbind(AirportTAZ1, unique(TazGCD[TAZ1 %in% BASE_TAZ1_FOREIGN, .(TAZ1)]))
  AirAccess <- data.table(expand.grid(oTAZ1 = unique(TAZ1Pairs$oTAZ1), dTAZ1 = AllAirportTAZ1$TAZ1), key = c("oTAZ1", "dTAZ1"))
  AirAccess[TruckSkim, c("truckdist","trucktime") := list(i.truckdist, i.trucktime)]
  AirAccess[is.na(truckdist) & oTAZ1 == dTAZ1, c("truckdist","trucktime") := 0]
  AirAccess <- AirAccess[AirAccess[,.I[which.min(truckdist)], by = oTAZ1]$V1]

  AirEgress <- data.table(expand.grid(oTAZ1 = AllAirportTAZ1$TAZ1, dTAZ1 = unique(TAZ1Pairs$dTAZ1)), key = c("oTAZ1","dTAZ1"))
  AirEgress[TruckSkim, c("truckdist","trucktime") := list(i.truckdist, i.trucktime)]
  AirEgress[is.na(truckdist) & oTAZ1 == dTAZ1, c("truckdist","trucktime") := 0]
  AirEgress <- AirEgress[AirEgress[,.I[which.min(truckdist)], by = dTAZ1]$V1]
  
  # Combine elements into oTAZ1-TATAZ1-ATTAZ1-dRTAZ1 path
  TruckAir <- TAZ1Pairs[AirAccess[, .(oTAZ1, TATAZ1 = dTAZ1, aatrucktime = trucktime, aatruckdist = truckdist)], on = "oTAZ1", nomatch = 0]
  TruckAir <- TruckAir[AirEgress[, .(dTAZ1, ATTAZ1 = oTAZ1, aetrucktime = trucktime, aetruckdist = truckdist)], on = "dTAZ1", nomatch = 0]
  # Use GCD for airport to airport distances and time estimation
  TAZ1GCD <- TAZ1GCD[, .(GCD = mean(GCD)), keyby = .(oTAZ1, dTAZ1)]
  TruckAir <- TruckAir[TAZ1GCD[, .(TATAZ1 = oTAZ1, ATTAZ1 = dTAZ1, taairdist = GCD)], on = c("TATAZ1","ATTAZ1"), nomatch = 0]
  TruckAir[, taairtime := taairdist * 60 / ModeChoiceParameters[Variable == "AirSpeed", Value]]
  setkey(TruckAir, oTAZ1, dTAZ1)
  TruckAir[, tatrucktime := aatrucktime + aetrucktime]
  TruckAir[, tatruckdist := aatruckdist + aetruckdist]
  TruckAir[,c("aatrucktime","aatruckdist","aetrucktime","aetruckdist"):=NULL]
  
  # Join on the truck air paths
  ShipmentRoutes <- merge(ShipmentRoutes, TruckAir, by = c("oTAZ1", "dTAZ1"), all.x = TRUE)
  
  # Update progress log
  progressUpdate(subtaskprogress = 0.50, subtask = "Process Skim Data", prop = 1/6, dir = SCENARIO_LOG_PATH)
  
  ### Truck-Port
  AllPortTAZ1 <- rbind(PortTAZ1, unique(TazGCD[TAZ1 %in% BASE_TAZ1_FOREIGN, .(TAZ1)]))
  PortAccess <- data.table(expand.grid(oTAZ1 = unique(TAZ1Pairs$oTAZ1), dTAZ1 = AllPortTAZ1$TAZ1), key = c("oTAZ1", "dTAZ1"))
  PortAccess[TruckSkim, c("truckdist","trucktime") := list(i.truckdist, i.trucktime)]
  PortAccess[is.na(truckdist) & oTAZ1 == dTAZ1 & oTAZ1 %in% BASE_TAZ1_FOREIGN, c("truckdist","trucktime") := 0]
  PortAccess <- PortAccess[!is.na(truckdist)]
  PortAccess <- PortAccess[PortAccess[,.I[which.min(truckdist)], by = oTAZ1]$V1]
  
  PortEgress <- data.table(expand.grid(oTAZ1 = AllPortTAZ1$TAZ1, dTAZ1 = unique(TAZ1Pairs$dTAZ1)), key = c("oTAZ1","dTAZ1"))
  PortEgress[TruckSkim, c("truckdist","trucktime") := list(i.truckdist, i.trucktime)]
  PortEgress[is.na(truckdist) & oTAZ1 == dTAZ1 & dTAZ1 %in% BASE_TAZ1_FOREIGN, c("truckdist","trucktime") := 0]
  PortEgress <- PortEgress[!is.na(truckdist)]
  PortEgress <- PortEgress[PortEgress[,.I[which.min(truckdist)], by = dTAZ1]$V1]
  
  # Combine elements into oTAZ1-TPTAZ1-PTTAZ1-dTAZ1 path
  TruckPort <- TAZ1Pairs[PortAccess[, .(oTAZ1, TPTAZ1 = dTAZ1, patrucktime = trucktime, patruckdist = truckdist)], on = "oTAZ1", nomatch = 0]
  TruckPort <- TruckPort[PortEgress[, .(dTAZ1, PTTAZ1 = oTAZ1, petrucktime = trucktime, petruckdist = truckdist)], on = "dTAZ1", nomatch = 0]
  # These will only be available to international shipments, so make sure one end of the trip is overseas
  TruckPort <- TruckPort[oTAZ1 %in% BASE_TAZ1_FOREIGN | dTAZ1 %in% BASE_TAZ1_FOREIGN]
  # Use GCD for port to port distances and time estimation -- will be relatively accurate for single ocean trips, less for panama canal routes
  TruckPort <- TruckPort[TAZ1GCD[, .(TPTAZ1 = oTAZ1, PTTAZ1 = dTAZ1, tpwaterdist = GCD)], on = c("TPTAZ1","PTTAZ1"), nomatch = 0]
  TruckPort[, tpwatertime := tpwaterdist * 60 / ModeChoiceParameters[Variable == "WaterSpeed", Value]]
  setkey(TruckPort, oTAZ1, dTAZ1)
  TruckPort[, tptrucktime := patrucktime + petrucktime]
  TruckPort[, tptruckdist := patruckdist + petruckdist]
  TruckPort[,c("patrucktime","patruckdist","petrucktime","petruckdist"):=NULL]
  
  # Join on the truck port paths
  ShipmentRoutes <- merge(ShipmentRoutes, TruckPort, by = c("oTAZ1", "dTAZ1"), all.x = TRUE)
  
  # Update progress log
  progressUpdate(subtaskprogress = 0.60, subtask = "Process Skim Data", prop = 1/6, dir = SCENARIO_LOG_PATH)
  
  ### Truck-Rail-Port
  # RailAccess -- add records for international zones
  RailAccess <- rbind(RailAccess[!oTAZ1 %in% BASE_TAZ1_FOREIGN], data.table(oTAZ1 = BASE_TAZ1_FOREIGN, dTAZ1 = BASE_TAZ1_FOREIGN, truckdist = 0, trucktime = 0))
  # RailEgress -- add records for international zones
  RailEgress <- rbind(RailEgress[!dTAZ1 %in% BASE_TAZ1_FOREIGN], data.table(oTAZ1 = BASE_TAZ1_FOREIGN, dTAZ1 = BASE_TAZ1_FOREIGN, truckdist = 0, trucktime = 0))
  
  # Identify rail to ports connections
  RailPortAccess <- data.table(expand.grid(oTAZ1 = unique(RailAccess$dTAZ1), dTAZ1 = AllPortTAZ1$TAZ1), key = c("oTAZ1", "dTAZ1"))
  RailPortAccess[RailSkim, c("raildist","railtime") := list(i.raildist, i.railtime)]
  RailPortAccess[is.na(raildist) & oTAZ1 == dTAZ1 & oTAZ1 %in% BASE_TAZ1_FOREIGN, c("raildist","railtime") := 0]
  RailPortAccess <- RailPortAccess[!is.na(raildist)]
  RailPortAccess <- RailPortAccess[RailPortAccess[,.I[which.min(raildist)], by = oTAZ1]$V1]
  
  RailPortEgress <- data.table(expand.grid(oTAZ1 = AllPortTAZ1$TAZ1, dTAZ1 = unique(RailEgress$oTAZ1)), key = c("oTAZ1","dTAZ1"))
  RailPortEgress[RailSkim, c("raildist","railtime") := list(i.raildist, i.railtime)]
  RailPortEgress[is.na(raildist) & oTAZ1 == dTAZ1 & dTAZ1 %in% BASE_TAZ1_FOREIGN, c("raildist","railtime") := 0]
  RailPortEgress <- RailPortEgress[!is.na(raildist)]
  RailPortEgress <- RailPortEgress[RailPortEgress[,.I[which.min(raildist)], by = dTAZ1]$V1]
  
  # Combine the truck-rail legs on to the rail-port legs
  RailPortAccess <- merge(RailAccess[,.(oTAZ1, TRP1TAZ1 = dTAZ1, truckdist, trucktime)], 
                          RailPortAccess[,.(TRP1TAZ1 = oTAZ1, TRP2TAZ1 = dTAZ1, raildist, railtime)],
                          by = "TRP1TAZ1")
  
  RailPortEgress <- merge(RailEgress[,.(PRT2TAZ1 = oTAZ1, dTAZ1, truckdist, trucktime)], 
                          RailPortEgress[,.(PRT1TAZ1 = oTAZ1, PRT2TAZ1 = dTAZ1, raildist, railtime)],
                          by = "PRT2TAZ1")
  
  # Combine elements into oTAZ1-TRP1TAZ1-TRP2TAZ1-PRT1TAZ1-PRTTAZ12-dTAZ1 path
  TruckRailPort <- TAZ1Pairs[RailPortAccess[, .(oTAZ1, TRP1TAZ1, TRP2TAZ1, patrucktime = trucktime, patruckdist = truckdist, paraildist = raildist, parailtime = railtime)], on = "oTAZ1", nomatch = 0]
  TruckRailPort <- TruckRailPort[RailPortEgress[, .(dTAZ1, PRT1TAZ1, PRT2TAZ1, petrucktime = trucktime, petruckdist = truckdist, perailtime = railtime, peraildist = raildist)], on = "dTAZ1", nomatch = 0]
  # These will only be available to international shipments, so make sure one end of the trip is overseas
  TruckRailPort <- TruckRailPort[oTAZ1 %in% BASE_TAZ1_FOREIGN | dTAZ1 %in% BASE_TAZ1_FOREIGN]
  # Use GCD for port to port distances and time estimation -- will be relatively accurate for single ocean trips, less for panama canal routes
  TruckRailPort <- TruckRailPort[TAZ1GCD[, .(TRP2TAZ1 = oTAZ1, PRT1TAZ1 = dTAZ1, trpwaterdist = GCD)], on = c("TRP2TAZ1","PRT1TAZ1"), nomatch = 0]
  TruckRailPort[, trpwatertime := trpwaterdist * 60 / ModeChoiceParameters[Variable == "WaterSpeed", Value]]
  setkey(TruckRailPort, oTAZ1, dTAZ1)
  TruckRailPort[, trptrucktime := patrucktime + petrucktime]
  TruckRailPort[, trptruckdist := patruckdist + petruckdist]
  TruckRailPort[, trprailtime := parailtime + perailtime]
  TruckRailPort[, trpraildist := paraildist + peraildist]
  TruckRailPort[,c("patrucktime","patruckdist","petrucktime","petruckdist", 
                   "parailtime","paraildist","perailtime","peraildist"):=NULL]

  # Join on the truck port paths
  ShipmentRoutes <- merge(ShipmentRoutes, TruckRailPort, by = c("oTAZ1", "dTAZ1"), all.x = TRUE)
  
  # Update progress log
  progressUpdate(subtaskprogress = 0.70, subtask = "Process Skim Data", prop = 1/6, dir = SCENARIO_LOG_PATH)
  
  ## Truck-PipePetrol
  
  # Identify closest petrol pipeline access and egress connections
  PipePetrolAccess <- data.table(expand.grid(oTAZ1 = unique(TAZ1Pairs$oTAZ1), dTAZ1 = PipePetrolTAZ1$TAZ1), key = c("oTAZ1","dTAZ1"))
  PipePetrolAccess[TruckSkim, c("truckdist","trucktime") := list(i.truckdist, i.trucktime)]
  PipePetrolAccess <- PipePetrolAccess[PipePetrolAccess[,.I[which.min(truckdist)], by = oTAZ1]$V1]

  PipePetrolEgress <- data.table(expand.grid(oTAZ1 = PipePetrolTAZ1$TAZ1, dTAZ1 = unique(TAZ1Pairs$dTAZ1)), key = c("oTAZ1","dTAZ1"))
  PipePetrolEgress[TruckSkim, c("truckdist","trucktime") := list(i.truckdist, i.trucktime)]
  PipePetrolEgress <- PipePetrolEgress[PipePetrolEgress[,.I[which.min(truckdist)], by = dTAZ1]$V1]

  # Combine elements into oTAZ1-TPPTAZ1-PPTTAZ1-dTAZ1 path
  TruckPipePetrol <- TAZ1Pairs[PipePetrolAccess[, .(oTAZ1, TPPTAZ1 = dTAZ1, ppatrucktime = trucktime, ppatruckdist = truckdist)], on = "oTAZ1", nomatch = 0]
  TruckPipePetrol <- TruckPipePetrol[PipePetrolEgress[, .(dTAZ1, PPTTAZ1 = oTAZ1, ppetrucktime = trucktime, ppetruckdist = truckdist)], on = "dTAZ1", nomatch = 0]
  TruckPipePetrol <- TruckPipePetrol[PipePetrolSkim[, .(TPPTAZ1 = oTAZ1, PPTTAZ1 = dTAZ1, pptpipedist = pipedist, pptpipetime = pipetime)], on = c("TPPTAZ1","PPTTAZ1"), nomatch = 0]
  setkey(TruckPipePetrol, oTAZ1, dTAZ1)
  TruckPipePetrol[, tpptrucktime := ppatrucktime + ppetrucktime]
  TruckPipePetrol[, tpptruckdist := ppatruckdist + ppetruckdist]
  TruckPipePetrol[,c("ppatrucktime","ppatruckdist","ppetrucktime","ppetruckdist"):=NULL]

  # Join on the truck petrol pipeline paths
  ShipmentRoutes <- merge(ShipmentRoutes, TruckPipePetrol,  by = c("oTAZ1", "dTAZ1"), all.x = TRUE)
  
  # Update progress log
  progressUpdate(subtaskprogress = 0.80, subtask = "Process Skim Data", prop = 1/6, dir = SCENARIO_LOG_PATH)
  
  ## Truck-PipeCrude
  
  # Identify closest petrol pipeline access and egress connections
  PipeCrudeAccess <- data.table(expand.grid(oTAZ1 = unique(TAZ1Pairs$oTAZ1), dTAZ1 = PipeCrudeTAZ1$TAZ1), key = c("oTAZ1","dTAZ1"))
  PipeCrudeAccess[TruckSkim, c("truckdist","trucktime") := list(i.truckdist, i.trucktime)]
  PipeCrudeAccess <- PipeCrudeAccess[PipeCrudeAccess[,.I[which.min(truckdist)], by = oTAZ1]$V1]
  
  PipeCrudeEgress <- data.table(expand.grid(oTAZ1 = PipeCrudeTAZ1$TAZ1, dTAZ1 = unique(TAZ1Pairs$dTAZ1)), key = c("oTAZ1","dTAZ1"))
  PipeCrudeEgress[TruckSkim, c("truckdist","trucktime") := list(i.truckdist, i.trucktime)]
  PipeCrudeEgress <- PipeCrudeEgress[PipeCrudeEgress[,.I[which.min(truckdist)], by = dTAZ1]$V1]
  
  # Combine elements into oTAZ1-TPCTAZ1-PCTTAZ1-dTAZ1 path
  TruckPipeCrude <- TAZ1Pairs[PipeCrudeAccess[, .(oTAZ1, TPCTAZ1 = dTAZ1, pcatrucktime = trucktime, pcatruckdist = truckdist)], on = "oTAZ1", nomatch = 0]
  TruckPipeCrude <- TruckPipeCrude[PipeCrudeEgress[, .(dTAZ1, PCTTAZ1 = oTAZ1, pcetrucktime = trucktime, pcetruckdist = truckdist)], on = "dTAZ1", nomatch = 0]
  TruckPipeCrude <- TruckPipeCrude[PipeCrudeSkim[, .(TPCTAZ1 = oTAZ1, PCTTAZ1 = dTAZ1, pctpipedist = pipedist, pctpipetime = pipetime)], on = c("TPCTAZ1","PCTTAZ1"), nomatch = 0]
  setkey(TruckPipeCrude, oTAZ1, dTAZ1)
  TruckPipeCrude[, tpctrucktime := pcatrucktime + pcetrucktime]
  TruckPipeCrude[, tpctruckdist := pcatruckdist + pcetruckdist]
  TruckPipeCrude[,c("pcatrucktime","pcatruckdist","pcetrucktime","pcetruckdist"):=NULL]
  
  # Join on the truck petrol pipeline paths
  ShipmentRoutes <- merge(ShipmentRoutes, TruckPipeCrude,  by = c("oTAZ1", "dTAZ1"), all.x = TRUE)
  
  # Update progress log
  progressUpdate(subtaskprogress = 0.90, subtask = "Process Skim Data", prop = 1/6, dir = SCENARIO_LOG_PATH)
  
  ## Truck-PipeCoalNEC
  
  # Identify closest petrol pipeline access and egress connections
  PipeCoalNECAccess <- data.table(expand.grid(oTAZ1 = unique(TAZ1Pairs$oTAZ1), dTAZ1 = PipeCoalNECTAZ1$TAZ1), key = c("oTAZ1","dTAZ1"))
  PipeCoalNECAccess[TruckSkim, c("truckdist","trucktime") := list(i.truckdist, i.trucktime)]
  PipeCoalNECAccess <- PipeCoalNECAccess[PipeCoalNECAccess[,.I[which.min(truckdist)], by = oTAZ1]$V1]
  
  PipeCoalNECEgress <- data.table(expand.grid(oTAZ1 = PipeCoalNECTAZ1$TAZ1, dTAZ1 = unique(TAZ1Pairs$dTAZ1)), key = c("oTAZ1","dTAZ1"))
  PipeCoalNECEgress[TruckSkim, c("truckdist","trucktime") := list(i.truckdist, i.trucktime)]
  PipeCoalNECEgress <- PipeCoalNECEgress[PipeCoalNECEgress[,.I[which.min(truckdist)], by = dTAZ1]$V1]
  
  # Combine elements into oTAZ1-TPNTAZ1-PNTTAZ1-dTAZ1 path
  TruckPipeCoalNEC <- TAZ1Pairs[PipeCoalNECAccess[, .(oTAZ1, TPNTAZ1 = dTAZ1, pnatrucktime = trucktime, pnatruckdist = truckdist)], on = "oTAZ1", nomatch = 0]
  TruckPipeCoalNEC <- TruckPipeCoalNEC[PipeCoalNECEgress[, .(dTAZ1, PNTTAZ1 = oTAZ1, pnetrucktime = trucktime, pnetruckdist = truckdist)], on = "dTAZ1", nomatch = 0]
  TruckPipeCoalNEC <- TruckPipeCoalNEC[PipeCoalNECSkim[, .(TPNTAZ1 = oTAZ1, PNTTAZ1 = dTAZ1, pntpipedist = pipedist, pntpipetime = pipetime)], on = c("TPNTAZ1","PNTTAZ1"), nomatch = 0]
  setkey(TruckPipeCoalNEC, oTAZ1, dTAZ1)
  TruckPipeCoalNEC[, tpntrucktime := pnatrucktime + pnetrucktime]
  TruckPipeCoalNEC[, tpntruckdist := pnatruckdist + pnetruckdist]
  TruckPipeCoalNEC[,c("pnatrucktime","pnatruckdist","pnetrucktime","pnetruckdist"):=NULL]
  
  # Join on the truck petrol pipeline paths
  ShipmentRoutes <- merge(ShipmentRoutes, TruckPipeCoalNEC,  by = c("oTAZ1", "dTAZ1"), all.x = TRUE)
  
  # Update progress log
  progressUpdate(subtaskprogress = 1, subtask = "Process Skim Data", prop = 1/6, dir = SCENARIO_LOG_PATH)
  
  # Return results
  return(ShipmentRoutes)
  
}
