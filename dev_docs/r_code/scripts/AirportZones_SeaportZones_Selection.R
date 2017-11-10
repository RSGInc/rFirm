library(data.table)
library(rgdal)

# Load airports and seaports locations
Intermodal.shp <- readOGR("./scenarios/base/inputs", layer = "National_IntermodalFacilities08012017", verbose = FALSE)
Intermodal <- data.table(Intermodal.shp@data)

AirportZones <- Intermodal[Fac_Type == "Airport" & SumFreight > 1000,
                           .(Fac_ID, Fac_Name, TAZ1 = NUMA, SumFreight)]
setorder(AirportZones, -SumFreight)


SeaportZones <- Intermodal[Fac_Type == "Major Port",
                           .(Fac_ID, Fac_Name, TAZ1 = NUMA, SumFreight)]
setorder(SeaportZones, -SumFreight)

fwrite(AirportZones, "./lib/data/AirportZones.csv")
fwrite(SeaportZones, "./lib/data/SeaportZones.csv")
