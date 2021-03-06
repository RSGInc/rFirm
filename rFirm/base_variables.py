# #Define application geography
# name TAZ1 (national/large zones) and TAZ2 (regional/small zones)
# BASE_ZONE_SYSTEMS <- c(TAZ1 = "TAZ", TAZ2 = "TAZ")

# Define state FIPS for internal model region and buffer
# BASE_STATE_FIPS_INTERNAL <- c(41, 53)
# BASE_STATE_FIPS_BUFFER <- c(41, 53)

# Define TAZ ranges for different elements of the model region
# BASE_TAZ1_INTERNAL <- 1L:2147L #range of national TAZs that covers the model region
# BASE_TAZ1_HALO <- c(1L:2147L)  #range of national TAZs that covers model region plus halo
# range of national TAZs that covers model region and complete states
# BASE_TAZ1_HALO_STATES <- 1L:2549L

# range of national TAZs that covers model region and complete states
BASE_TAZ1_HALO_STATES = range(1, 2549)

# BASE_TAZ1_FOREIGN <- 2679L:2688L #range of foreign TAZs
# BASE_TAZ2_INTERNAL <- 1L:2147L #range of regional TAZs that covers the model region
# BASE_TAZ2_EXTERNAL <- 2148:2162L #range of regional TAZs that are external stations
#
# #Define application time periods, run years, and other temporal inputs
# BASE_SCENARIO_YEARS <- c(2015, 2020, 2025, 2030, 2035, 2040)
#
# Define other application parameters

# production value threshold for supplier selection
BASE_PROVALTHRESHOLD = 0.8

# BASE_SUPPLIERS_SAMPLED <- 10L #number of suppliers to sample in suuplier selection
# BASE_MIN_COMMODITY_FLOW <- 1L #minimum annual rounded commodity flow in Tons to simulate
# minimum shipment size in pounds to avoid simulation of many small shipments
# BASE_MIN_SHIPMENT_WEIGHT <- 10L
# BASE_SEED_VALUE  <- 5 #seed for sampling to ensure repeatable results
# point in trip for time period allocation, from ("START", "MIDDLE", "END")
# BASE_TIME_PERIOD_TRIP_POINT <- "START"

# NAICS 481 air, 482 rail, 483 water, 493 warehouse and storage
NAICS3_WAREHOUSE = [481, 482, 483, 484]
