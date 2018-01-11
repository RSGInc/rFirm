rm(list=ls())

# library("data.table")
# library("RSGFAF)

setwd("/Users/jeff.doyle/work/afreight/national_freight")

dest_data_dir <- "/Users/jeff.doyle/work/afreight/national_freight/data/"

### national_freight data files


SUSB_Firms <- read.csv(paste(r_national_freight_data_dir, "SUSB_Firms.csv", sep=""))
fwrite(SUSB_Firms, paste(dest_data_dir, "SUSB_Firms.csv", sep=""))

corresp_taz_faf4 <- read.csv(paste(r_national_freight_data_dir, "corresp_taz_faf4.csv", sep=""))
fwrite(corresp_taz_faf4, paste(dest_data_dir, "corresp_taz_faf4.csv", sep=""))

corresp_taz_fips <- read.csv(paste(r_national_freight_data_dir, "corresp_taz_fips.csv", sep=""))
fwrite(corresp_taz_fips, paste(dest_data_dir, "corresp_taz_fips.csv", sep=""))


### RSGFAF data files

RSGFAF_data_dir <- "/Users/jeff.doyle/work/RSGFAF/data/"

load(paste(RSGFAF_data_dir, "County_to_FAFZone.RData", sep=""))
fwrite(County_to_FAFZone, paste(dest_data_dir, "County_to_FAFZone.csv", sep=""))


### rfreight data files
rfreight_data_dir <- "/Users/jeff.doyle/work/rFreight/data/"

load(file = paste(rfreight_data_dir, "correspondences.rda", sep=""))
fwrite(NAICS2007_to_NAICS2007io, paste(dest_data_dir, "NAICS2007_to_NAICS2007io.csv", sep=""))
fwrite(NAICS2007io_to_SCTG, paste(dest_data_dir, "NAICS2007io_to_SCTG.csv", sep=""))
fwrite(NAICS2007, paste(dest_data_dir, "NAICS2007.csv", sep=""))
