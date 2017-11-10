#-----------------------------------------------------------------------------------
#LEHD Processing (to be used for employment rankings in firm synthesis code)
#-----------------------------------------------------------------------------------

# Set working directory and install needed packages
#setwd("C:/Users/kaveh.shabani/Desktop/NFM")
setwd("E:/Projects/Clients/FHWA/national_freight")

install.packages(("RCurl"))
library("RCurl", lib.loc="C:/Users/kaveh.shabani/Documents/R/win-library/3.3")
install.packages(("rgdal"))
library("rgdal", lib.loc="C:/Users/kaveh.shabani/Documents/R/win-library/3.3")
library("RSGFAF", lib.loc="./lib/pkgs/library")
library("rFreight", lib.loc="./lib/pkgs/library")
library(data.table)

#Downloads data for specified columns and appends the data from all states to one single column. 
#LEHD documentation: https://lehdmap.did.census.gov/data/lodes/LODES7/LODESTechDoc7.2.pdf
# States of interest
baseurl <- "https://lehdmap.did.census.gov/data/lodes/LODES7" ###TODO: website certification issue
outdf <- data.frame()

state.abb <- append(state.abb, "DC") #add missing states
sort(state.abb)

for (i in 1:length(state.abb)) {
  st <- tolower(state.abb[i]) #test: st <- "al"
  fname <- paste0(st, "_wac_S000_JT00_2014.csv.gz")
  
  #if (url.exists(paste(baseurl, st, "wac", fname, sep = "/"))) 
  {
    
    #download.file(paste(baseurl, st, "wac", fname, sep = "/"), paste("./dev/Data/LEHD", fname, sep = "/"), method = "curl")
    stdf <- read.csv(gzfile(paste("./dev/Data/LEHD", fname, sep = "/")), as.is = TRUE)
    stdf$w_geocode <- as.character(stdf$w_geocode)
    stdf$blockid <- stdf$w_geocode # get block-level
    stdf <- stdf[, c("blockid", "C000", paste0("CNS", c(paste0(0, 1:9), 10:20)))] # C000 = Total jobs, CNS01-CNS20 = jobs by NAICS 2 digit categories
    stdf <- aggregate(stdf[, c("C000", paste0("CNS", c(paste0(0, 1:9), 10:20)))],
                      list(blockid = stdf$blockid), sum, na.rm = TRUE)
    names(stdf)[2] <- c("Emp_Tot")
    outdf <- rbind(outdf, stdf)
    
  } #else {
    #cat("Warning: URL does not exist.", paste(baseurl, st, "wac", fname, sep = "/"))
  #}
}

lehddt <- data.table(outdf)
rm(stdf, outdf)
fwrite(lehddt, "./dev/Data/LEHD/lehd_data.csv")

# CNS01 Num Number of jobs in NAICS sector 11 (Agriculture, Forestry, Fishing and Hunting) 
# CNS02 Num Number of jobs in NAICS sector 21 (Mining, Quarrying, and Oil and Gas Extraction) 
# CNS03 Num Number of jobs in NAICS sector 22 (Utilities) 
# CNS04 Num Number of jobs in NAICS sector 23 (Construction) 
# CNS05 Num Number of jobs in NAICS sector 31-33 (Manufacturing) 
# CNS06 Num Number of jobs in NAICS sector 42 (Wholesale Trade) 
# CNS07 Num Number of jobs in NAICS sector 44-45 (Retail Trade) 
# CNS08 Num Number of jobs in NAICS sector 48-49 (Transportation and Warehousing) 
# CNS09 Num Number of jobs in NAICS sector 51 (Information) 
# CNS10 Num Number of jobs in NAICS sector 52 (Finance and Insurance) 
# CNS11 Num Number of jobs in NAICS sector 53 (Real Estate and Rental and Leasing) 
# CNS12 Num Number of jobs in NAICS sector 54 (Professional, Scientific, and Technical Services) 
# CNS13 Num Number of jobs in NAICS sector 55 (Management of Companies and Enterprises) 
# CNS14 Num Number of jobs in NAICS sector 56 (Administrative and Support and Waste Management and Remediation Services) 
# CNS15 Num Number of jobs in NAICS sector 61 (Educational Services) 
# CNS16 Num Number of jobs in NAICS sector 62 (Health Care and Social Assistance) 
# CNS17 Num Number of jobs in NAICS sector 71 (Arts, Entertainment, and Recreation) 
# CNS18 Num Number of jobs in NAICS sector 72 (Accommodation and Food Services) 
# CNS19 Num Number of jobs in NAICS sector 81 (Other Services [except Public Administration]) 
# CNS20 Num Number of jobs in NAICS sector 92 (Public Administration)

# Prepare for shapefiles processing
#install.packages("maps")
library(maps)
data(state.fips) #from maps package

#add missing states
state.fips <- rbind( state.fips, data.frame("fips"= 2, "ssa"= 2, "region"= 0, "division"= 0, "abb"= "AK", "polyname"= "alaska"))
state.fips <- rbind( state.fips, data.frame("fips"= 15, "ssa"= 12, "region"= 0, "division"= 0, "abb"= "HI", "polyname"= "hawaii"))

state.fips$fips <- ifelse (state.fips$fips < 10, paste0("0", state.fips$fips), state.fips$fips)
statenum <- unique(state.fips$fips[state.fips$abb %in% state.abb])
downdir = "./dev/Data/GIS/Census Blocks"

# Function to download and process the block shapefiles
# Returns file location of the points layer R data file
down_blk_shp <- function(statenum, downdir, overwrite = FALSE) {
  
  # File name for this state
  fname <- paste0("stpoints", statenum, ".Rdata")
  
  # Download and save only if no file exists or if overwrite is TRUE
  if (overwrite | !file.exists(file.path(downdir, fname))) {
    
    # Download
    #download.file(paste0("ftp://ftp2.census.gov/geo/tiger/TIGER2014/TABBLOCK/tl_2014_", statenum, "_tabblock10.zip"),
                  #file.path(downdir, paste0("tl_2014_", statenum, "_tabblock10.zip")))
    
    # Unzip
    cat("Unzipping...\n")
    #unzip(file.path(downdir, paste0("tl_2014_", statenum, "_tabblock10.zip")), exdir = downdir)
    
    # Read
    stblock <- readOGR(downdir, paste0("tl_2014_", statenum, "_tabblock10"))
    
    # convert to a points layer -- centroids
    stpoints <- SpatialPointsDataFrame(coordinates(stblock),
                                       data.frame(stblock),
                                       bbox = bbox(stblock),
                                       proj4string = CRS(proj4string(stblock)))
    save(stpoints, file = file.path(downdir, fname))
    
  }
  
  return(file.path(downdir, fname))
}  

# Read in the model TAZ shapefile
#########################################################################################
#########################################################################################
# NFM TAZ shapefile
nfmtaz <- readOGR("./dev/Data/GIS/NUMA Zones","NUMA_Polygon")
plot(nfmtaz)
head(nfmtaz@data)
proj4string(nfmtaz) #[1] "+proj=longlat +datum=NAD83 +no_defs +ellps=GRS80 +towgs84=0,0,0"

# Convert NFM TAZ to a points layer of TAZ centroids
nfmtazp <- SpatialPointsDataFrame(coordinates(nfmtaz),
                                  data.frame(nfmtaz),
                                  bbox = bbox(nfmtaz),
                                  proj4string = CRS(proj4string(nfmtaz)))
plot(nfmtaz)

#FAF4 zones
faf4  <- readOGR("./dev/Data/GIS/FAF4 Zones","FAF4_REGIONS")
plot(faf4)
head(faf4@data)
proj4string(faf4) #[1] "+proj=longlat +datum=NAD83 +no_defs +ellps=GRS80 +towgs84=0,0,0"
# Convert to same CRS as the NFM TAZ layer
faf4.longlat <- faf4
faf4 <- spTransform(faf4, CRS(proj4string(nfmtaz)))

# Convert FAF TAZ to a points layer of TAZ centroids
faf4p <- SpatialPointsDataFrame(coordinates(faf4),
                                data.frame(faf4),
                                bbox = bbox(faf4),
                                proj4string = CRS(proj4string(faf4)))
plot(nfmtaz)
plot(faf4p, add = TRUE)

#FAF4 international
faf4int  <- readOGR("./dev/Data/GIS/FAF International Zones","FAF_International_Zones")
plot(faf4int)
head(faf4int@data)
proj4string(faf4int) #[1] "+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0"

#FAF zone data
data(FAFZones) #RSGFAF
head(FAFZones)

#County layer
county <- readOGR("./dev/Data/GIS/US Counties","tl_2010_us_county10")
plot(county)
head(county@data)
proj4string(county) #[1] "+proj=longlat +datum=NAD83 +no_defs +ellps=GRS80 +towgs84=0,0,0"
county <- spTransform(county, CRS(proj4string(nfmtaz)))

#Save
save(list = ls(all = TRUE), file = "./dev/Data/GIS/NFM_Spatial.RData")

#load the spatial data workspace 
load(file = "./dev/Data/GIS/NFM_Spatial.RData")

#########################################################################################
#########################################################################################

# Function to map block points to TAZs and update shapefiles
proc_blk_shp <- function(blockpoints, downdir) {
  
  # Load the set of block points
  load(blockpoints)
  
  # Convert to same CRS at the taz layer
  stpoints <- spTransform(stpoints, CRS(proj4string(nfmtaz)))
  
  # Identify the taz that each block centroid is in
  sttazlist <- over(stpoints, nfmtaz)
  
  # Add TAZ to the block data
  stpoints@data <- cbind(stpoints@data, sttazlist)
  
  # Create a character version of the blockid to join to the LEHD data
  stpoints$blockid <- as.character(stpoints$GEOID10)
  
  # Save and return results
  newfname <- gsub(x = blockpoints, pattern = "\\.R", replacement = "taz.R")
  save(stpoints, file = newfname)
  return(stpoints@data)
}

# Process state data and assign blocks to TAZs
#stateblkpts <- down_blk_shp("01", "./dev/Data/GIS/Census Blocks", overwrite = TRUE)
stateblkpts <- lapply(X = statenum, FUN = down_blk_shp, downdir = "./dev/Data/GIS/Census Blocks", overwrite = FALSE)

#stdatalist <- proc_blk_shp(stateblkpts, "./dev/Data/GIS/Census Blocks")
stdatalist  <- lapply(X = stateblkpts, FUN = proc_blk_shp, downdir = "./dev/Data/GIS/Census Blocks")

# Join on the TAZ to the LEHD data amd tabulate block data into TAZ totals
stdatadt <- data.table(do.call(args = stdatalist, what = rbind))
#stdatadt <- data.table(stdatalist)
lehddt[, blockid := ifelse(nchar(blockid) == 14, paste0("0", blockid), blockid)]
###TODO: Check TAZ = SMZRMZ
lehddt <- merge(lehddt, stdatadt[, .(blockid, TAZ = FID_1, StateFIPS = STATEFP10, CountyFIPS = COUNTYFP10)], all.x = TRUE, by = "blockid")
lehdtaz <- lehddt[!is.na(TAZ), lapply(.SD,sum),by=.(TAZ, CountyFIPS, StateFIPS),.SDcols=2:22][order(TAZ, CountyFIPS, StateFIPS)]

# Save that file for later use
fwrite(lehdtaz, "./dev/Data/lehd_taz.csv")
lehdtaz <- fread("./dev/Data/lehd_taz.csv")

# Prepare a final LEHD employment by TAZ table to use later to assign CBP establishments and firms from a county to a TAZ
#-------------------------------------------------------------------------------------------------------------------------
# Reshape lehdtaz to long and recode the fields names to match NAICS groups
lehdtazm <- melt(lehdtaz[,!"Emp_Tot", with = FALSE], id.vars = c("TAZ", "CountyFIPS", "StateFIPS" ), variable.name = "CNS", value.name = "LEmp")
lehdtazm[, n2 := c(11, 21:23, 3133, 42, 4445, 4849, 51:56, 61, 62, 71, 72, 81, 92)[CNS]]
lehdtazm[, CNS := NULL]

# Calculate cumulative percentage of employment by industry by county
lehdtazm[, PctEmp := LEmp / sum(LEmp), by = .(n2, CountyFIPS, StateFIPS)]
lehdtazm[is.nan(PctEmp), PctEmp := 0] # fix any NaNs
setkey(lehdtazm, StateFIPS, CountyFIPS, n2, PctEmp)
lehdtazm[, CumPctEmp := cumsum(PctEmp), by = .(StateFIPS, CountyFIPS, n2)]

# Categorize into the 1-10 ranking for use in the allocation process
lehdtazm[, EmpRank := ifelse(CumPctEmp == 0, 1, ceiling(CumPctEmp * 10))]

# Convert county and state to integer for consistency
lehdtazm[, CountyFIPS := as.integer(as.character(CountyFIPS))]
lehdtazm[, StateFIPS  := as.integer(as.character(StateFIPS))]

# Save that file for later use
fwrite(lehdtazm, "./dev/Data/lehdtazm.csv")
