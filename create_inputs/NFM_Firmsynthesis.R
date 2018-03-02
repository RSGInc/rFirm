#-----------------------------------------------------------------------------------
## Prepare inputs for Firm Synthesis
#-----------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------
## Load libraries for Firm Synthesis
#-----------------------------------------------------------------------------------

library(data.table)
library(rFreight, lib.loc = "pkgs")

#Inputs
#------

# 2014 County Business Patterns Data
# From BEA website-National County Business Pattern-Number of employees and businesses
# (by employee size group) by state, county and NAICS (2,3,4,5 and 6 digits)
# USe 2014 as latest in 2007 NAICS codes, consistent with the Input Output data
#if(!file.exists("Data/CBP/cbp14co.txt")){
#  # Download the file if not exist
#  download.file("ftp://ftp.census.gov/econ2014/CBP_CSV/cbp14co.zip",
#                "./Data/CBP/cbp14co.zip")
#  unzip("./Data/CBP/cbp14co.zip",exdir="./Data/CBP")
#}
unzip("inputs/cbp14co.zip")
cbp<-fread("cbp14co.txt")
file.remove("cbp14co.txt")

#correspondece between FAF zones and counties
County_to_FAFZone <- read.csv("inputs/County_to_FAFZone.csv") #from RSGFAF package

#correspondece between FAF zones and Model TAZs (NUMA Zones)
FAF_NUMA    <- fread ("inputs/corresp_taz_faf4.csv")
FIPS_NUMA   <- fread ("inputs/corresp_taz_fips.csv")  #TAZs that have two counties inside them : (304,823,828,1432,4381,4568) ##TODO:Check this?

FIPS_NUMA[is.na(FIPS_NUMA)] <- 0

#naics-SCTG makers list
# NAICS2007_to_NAICS2007io	#from the rFreight package
# NAICS2007io_to_SCTG       #from the rFreight package (only includes n2<52) ##TODO: Check this?
# 
# #labels for naics
# NAICS2007                 #from the rFreight package
# NAICS2012                 #from the rFreight package

#Process CBP data
#----------------
#Remove the fipscty == 999 (data contains totals rows for state by industry)
#Get just rows with complete 6 digit naics code
cbp <- cbp[fipscty != "999",]
cbp <- cbp[grep("[0-9]{6}",naics,100),]

#Summarize employment and establishments by naics and taz
#add the FAFZONE and then the TAZ for areas outside of model area
cbp[, StateFIPS := as.integer(fipstate)]
cbp[, CountyFIPS := as.integer(fipscty)]
cbp[County_to_FAFZone, FAF4 := i.FAFZone_4.2, on = c("StateFIPS", "CountyFIPS")] 
unique(cbp[is.na(FAF4),list(StateFIPS,CountyFIPS)]) #check for any missing
#Update missing for Alaska
#cbp[is.na(FAF4), FAF4 := 20L]

#join on the TAZ (one per FAFZONE)
cbp[FIPS_NUMA, TAZ := i.TAZ, on = c("StateFIPS", "CountyFIPS")]
#colSums(is.na(cbp)) #check for any NAs in the table

#Sum establishment size categories into 8 categories
cbp <- cbp[, .(emp=sum(emp),est=sum(est),e1=sum(n1_4,n5_9,n10_19),e2=sum(n20_49,n50_99),e3=sum(n100_249),e4=sum(n250_499),
               e5=sum(n500_999),e6=sum(n1000_1,n1000_2),e7=sum(n1000_3),e8=sum(n1000_4)), 
               by = .(naics, TAZ, CountyFIPS, StateFIPS, FAF4)][order(TAZ,naics)]

#Read-in the SUSB National Firms Data
SUSB_F <- fread("inputs/SUSB_Firms.csv")
SUSB_F[is.na(SUSB_F)] <- 0    #sum(colSums(SUSB_F[,-1])) [1] 5,930,979

#Merge SUSB with CBP
cbp[, emp := 10*e1 + 60*e2 +  175*e3 + 375*e4 + 750*e5 + 1750*e6 + 3750*e7 + 5000*e8]
cbp[, naics := as.integer(naics)]
cbp[, 8:15 := NULL]

#sum(cbp$emp)              #sum(cbp$est)
#[1] 176,153,420          #[1] 7,526,881


cbp[, prob := emp/sum(emp), by = naics]
cbp <- merge(cbp, SUSB_F, by = c("naics"), allow.cartesian = TRUE, all.x = TRUE) ###TODO: Firms might get assigned to incorrect zones!
cbp[, 9:31 := lapply(.SD, function(x) bucketRound(x * cbp[['prob']] )), .SDcols = 9:31] ###TODO: Can probably improve the approach!
cbp[, prob := NULL]
cbp[, est := NULL]
cbp[, emp := NULL]


##############################################################################
# Read in the rankings table (output of the LEHD data processing code) and the processed cbp table
lehdtazm <- fread("inputs/lehdtazm.csv")

# Enumerate the CBP firms
#-----------------------------------------------
# Melt to create separate rows for each firm size category
cbp.firms <- melt(cbp, measure.vars = paste0("f", 1:23), variable.name = "fsizecat", value.name = "firms")
cbp.firms[, fsizecat := as.integer(fsizecat)] #convert fsizecat to an integer (1:23)
rm(cbp, SUSB_F)

# Remove records where number of firms is 0
cbp.firms <- cbp.firms[firms > 0]

# Enumerate the firms using the firms variable
cbp.firms <- cbp.firms[rep(seq_len(cbp.firms[, .N]), firms)]

# Estimate the number of employees
cbp.firms[, emp := c(2, 7, 12, 17, 22, 27, 32, 37, 44, 62, 87, 124, 174, 249, 349, 449, 624, 874, 1249, 1749, 2249, 3749, 5000)[fsizecat]]
cbp.firms[, firms := NULL]
cbp.firms[, TAZ := NULL]

# Assign firms from counties to TAZs
#-----------------------------------------------------------------
# Add an id and n2
cbp.firms[, firmid := .I]
cbp.firms[, n2 := as.integer(substr(naics, 1, 2))]

# Assign specific NAICS categories which are used to locate businesses to tazs
cbp.firms[n2 %in% c(31, 32, 33), n2 := 3133]
cbp.firms[n2 %in% c(44, 45),     n2 := 4445]
cbp.firms[n2 %in% c(48, 49),     n2 := 4849]

gc()

# Merge the rankings dataset to the firms database based on county
cbp.firms <- merge(cbp.firms, lehdtazm, by = c("CountyFIPS", "StateFIPS", "n2"), allow.cartesian = TRUE, all.x = TRUE) 

# Select candidate tazs based on the industry of the firm, firm size, and ranking of
# that particular industry in a TAZ
cbp.firms[, candidate := 0L]
cbp.firms[emp >  5000 & EmpRank %in% c(9, 10), candidate := 1L]
cbp.firms[emp >  2000 & emp <= 5000 & EmpRank %in% 7:10, candidate := 1L]
cbp.firms[emp >  500  & emp <= 2000 & EmpRank %in% 5:10, candidate := 1L]
cbp.firms[emp >  100  & emp <= 500  & EmpRank %in% 4:10, candidate := 1L]
cbp.firms[emp >  20   & emp <= 100  & EmpRank %in% 2:10, candidate := 1L]
cbp.firms[emp <= 20   & EmpRank %in% c(1:10), candidate := 1L]

# For the small number of businesses that did not get a candidate TAZ - allow those
# to have some candidates (small error is better than omitting the businesses)
cbp.firms[firmid %in% cbp.firms[, sum(candidate), by = firmid][V1 == 0]$firmid, candidate := 1L]

# Remove non-candidate TAZs
cbp.firms <- cbp.firms[candidate == 1]

# Generate a random number based on which one of the candidate TAZs would be selected
cbp.firms[, u := runif(.N)]

# Assign the TAZ for which the random number generated is the highest among all candidate tazs
cbp.firms <- cbp.firms[cbp.firms[, .I[which.max(u)], by = firmid]$V1]

# Compare the employment by TAZ to that from the LEHD data
emp_taz <- cbp.firms[, .(emp = sum(emp)), by = .(CountyFIPS, StateFIPS, TAZ, n2)]
emp_taz <- merge(emp_taz, lehdtazm[, .(CountyFIPS, StateFIPS, TAZ, n2, LEmp)],
                 by = c("CountyFIPS", "StateFIPS", "TAZ", "n2"), all = TRUE)
emp_taz[is.na(emp), emp := 0] ##TODO: The employment numbers are probably not comparable.


# Save Final File for use in firm-est connection code
cbp.firms[, 10:15 := NULL]
cbp.firms$fsizecat <- interaction( "f", cbp.firms$fsizecat, sep = "")

#--------------------------------------------------------------------------------------
#create agriculture and foreign firms data
#-------------------------
agfirms <- merge(data.table(NAICSio=NAICS2007io_to_SCTG$NAICSio[NAICS2007io_to_SCTG$SCTG %in% c(1,2)], k=1),
                 data.table(unique(cbp.firms[,.(TAZ,CountyFIPS,StateFIPS,FAF4)]),k=1),
                 by = "k", allow.cartesian = TRUE)

agfirms <- merge(agfirms, NAICS2007_to_NAICS2007io[,.(NAICSio, naics2007=as.character(NAICS))], by = "NAICSio", allow.cartesian = TRUE)
# There is one to one mapping for agricultural firms between NAICS2007 and NAICS2012
# Converting from NAICS code from 2007 to 2012 for sanity check
agfirms[NAICS2007_to_NAICS2012[,.(naics2007=as.character(NAICS2007),NAICS2012)],naics:=as.character(NAICS2012),on="naics2007"]
agfirms[,naics2007:=NULL]
agfirms[,c("NAICSio","k") := NULL]
agfirms[,c("f1"):=1]
agfirms[,c("emp",paste0("f",2:23)):=0]
agfirms <- melt(agfirms, measure.vars = paste0("f", 1:23), variable.name = "fsizecat", value.name = "firms")
agfirms <- agfirms[firms==1]
agfirms[,c("firms") := NULL]
agfirms[,emp:=c(2, 7, 12, 17, 22, 27, 32, 37, 44, 62, 87, 124, 174, 249, 349, 449, 624, 874, 1249, 1749, 2249, 3749, 5000)[fsizecat]]
agfirms[, n2 := as.integer(substr(naics, 1, 2))]
agfirms[n2 %in% c(31, 32, 33), n2 := 3133]
agfirms[n2 %in% c(44, 45),     n2 := 4445]
agfirms[n2 %in% c(48, 49),     n2 := 4849]
agfirms[, firmid := .I + nrow(cbp.firms)]

foreignfirms <- data.table(expand.grid(naics = as.character(unique(NAICS2012$NAICS6)), FAF4 = 801:808))
foreignfirms <- foreignfirms[!naics %in% c("491110", "814110")] #remove the housing NAICS code and postal services
foreignfirms[, n2 := as.integer(substr(naics, 1, 2))]
foreignfirms[n2 %in% c(31, 32, 33), n2 := 3133]
foreignfirms[n2 %in% c(44, 45),     n2 := 4445]
foreignfirms[n2 %in% c(48, 49),     n2 := 4849]
foreignfirms[, c("CountyFIPS", "StateFIPS","TAZ") := 0L]
foreignfirms[, c("emp", paste0("f", 1:22)) := 0L]
foreignfirms[, paste0("f", 23) := 1L]
foreignfirms <- melt(foreignfirms, measure.vars = paste0("f", 1:23), variable.name = "fsizecat", value.name = "firms")
foreignfirms <- foreignfirms[firms==1]
foreignfirms[,c("firms") := NULL]
foreignfirms[,emp:=c(2, 7, 12, 17, 22, 27, 32, 37, 44, 62, 87, 124, 174, 249, 349, 449, 624, 874, 1249, 1749, 2249, 3749, 5000)[fsizecat]]
foreignfirms[, firmid := .I + nrow(cbp.firms) + nrow(agfirms)]
#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------
#Combine all the firms
#-------------------------

cbp.firms <- rbind(cbp.firms, agfirms, foreignfirms)
cbp.firms[, n2 := as.integer(substr(naics, 1, 2))]
cbp.firms[, n4 := as.integer(substr(naics, 1, 4))]
dir.create("outputs", showWarnings=FALSE)
fwrite(cbp.firms, "outputs/cbp.firms_final.csv")

