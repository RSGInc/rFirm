#-----------------------------------------------------------------------------------
## Prepare inputs for Establishment Synthesis
#-----------------------------------------------------------------------------------
setwd("dev/")

library(data.table)
library(RSGFAF, lib.loc = "../lib/pkgs/library/")
library(rFreight, lib.loc = "../lib/pkgs/library/")


# 2014 County Business Patterns Data
# From BEA website-National County Business Pattern-Number of employees and businesses
# (by employee size group) by state, county and NAICS (2,3,4,5 and 6 digits)
# USe 2014 as latest in 2007 NAICS codes, consistent with the Input Output data
if(!file.exists("Data/CBP/cbp14co.txt")){
  # Download the file if not exist
  download.file("ftp://ftp.census.gov/econ2014/CBP_CSV/cbp14co.zip",
                "./Data/CBP/cbp14co.zip")
  unzip("./Data/CBP/cbp14co.zip",exdir="./Data/CBP")
}
cbp<-fread("./Data/CBP/cbp14co.txt")

data(County_to_FAFZone)   #from the RSGFAF package
FAF_NUMA    <- fread ("./Data/corresp_taz_faf4.csv")
FIPS_NUMA   <- fread ("./Data/corresp_taz_fips.csv")

cbp <- cbp[grep("[0-9]{6}",naics,100),]
cbp <- cbp[fipscty != "999",]
cbp[, StateFIPS := as.integer(fipstate)]
cbp[, CountyFIPS := as.integer(fipscty)]
cbp[County_to_FAFZone, FAF4 := i.FAFZone_4.2, on = c("StateFIPS", "CountyFIPS")] 
unique(cbp[is.na(FAF4),list(StateFIPS,CountyFIPS)]) #check for any missing
#Update missing for Alaska
cbp[is.na(FAF4), FAF4 := 20L]

#join on the TAZ (one per FAFZONE)
cbp[FIPS_NUMA, TAZ := i.TAZ, on = c("StateFIPS", "CountyFIPS")]

#Sum establishment size categories into 8 categories
cbp <- cbp[, .(emp=sum(emp),est=sum(est),e1=sum(n1_4),e2=sum(n5_9),e3=sum(n10_19),e4=sum(n20_49),e5=sum(n50_99),e6=sum(n100_249),
               e7=sum(n250_499),e8=sum(n500_999),e9=sum(n1000_1),e10=sum(n1000_2),e11=sum(n1000_3),e12=sum(n1000_4)), 
               by = .(naics, TAZ, CountyFIPS, StateFIPS, FAF4)][order(TAZ,naics)]

cbp[, emp := 2*e1 + 7*e2 +  15*e3 +  35*e4 +  75*e5 +  175*e6 +  375*e7 +  750*e8 +  1250*e9 +  2000*e10 +  3750*e11 +  5000*e12]
cbp[, naics := as.integer(naics)]

cbp[, est := NULL]
cbp[, emp := NULL]

# fwrite(cbp, "./Data/cbp.establishments.csv")

##############################################################################
# Read in the rankings table (output of the LEHD data processing code)
lehdtazm <- fread("./Data/lehdtazm.csv")

# Enumerate the CBP establishments
#-----------------------------------------------
# Melt to create separate rows for each establishment size category
cbp.establishments <- melt(cbp, measure.vars = paste0("e", 1:12), variable.name = "esizecat", value.name = "establishments")
cbp.establishments[, esizecat := as.integer(esizecat)] #convert esizecat to an integer (1:23) AG (1:12)
rm(cbp)

# Remove records where number of establishments is 0
cbp.establishments <- cbp.establishments[establishments > 0]

# Enumerate the establishments using the est variable
cbp.establishments <- cbp.establishments[rep(seq_len(cbp.establishments[, .N]), establishments)]

# Estimate the number of employees
cbp.establishments[, emp := c(2, 7, 15, 35, 75, 175, 375, 750, 1250, 2000, 3750, 5000)[esizecat]]
cbp.establishments[, establishments := NULL]
cbp.establishments[, TAZ := NULL]

# Assign establishments from counties to TAZs
#-----------------------------------------------------------------
# Add an id and n2
cbp.establishments[, estid := .I]
cbp.establishments[, n2 := as.integer(substr(naics, 1, 2))]

# Assign specific NAICS categories which are used to locate businesses to tazs
cbp.establishments[n2 %in% c(31, 32, 33), n2 := 3133]
cbp.establishments[n2 %in% c(44, 45),     n2 := 4445]
cbp.establishments[n2 %in% c(48, 49),     n2 := 4849]

# Merge the rankings dataset to the establishments database based on county
cbp.establishments <- merge(cbp.establishments, lehdtazm, by = c("CountyFIPS", "StateFIPS", "n2"), allow.cartesian = TRUE, all.x = TRUE) 

# Select candidate tazs based on the industry of the establishment, establishment size, and ranking of
# that particular industry in a TAZ
cbp.establishments[, candidate := 0L]
cbp.establishments[emp >  5000 & EmpRank %in% c(9, 10), candidate := 1L]
cbp.establishments[emp >  2000 & emp <= 5000 & EmpRank %in% 7:10, candidate := 1L]
cbp.establishments[emp >  500  & emp <= 2000 & EmpRank %in% 5:10, candidate := 1L]
cbp.establishments[emp >  100  & emp <= 500  & EmpRank %in% 4:10, candidate := 1L]
cbp.establishments[emp >  20   & emp <= 100  & EmpRank %in% 2:10, candidate := 1L]
cbp.establishments[emp <= 20   & EmpRank %in% c(1:10), candidate := 1L]

# For the small number of businesses that did not get a candidate TAZ - allow those
# to have some candidates (small error is better than omitting the businesses)
cbp.establishments[estid %in% cbp.establishments[, sum(candidate), by = estid][V1 == 0]$estid, candidate := 1L]

# Remove non-candidate TAZs
cbp.establishments <- cbp.establishments[candidate == 1]

# Generate a random number based on which one of the candidate TAZs would be selected
cbp.establishments[, u := runif(.N)]

# Assign the TAZ for which the random number generated is the highest among all candidate tazs
cbp.establishments <- cbp.establishments[cbp.establishments[, .I[which.max(u)], by = estid]$V1]

# Compare the employment by TAZ to that from the LEHD data
emp_taz <- cbp.establishments[, .(emp = sum(emp)), by = .(CountyFIPS, StateFIPS, TAZ, n2)]
emp_taz <- merge(emp_taz, lehdtazm[, .(CountyFIPS, StateFIPS, TAZ, n2, LEmp)],
                 by = c("CountyFIPS", "StateFIPS", "TAZ", "n2"), all = TRUE)
emp_taz[is.na(emp), emp := 0]

#--------------------------------------------------------------------------------------
# # Check sums - signif more in CBP than LEHD
# sum(emp_taz$emp)
# sum(emp_taz$LEmp)
# 
# # Check merges etc didn't alter employment
# sum(lehdtazm$LEmp)
# sum(cbp.establishments$emp)
# 
# # Check plot
# png("./Data/employment.png", width = 7.5, height = 7.5, res = 300, units = "in")
# plot(emp_taz$emp, emp_taz$LEmp,
#      xlab = "CBP Employment", ylab = "LEHD Employment", xlim = c(0, 175000), ylim = c(0, 175000))
# dev.off()
# 
# # Plot employment by TAZ (total)
# emp_taz <- emp_taz[, .(emp = sum(emp), LEmp = sum(LEmp)), by = TAZ]
# png("./Data/employment_taz.png", width = 7.5, height = 7.5, res = 300, units = "in")
# plot(emp_taz$emp, emp_taz$LEmp,
#      xlab = "CBP Employment", ylab = "LEHD Employment", xlim = c(0, 4000000), ylim = c(0, 4000000))
# dev.off()
#--------------------------------------------------------------------------------------

# Save Final File for use in establishment-est connection code
cbp.establishments[, 10:15 := NULL]
cbp.establishments$esizecat <- interaction( "e", cbp.establishments$esizecat, sep = "")

#--------------------------------------------------------------------------------------
#create agriculture and foreign establishments data
#-------------------------
ag.est <- merge(data.table(NAICSio=NAICS2007io_to_SCTG$NAICSio[NAICS2007io_to_SCTG$SCTG %in% c(1,2)], k=1),
                 data.table(unique(cbp.establishments[,.(TAZ,CountyFIPS,StateFIPS,FAF4)]),k=1),
                 by = "k", allow.cartesian = TRUE)

ag.est <- merge(ag.est, NAICS2007_to_NAICS2007io[,.(NAICSio, naics2007=as.character(NAICS))], by = "NAICSio", allow.cartesian = TRUE)
# There is one to one mapping for agricultural firms between NAICS2007 and NAICS2012
# Converting for sanity check
ag.est[NAICS2007_to_NAICS2012[,.(naics2007=as.character(NAICS2007),NAICS2012)],naics:=as.character(NAICS2012),on="naics2007"]
ag.est[,naics2007:=NULL]
ag.est[,c("NAICSio","k") := NULL]
ag.est[,c("e1"):=1]
ag.est[,c("emp",paste0("e",2:12)):=0]
ag.est <- melt(ag.est, measure.vars = paste0("e", 1:12), variable.name = "esizecat", value.name = "est")
ag.est <- ag.est[est==1]
ag.est[,c("est") := NULL]
ag.est[, emp := c(2, 7, 15, 35, 75, 175, 375, 750, 1250, 2000, 3750, 5000)[esizecat]]
ag.est[, n2 := as.integer(substr(naics, 1, 2))]
ag.est[n2 %in% c(31, 32, 33), n2 := 3133]
ag.est[n2 %in% c(44, 45),     n2 := 4445]
ag.est[n2 %in% c(48, 49),     n2 := 4849]
ag.est[, estid := .I + nrow(cbp.establishments)]

foreign.est <- data.table(expand.grid(naics = as.character(unique(NAICS2012$NAICS6)), FAF4 = 801:808))
foreign.est <- foreign.est[!naics %in% c("491110", "814110")] #remove the housing NAICS code and postal services
foreign.est[, n2 := as.integer(substr(naics, 1, 2))]
foreign.est[n2 %in% c(31, 32, 33), n2 := 3133]
foreign.est[n2 %in% c(44, 45),     n2 := 4445]
foreign.est[n2 %in% c(48, 49),     n2 := 4849]
foreign.est[, c("CountyFIPS", "StateFIPS","TAZ") := 0L]
foreign.est[, c("emp", paste0("e", 1:12)) := 0L]
foreign.est[, paste0("e", 12) := 1L]
foreign.est <- melt(foreign.est, measure.vars = paste0("e", 1:12), variable.name = "esizecat", value.name = "est")
foreign.est <- foreign.est[est==1]
foreign.est[,c("est") := NULL]
foreign.est[, emp := c(2, 7, 15, 35, 75, 175, 375, 750, 1250, 2000, 3750, 5000)[esizecat]]
foreign.est[, estid := .I + nrow(cbp.establishments) + nrow(ag.est)]
#--------------------------------------------------------------------------------------

#-------------------------------------
#Combine all the establishments
#-------------------------------------

cbp.establishments <- rbind(cbp.establishments, ag.est, foreign.est)
cbp.establishments[, n2 := as.integer(substr(naics, 1, 2))]

fwrite(cbp.establishments, "./Data/cbp.establishments_final.csv")
setwd("..")
