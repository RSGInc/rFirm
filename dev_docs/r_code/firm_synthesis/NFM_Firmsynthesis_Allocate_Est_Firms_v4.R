#Example script to allocate firms to establishments  

# Pseudo Steps				
# 1	For each estid with e12 in a zone, select a firmid with f23 in the same zone (zone is combination of state-county-TAZ)			
# 1-a		If more firms than establishments, randomly select one firmid for each estid (remaining firmids will be assigned to e11 or e10 or e9 or .)		
# 1-b		If more establishments than firms, randomly select one firmid to be assigned to more than one estid		
# 1-c		If no firmid, remove the estid or flag it		
# 2	For each estid with e11 in a zone select a firmid with f23 or f22 in the same zone			
# 3	For each estid with e10 in a zone select a firmid with f23 or f22 or f21 or f20 in the same zone			
# 4	Follow the same steps for next e and f size categories			

# In order to make sure all firms are assigned:
# 1 Find at least one establishment for each firm in the same county and naics
# 2 For the establishments with no firm found, find the closest firm county to the establishment county (by the same naics)
# 3 Update the county of the firm to the new found county FIPS

# Packages
library(data.table)

# Load in the example files and correspodences
est <- fread("./dev/Data/cbp.establishments_final.csv")
firm <- fread("./dev/Data/cbp.firms_final.csv")
esize_fsize <- fread("./dev/Data/corresp_estsize_firmsize.csv")
setkey(esize_fsize, EstablishmentSizeNum, FirmSizeNum)
#colSums(is.na(est)) #colSums(is.na(firm)) #check for any NAs in the two tables
firm[, selected := 0L]
## Allocate each establishment to a firm

# establishment size groups in reverse order
estgroups <- sort(unique(esize_fsize$EstablishmentSizeNum), decreasing = TRUE)

# Loop over 2-digit naics to avoid memory issue
naicslist <- list()
NAICS2 <- unique(est$n2)

#list for logging
loglist <- list()

for (naics2 in NAICS2){
  
  sizelist <- list()
  
  for (i in 1:12) {
    
    print(paste(naics2, i))
    
    #start with the largest group
    estgroup <- estgroups[i] 
    
    #what are the corresponding esizcats and fsizecats
    esizecats <- esize_fsize[EstablishmentSizeNum == estgroup]$EstablishmentSize
    fsizecats <- esize_fsize[EstablishmentSizeNum >= estgroup]$FirmSize
    
    #get the establishments and firms
    esttemp  <- est   [naics2 == n2 & esizecat %in% esizecats]
    firmtemp <- firm  [naics2 == n2 & fsizecat %in% fsizecats]
    
    #select a firm for each establishment (where they are in the same county) 
    estfirmtemp <- merge(esttemp, 
                         firmtemp[,.(naics, CountyFIPS, FAF4, firmid,selected)], 
                         by = c("naics", "CountyFIPS", "FAF4"), #merge on FAF4 too to ensure the countyFIPS refers to same county, also same FAF zone for interational
                         all.x = TRUE, 
                         allow.cartesian = TRUE)
    
    estfirmtemp[,temprand := runif(.N)]
    estfirmtemp[selected==1, temprand := temprand * 0.01]
    estfirmtemp[selected==0, temprand := temprand + 0.9*(1-temprand)]
    
    sizelist[[i]] <- estfirmtemp[estfirmtemp[,.I[which.max(temprand)], by = estid]$V1]
    firm[firmid %in% unique(sizelist[[i]]$firmid), selected := 1L]
    
    loglist[[paste(naics2, i,sep="_")]] <- firm[,.N, by = selected]
    print(loglist[[paste(naics2, i,sep="_")]])
    
  }
  
  naicslist[[naics2]] <- rbindlist(sizelist)
}

outdf1 <- rbindlist(naicslist)
rm(naicslist, estfirmtemp, esttemp, firmtemp)
outdf1[,c("selected", "temprand") := NULL]
gc()

saveRDS(outdf1, "./dev/Data/outdf1_withCounty_July7.rds")
length(unique(outdf1$firmid)) / length(unique(firm$firmid))  #with CountyFIPS: 4311149/6059738 #[1] 0.7114415
Establishments <- outdf1[!(is.na(firmid))]

#######################################################
# Some estbalishments did not get a firm (in the same TAZ and naics) allocated to them. Allocate a firm based on the naics code only.

Tempest <- outdf1[ (is.na(firmid))]
Tempest[, firmid := NULL]
rm(outdf1)
gc()

#loop on 2 digit naics
naicslist <- list()
NAICS2 <- unique(Tempest$n2)

###takes about 10 minutes to run (on WRJ modeling computer)
loglist <- list()
for (naics2 in NAICS2){
  sizelist <- list()
  
  for (i in 1:12) {
    
    print(paste(naics2, i))
    
    #start with the largest group
    estgroup <- estgroups[i] 
    
    #what are the corresponding esizcats and fsizecats
    esizecats <- esize_fsize[EstablishmentSizeNum == estgroup]$EstablishmentSize
    fsizecats <- esize_fsize[EstablishmentSizeNum >= estgroup]$FirmSize
    
    #get the establishments and firms
    esttemp  <- Tempest[naics2 == n2 & esizecat %in% esizecats]
    firmtemp <- firm   [naics2 == n2 & fsizecat %in% fsizecats]
    
    #select a firm for each establishment (where they are in the same zone) ##TODO: by TAZ and naics? (same industry for firm and est?)
    estfirmtemp <- merge(esttemp,
                         firmtemp[,.(naics, StateFIPS, FAF4, firmid, selected)],
                         by = c("naics", "StateFIPS", "FAF4"), all.x = TRUE, allow.cartesian = TRUE)
    estfirmtemp[, temprand := runif(.N)] #keep NAs and select from firms elsewehere in the same naics
    estfirmtemp[selected==1, temprand := temprand * 0.01]
    estfirmtemp[selected==0, temprand := temprand + 0.9*(1-temprand)]
    
    sizelist[[i]] <- estfirmtemp[estfirmtemp[,.I[which.max(temprand)], by = estid]$V1]
    firm[firmid %in% unique(sizelist[[i]]$firmid), selected := 1]
    
    loglist[[paste(naics2, i,sep="_")]] <- firm[,.N, by = selected]
    print(loglist[[paste(naics2, i,sep="_")]])
  }
  
  naicslist[[naics2]] <- rbindlist(sizelist)
}

Tempest <- rbindlist(naicslist)

rm(sizelist, naicslist, estfirmtemp, esttemp, firmtemp)
Tempest[,c("selected", "temprand") := NULL]
gc()

length(unique(Tempest$firmid)) / length(unique(firm$firmid))

#######################################################

Establishments <- rbind(Establishments, Tempest)
rm(Tempest)

#check firms selected representation of all firms
length(unique(Establishments$firmid))/length(unique(firm$firmid))

# Save the final table
fwrite(Establishments, "./dev/Data/FirmsandEstablishments.csv")
saveRDS(Establishments, "./dev/Data/FirmsandEstablishments.rds")

#Establishments <- readRDS("./dev/Data/FirmsandEstablishments.rds")

##############################################################################################################
##############################################################################################################
##Some statistics

#length(unique(Establishments$firmid))
#[1] 4,289,103

#length(unique(Establishments$naics))
#[1] 1249

#length(unique(Establishments$TAZ))
#[1] 4,485

#sum(Establishments$emp)
#[1] 129,690,403

#check est/frim ratio by naics
Est <- setDT(Establishments)[, .(unique_est  = uniqueN(estid)),  by = naics]
Frm <- setDT(Establishments)[, .(unique_firm = uniqueN(firmid)), by = naics]
setkey(Est, naics)
setkey(Frm, naics)
Ratios <- merge(Est, Frm, by = c("naics"))

Establishments[, n3 := as.integer(substr(naics, 1, 3))]
Est <- setDT(Establishments)[, .(unique_est  = uniqueN(estid)),  by = n3]
Frm <- setDT(Establishments)[, .(unique_firm = uniqueN(firmid)), by = n3]
setkey(Est, n3)
setkey(Frm, n3)
Ratios_n3 <- merge(Est, Frm, by = c("n3"))

fwrite(Ratios,    "./dev/Data/Output Analysis/Ratios.csv")
fwrite(Ratios_n3, "./dev/Data/Output Analysis/Ratios_n3.csv")

#check est/frim ration by TAZ and state
Est <- setDT(Establishments)[, .(unique_est  = uniqueN(estid)),  by = TAZ]
Frm <- setDT(Establishments)[, .(unique_firm = uniqueN(firmid)), by = TAZ]
setkey(Est, TAZ)
setkey(Frm, TAZ)
Ratios_TAZ <- merge(Est, Frm, by = c("TAZ"))

fwrite(Ratios_TAZ,    "./dev/Data/Output Analysis/Ratios_TAZ.csv")

Est <- setDT(Establishments)[, .(unique_est  = uniqueN(estid)),  by = StateFIPS]
Frm <- setDT(Establishments)[, .(unique_firm = uniqueN(firmid)), by = StateFIPS]
setkey(Est, StateFIPS)
setkey(Frm, StateFIPS)
Ratios_State <- merge(Est, Frm, by = c("StateFIPS"))

fwrite(Ratios_State,    "./dev/Data/Output Analysis/Ratios_State.csv")

#########################################################################################################
#########################################################################################################
# Plot establishments by Firms (total)
library(rgdal)
library(regoes)
library(ggplot2)
library(rFreight)
library(maps)

# Plotting helpers
#define color using RSG palette
rsgcolordf <- data.frame(red=c(246,0,99,186,117,255,82),
                         green=c(139,111,175,18,190,194,77),
                         blue=c(31,161,94,34,233,14,133),
                         colornames=c("orange","marine","leaf","cherry","sky","sunshine","violet"))

# NFM TAZ shapefile
nfmtaz <- readOGR("./dev/Data/GIS/NUMA Zones","NUMA_Polygon")
plot(nfmtaz)
head(nfmtaz@data)
proj4string(nfmtaz) #[1] "+proj=longlat +datum=NAD83 +no_defs +ellps=GRS80 +towgs84=0,0,0"

est <- fread("./dev/Data/cbp.establishments_final.csv")
firm <- fread("./dev/Data/cbp.firms_final.csv")

# some charts of data
Establishments <- readRDS("./Data/FirmsandEstablishments.rds")
Establishments[, esizecat := factor(esizecat, levels = c("e1", "e2", "e3", "e4", "e5", "e6", "e7", "e8", "e9", "e10", "e11", "e12"),
                                    labels = c("1-4","5-9","10-\n19","20-\n49","50-\n99","100-\n249","250-\n499","500-\n999", "1000-\n1499", "1500-\n2499", "2500-\n4999", "5000+"),
                                    ordered = TRUE)]

ESize <- Establishments[, .N, by = esizecat]
EN2 <- Establishments[, .N, by = n2]
ESizeByN2 <- Establishments[, .N, by = .(n2, esizecat)]

EN2[unique(NAICS2007[,.(n2 = NAICS2, Label2)]), Industry := i.Label2, on = "n2"]
ESizeByN2[unique(NAICS2007[,.(n2 = NAICS2, Label2)]), Industry := i.Label2, on = "n2"]

EN2[n2 == 3133, Industry := "Manufacturing"]
EN2[n2 == 4445, Industry := "Retail Trade"]
EN2[n2 == 4849, Industry := "Transportation and Warehousing"]
EN2 <- EN2[, .(N = sum(N)), by = Industry]

ESizeByN2[n2 == 3133, Industry := "Manufacturing"]
ESizeByN2[n2 == 4445, Industry := "Retail Trade"]
ESizeByN2[n2 == 4849, Industry := "Transportation and Warehousing"]
ESizeByN2 <- ESizeByN2[, .(N = sum(N)), by = .(Industry, esizecat)]

g_size <- ggplot(ESize,aes(x=esizecat, y=N)) +
  geom_bar(fill=rgb(rsgcolordf[2,],maxColorValue=255), stat="identity") +
  ylab("Number of Establishments") + 
  xlab("Number of Employees\nper Establishment") +
  labs(title="Establishments by Size") +
  theme(axis.ticks = element_blank())

pngname <- paste0("./dev/Data/Output Analysis/EstablishmentsSize.png")
png(pngname, width = 10, height = 7.5, res = 300, units = 'in')
g_size
dev.off()

g_n2 <- ggplot(EN2,aes(x=factor(Industry), y=N)) +
  geom_bar(fill=rgb(rsgcolordf[4,],maxColorValue=255), stat="identity") +
  ylab("Number of Establishments") + 
  xlab("Industry") +
  labs(title="Establishments by Industry") +
  theme(axis.ticks = element_blank()) +
  coord_flip()

pngname <- paste0("./dev/Data/Output Analysis/EstablishmentsIndustry.png")
png(pngname, width = 10, height = 7.5, res = 300, units = 'in')
g_n2
dev.off()

emp_labs <- c("1-4","5-9","10-19","20-49","50-99","100-249","250-499","500-999", "1000-1499", "1500-2499", "2500-4999", "5000+")
g_size_n2 <- ggplot(ESizeByN2,aes(x= esizecat, y=N)) +
  geom_bar(fill=rgb(rsgcolordf[3,],maxColorValue=255), stat="identity") +
  ylab("Number of Establishments") + 
  scale_x_discrete(name = "Number of Employees\nper Establishment", labels = emp_labs) +
  labs(title="Establishments by Size") +
  theme(axis.ticks = element_blank(), axis.text.x=element_text(angle=90,hjust = 1)) +
  facet_wrap(~Industry, ncol = 3)

pngname <- paste0("./dev/Data/Output Analysis/EstablishmentsSizeIndustry.png")
png(pngname, width = 10, height = 7.5, res = 300, units = 'in')
g_size_n2
dev.off()

#Establishments per firm
FirmEst <- Establishments[, .N, by = .(firmid, n2)]
FirmEst[unique(NAICS2007[,.(n2 = NAICS2, Label2)]), Industry := i.Label2, on = "n2"]
FirmEst[n2 == 3133, Industry := "Manufacturing"]
FirmEst[n2 == 4445, Industry := "Retail Trade"]
FirmEst[n2 == 4849, Industry := "Transportation and Warehousing"]
FirmEst <- FirmEst[, .(N = sum(N)), by = .(Industry, firmid)]

FirmEstNum <- FirmEst[, .(NumFirms = .N), by = N]
FirmEstN2 <- FirmEst[, .(NumFirms = .N), by = .(N, Industry)]

g_firms <- ggplot(FirmEstNum[N <= 20],aes(x=N, y=NumFirms)) +
  geom_bar(fill=rgb(rsgcolordf[2,],maxColorValue=255), stat="identity") +
  ylab("Number of Firms") + 
  xlab("Number of Establishments per Firm (<= 20)") +
  labs(title="Firms by Number of Estasblishments") +
  theme(axis.ticks = element_blank())

pngname <- paste0("./dev/Data/Output Analysis/FirmsEst.png")
png(pngname, width = 10, height = 7.5, res = 300, units = 'in')
g_firms
dev.off()

g_firms_n2 <- ggplot(FirmEstN2[N <= 20],aes(x=N, y=NumFirms)) +
  geom_bar(fill=rgb(rsgcolordf[3,],maxColorValue=255), stat="identity") +
  ylab("Number of Firms") + 
  xlab("Number of Establishments per Firm (<= 20)") +
  labs(title="Firms by Number of Estasblishments") +
  theme(axis.ticks = element_blank()) +
  facet_wrap(~Industry, ncol = 3)

pngname <- paste0("./dev/Data/Output Analysis/FirmsEstIndustry.png")
png(pngname, width = 10, height = 7.5, res = 300, units = 'in')
g_firms_n2
dev.off()

#distance dist
Establishments[firm, FirmTAZ := i.TAZ, on = "firmid"]
highskim <- fread("./dev/Data/Skim/Highway_Skim.csv")
head(highskim)
Establishments[highskim[, .(TAZ = V1, FirmTAZ = V2, Distance = V3)], Distance := i.Distance, on = c("TAZ", "FirmTAZ")]

g_dist_hist <- ggplot(Establishments,aes(x=Distance)) + 
  geom_histogram(fill=rgb(rsgcolordf[3,],maxColorValue=255)) +
  ylab("Number of Establishments") + 
  xlab("Highway Distance (Miles)") +
  labs(title="Firm to Establishment Distance") +
  theme(axis.ticks = element_blank())

g_dist_hist_diff_zone <- ggplot(Establishments[TAZ != FirmTAZ],aes(x=Distance)) + 
  geom_histogram(fill=rgb(rsgcolordf[2,],maxColorValue=255)) +
  ylab("Number of Establishments") + 
  xlab("Highway Distance (Miles)") +
  labs(title="Firm to Establishment Distance (Not in Same Zone)") +
  theme(axis.ticks = element_blank())

Establishments[unique(NAICS2007[,.(n2 = NAICS2, Label2)]), Industry := i.Label2, on = "n2"]
Establishments[n2 == 3133, Industry := "Manufacturing"]
Establishments[n2 == 4445, Industry := "Retail Trade"]
Establishments[n2 == 4849, Industry := "Transportation and Warehousing"]

g_dist_hist_diff_zone_ind <- ggplot(Establishments[TAZ != FirmTAZ],aes(x=Distance)) + 
  geom_histogram(fill=rgb(rsgcolordf[4,],maxColorValue=255)) +
  ylab("Number of Establishments") + 
  xlab("Highway Distance (Miles)") +
  labs(title="Firm to Establishment Distance (Not in Same Zone)") +
  theme(axis.ticks = element_blank()) +
  facet_wrap(~Industry, ncol = 3)

pngname <- paste0("./dev/Data/Output Analysis/FirmsEstDistance.png")
png(pngname, width = 10, height = 7.5, res = 300, units = 'in')
g_dist_hist
dev.off()

pngname <- paste0("./dev/Data/Output Analysis/FirmsEstDistanceDiffZone.png")
png(pngname, width = 10, height = 7.5, res = 300, units = 'in')
g_dist_hist_diff_zone
dev.off()

pngname <- paste0("./dev/Data/Output Analysis/FirmsEstDistanceDiffZoneIndustry.png")
png(pngname, width = 10, height = 7.5, res = 300, units = 'in')
g_dist_hist_diff_zone_ind
dev.off()

# Maps of some sample firms
#US (lower 48 only)s

nfmtazcoords <- data.table(x=coordinates(nfmtaz)[,1], y = coordinates(nfmtaz)[,2], TAZ = nfmtaz$FID_1)
ExFirm <- Establishments[firmid == 5919311]
ExFirm[nfmtazcoords, x := i.x, on = "TAZ"]
ExFirm[nfmtazcoords, y := i.y, on = "TAZ"]
ExFirm[nfmtazcoords[,.(FirmTAZ = TAZ, x, y)], Fx := i.x, on = "FirmTAZ"]
ExFirm[nfmtazcoords[,.(FirmTAZ = TAZ, x, y)], Fy := i.y, on = "FirmTAZ"]

all_states <- map_data("state")
p <- ggplot()
p <- p + geom_polygon( data=all_states, aes(x=long, y=lat, group = group),colour="white", fill="gray" )

g_est_map_1 <- p + 
  geom_point(data=ExFirm,aes(x,y),color=rgb(rsgcolordf[4,],maxColorValue=255),size=0.6) +
  geom_point(data=ExFirm[1],aes(Fx,Fy),color=rgb(rsgcolordf[2,],maxColorValue=255),size=3) +
  labs(title = "Establishment Locations (Firm = 5919311)") +
  xlab("Longitude") + ylab("Latitude") +
  xlim(-130, -60) + ylim(25, 50)

pngname <- paste0("./dev/Data/Output Analysis/EstablishmentsForFirm5919311.png")
png(pngname, width = 10, height = 7.5, res = 300, units = 'in')
g_est_map_1
dev.off()

ExFirms <- FirmEst[Industry == "Retail Trade"][order(N, decreasing = TRUE)][1:12]
ExFirms <- Establishments[firmid %in% ExFirms$firmid]
ExFirms[nfmtazcoords, x := i.x, on = "TAZ"]
ExFirms[nfmtazcoords, y := i.y, on = "TAZ"]
ExFirms[nfmtazcoords[,.(FirmTAZ = TAZ, x, y)], Fx := i.x, on = "FirmTAZ"]
ExFirms[nfmtazcoords[,.(FirmTAZ = TAZ, x, y)], Fy := i.y, on = "FirmTAZ"]

g_est_map_12 <- p + 
  geom_point(data=ExFirms,aes(x,y),color=rgb(rsgcolordf[4,],maxColorValue=255),size=0.6) +
  geom_point(data=ExFirms,aes(Fx,Fy),color=rgb(rsgcolordf[2,],maxColorValue=255),size=3) +
  labs(title = "Establishment Locations (12 Largest Retail Firms)") +
  xlab("Longitude") + ylab("Latitude") +
  xlim(-130, -60) + ylim(25, 50) +
  facet_wrap(~firmid, ncol = 4)

pngname <- paste0("./dev/Data/Output Analysis/EstablishmentsForRetailFirms.png")
png(pngname, width = 10, height = 7.5, res = 300, units = 'in')
g_est_map_12
dev.off()