# In order to make sure all firms are assigned:
# 1 Find at least one establishment for each firm in the same county and naics
# 2 For the establishments with no firm found, find the closest firm county to the establishment county (by the same naics)
# 3 Update the county of the firm to the new found county FIPS

library(data.table)
library(rgdal)
library(igraph)
library(Matrix)

# Load in the example files and correspodences
setwd("./dev")
# load("./new_approach1.RData")
est <- fread("./Data/cbp.establishments_final.csv")
firm <- fread("./Data/cbp.firms_final.csv")
esize_fsize <- fread("./Data/corresp_estsize_firmsize.csv")
setkey(esize_fsize, EstablishmentSizeNum, FirmSizeNum)

#----------------------------------------------------------------------------------------------
## Allocate a single establishment to each firm as its "headquarters" (or only estasblishment)

#split out just domestic firms and establishments
estforeign <- est[FAF4 %in% 801:808]
est <- est[!FAF4 %in% 801:808]
firmforeign <- firm[FAF4 %in% 801:808]
firm <- firm[!FAF4 %in% 801:808]

# establishment size groups in reverse order
estgroups <- sort(unique(esize_fsize$EstablishmentSizeNum), decreasing = TRUE)

# Loop over 2-digit naics to avoid memory issue
naicslist <- list()
NAICS2 <- unique(est$n2)

#has the establishment been selected
# est[, selected := -1L]
#has the firm been selected
firm[, selected := 0L]
est[, assigned:=0L]

# naics2 <- NAICS2[1]
# i <- 1
# customSample <- function(nFirm,nEst,Firms,prob=NULL){
#   if(nFirm == nEst){
#     if(nFirm>1) return(sample(Firms))
#     return(Firms)
#   } else if(nFirm > nEst){
#     if(!is.null(prob)) {
#       return(sample(Firms,nEst,replace = FALSE))
#     } else {
#       return(sample(Firms,nEst,replace = FALSE,prob = prob))
#     }
#   } else if(nFirm < nEst){
#     return(sample(c(Firms,rep(NA,nEst-nFirm)),replace = FALSE))
#   }
# }

# Change CountFIPS to TAZ?
set.seed(151)

for (naics2 in NAICS2){
  
  sizelist <- list()
  
  for (i in 1:12) {
    
    print(paste(naics2, i))
    
    #start with the largest group
    estgroup <- estgroups[i] 
    
    #what are the corresponding esizcats and fsizecats
    esizecats <- esize_fsize[EstablishmentSizeNum == estgroup]$EstablishmentSize
    fsizecats <- esize_fsize[EstablishmentSizeNum <= estgroup]$FirmSize
    
    #get the establishments and firms
    esttemp  <- est   [naics2 == n2 & esizecat %in% esizecats & assigned == 0]
    firmtemp <- firm  [naics2 == n2 & fsizecat %in% fsizecats & selected == 0]
    
    #sample an appropriate establishment for each firm
    firmneeded <- firmtemp[,.(Firm = .N), keyby = .(naics, CountyFIPS, FAF4)]
    estsample <- esttemp[,.(Est = .N, EstMin = .I[which.min(.I)], EstMax = .I[which.max(.I)]), keyby = .(naics, CountyFIPS, FAF4)]
    estsample <- merge(estsample, firmneeded, by = c("naics", "CountyFIPS", "FAF4"), all = TRUE)
    firmsample <- firmtemp[,.(Firm = .N, FirmMin = .I[which.min(.I)], FirmMax = .I[which.max(.I)]), keyby = .(naics, CountyFIPS, FAF4)]
    
    #select an establishment for each firm (same county)
    estfirmtemp <- merge(esttemp,
                         firmtemp[,.(naics, CountyFIPS, FAF4, firmid,selected)],
                         by = c("naics", "CountyFIPS", "FAF4"), #merge on FAF4 too to ensure the countyFIPS refers to same county, also same FAF zone for interational
                         all.x = TRUE,
                         allow.cartesian = TRUE)
    estfirmtemp <- estfirmtemp[!is.na(firmid)]
    
    uniquePairs <- estfirmtemp[,{uniqueEst <- length(unique(estid)); estid <- unique(estid); uniqueFirm <- length(unique(firmid)); firmid <- unique(firmid); firmid <- customSample(uniqueFirm,uniqueEst,firmid);data.table(estid,firmid)},by=c("naics","CountyFIPS","FAF4")] #;print(paste0("Firm: ",uniqueFirm," Est: ", uniqueEst," Firmselected: ",paste0(firmid,collapse=", ")))
    uniquePairs <- uniquePairs[!is.na(firmid)]
    
    # estfirmtemp[,temprand := runif(.N)]
    # estfirmtemp[selected==1, temprand := temprand * 0.01]
    # estfirmtemp[selected==0, temprand := temprand + 0.9*(1-temprand)]
    # estfirmselected <- estfirmtemp[estfirmtemp[,.I[which.max(temprand)], by = estid]$V1]
    
    sizelist[[i]] <- estfirmtemp[uniquePairs,on=.(naics,CountyFIPS,FAF4,estid,firmid)]
    # firm[selected == -1 & firmid %in% unique(estfirmtemp$firmid), selected := 0L]
    firm[firmid %in% unique(sizelist[[i]]$firmid), selected := 1L]
    est[estid %in% unique(sizelist[[i]]$estid), assigned := 1L]
    
  }
  
  naicslist[[naics2]] <- rbindlist(sizelist)
}

outdf1 <- rbindlist(naicslist)
rm(naicslist, estfirmtemp, esttemp, firmtemp)
outdf1[,c("selected", "temprand") := NULL]
gc()

# firmnotselected <- firm[selected==0L]
# estnotassigned <- est[assigned==0L]

FactorToNumeric <- function(x) return(as.numeric(as.character(x)))

nfmtaz <- readOGR("../dev/Data/GIS/NUMA Zones","NUMA_Polygon")
NUMAZones <- data.table(nfmtaz@data)
NUMAZones <- NUMAZones[, .(FID_1, STATEFP1_1, GEOID10_1, LONG = FactorToNumeric(INTPTLON_1), LAT = FactorToNumeric(INTPTLAT_1))]
TAZDistances <- rFreight::pairsGCD(NUMAZones[,.(FID_1,LONG,LAT)])
setnames(TAZDistances,c("oFID_1","dFID_1"),c("oTAZ","dTAZ"))
setkey(TAZDistances,oTAZ,dTAZ)


### Logic
## 1. Get the unique establishments and firm by the naics group and the size
## 2. Create a list of unique firms and unique establishments.
## 3. Loop through the firm and for each firm select the closest establishment.
## 4. Remove the establishment from the list of unique establishments.



# naics2 <- NAICS2[1]
# i <- 1


library(data.table)
library(rgdal)
library(igraph)
library(Matrix)
load("./firmByNaics/firstdata.RData")

ConnectFirmsToEstablishments2 <- function(nFirms,nEst,Pairs,maxmatch=TRUE){
  if(nFirms > 0 & nEst > 0 & Pairs[,.N] > 0){
    if(maxmatch){
      # print("Making Matrix")
      # print(paste0("nFirms: ", nFirms, " nEst: ", nEst, " Combinations: ", Pairs[,.N]))
      Pairs[,DjikDistance:=1/Distance]
      if(Pairs[Distance>0,.N]>0) {
        maxDistance <- Pairs[Distance>0,max(DjikDistance)]
        # print(paste0("Max Distance: ", maxDistance))
        Pairs[is.infinite(DjikDistance),DjikDistance:=maxDistance*1.1]
      } else {
        Pairs[is.infinite(DjikDistance),DjikDistance:=1]
      }
      if(nEst > nFirms){
        firmpairs <- sparseMatrix(i=as.numeric(factor(Pairs$firmid)),j=as.numeric(factor(Pairs$estid)),x=Pairs$DjikDistance,dimnames = list(levels(factor(Pairs$firmid)),levels(factor(Pairs$estid))))
        rm(Pairs)
        gc()
        # print("Making graph")
        firmgraph <- graph_from_incidence_matrix(firmpairs,weighted = TRUE)
        rm(firmpairs)
        gc()
        # print("Finding Maximal Bipartite Match")
        firmmaxmatch <- maximum.bipartite.matching(firmgraph)
        gc()
        # print("Sending Results")
        firmestpair <- data.table()
        if(firmmaxmatch$matching_size>0){
          firmestpair <- data.table(firmid=as.numeric(names(firmmaxmatch$matching)),estid=as.numeric(firmmaxmatch$matching))[!is.na(estid)][seq_len(firmmaxmatch$matching_size)]
          
          print(paste0("Firm: ",nFirms," Est: ", nEst," Firmselected: ",length(firmestpair$firmid)))
        } else {
          return(data.table(estid=integer(),firmid=integer()))
        }
      } else {
        firmpairs <- sparseMatrix(i=as.numeric(factor(Pairs$estid)),j=as.numeric(factor(Pairs$firmid)),x=Pairs$DjikDistance,dimnames = list(levels(factor(Pairs$estid)),levels(factor(Pairs$firmid))))
        rm(Pairs)
        gc()
        # print("Making graph")
        firmgraph <- graph_from_incidence_matrix(firmpairs,weighted = TRUE)
        rm(firmpairs)
        gc()
        # print("Finding Maximal Bipartite Match")
        firmmaxmatch <- maximum.bipartite.matching(firmgraph)
        gc()
        # print("Sending Results")
        if(firmmaxmatch$matching_size>0){
          firmestpair <- data.table(firmid=as.numeric(firmmaxmatch$matching),estid=as.numeric(names(firmmaxmatch$matching)))[!is.na(firmid)][seq_len(firmmaxmatch$matching_size)]
          
          print(paste0("Firm: ",nFirms," Est: ", nEst," Firmselected: ",length(firmestpair$firmid)))
        } else {
          return(data.table(estid=integer(),firmid=integer()))
        }
      } 
      # print(paste0("Firm: ",nFirms," Est: ", nEst," Firmselected: ",paste0(firmestpair$firmid,collapse=", ")))
      
      # print(paste0("Maximal Matching: ",Pairs[firmestpair,sum(Distance),on=c("estid","firmid")]/min(nFirms,nEst)))
      
      return(firmestpair)
    } else {
      if(nFirms==1){
        # print(paste0("nFirms: ", nFirms, " nEst: ", nEst, " Combinations: ", Pairs[,.N]))
        print(paste0("Firm: ",nFirms," Est: ", nEst," Firmselected: ",nEst))
        return(Pairs[,.(estid,firmid)])
      } else {
        # print(paste0("nFirms: ", nFirms, " nEst: ", nEst, " Combinations: ", Pairs[,.N]))
        Pairs[,DjikDistance:=1/Distance]
        if(Pairs[Distance>0,.N]>0) {
          maxDistance <- Pairs[Distance>0,max(DjikDistance)]
          # print(paste0("Max Distance: ", maxDistance))
          Pairs[is.infinite(DjikDistance),DjikDistance:=maxDistance*1.1]
        } else {
          Pairs[is.infinite(DjikDistance),DjikDistance:=1]
        }
        firmestpair <- Pairs[,.(firmid=customSample(nFirm = length(firmid), nEst = 1, Firms = firmid, prob = DjikDistance)),by=.(estid)]
        print(paste0("Firm: ",nFirms," Est: ", nEst," Firmselected: ",nEst))
        return(firmestpair)
      }
    }
  } else {
    return(data.table(estid=integer(),firmid=integer()))
  }
}

customSample <- function(nFirm,nEst,Firms,prob=NULL,replace=FALSE){
  if(nFirm == nEst){
    if(nFirm>1) return(sample(Firms))
    return(Firms)
  } else if(nFirm > nEst){
    if(is.null(prob)){
      return(sample(Firms,nEst,replace = replace))
    } else {
      return(sample(Firms,nEst,prob = prob, replace = replace))
    }
  } else if(nFirm < nEst){
    if(is.null(prob)){
      return(sample(c(Firms,rep(NA,nEst-nFirm)),replace = replace))
    } else {
      if(nFirm==1){
        return(rep(Firms,nEst))
      }
      return(sample(Firms,nEst,prob = prob, replace = replace))
    }
  }
}


# Loop over 2-digit naics to avoid memory issue
naicslist <- list()
NAICS2 <- unique(est$n2)
distancethreshold <- c(100,250,500,1000,Inf) # For sub optimal solution to maximum matching problem
est[,n4:=substr(naics,1,4)]
firm[,n4:=substr(naics,1,4)]

custommergesplit <- function(splits,est,firm,bycode,distthresh){
  split_est <- splits[1]
  split_firm <- splits[2]
  if(est[,.N] > 0 & firm[,.N] > 0){
    esttemp <- merge(est[splitn==split_est][,splitn:=NULL],firm[splitn==split_firm][,splitn:=NULL],by=c(bycode),all.x=TRUE,allow.cartesian=TRUE,suffixes = c(".est",".firm"))
    if("TAZ" %in% bycode){
      return(esttemp)
    }
    esttemp[,Distance:=TAZDistances[.(TAZ.est,TAZ.firm),GCD]]
    esttemp <- esttemp[!is.na(Distance)]
    return(esttemp[Distance<distthresh])
  } else {
    return(NULL)
  }
}

custommerge <- function(est,firm,bycode,distthresh){
  if(est[,.N] > 0 & firm[,.N] > 0){
    esttemp <- merge(est,firm,by=bycode,all.x=TRUE,allow.cartesian=TRUE,suffixes = c(".est",".firm"))
    if("TAZ" %in% bycode){
      return(esttemp)
    }
    esttemp[,Distance:=TAZDistances[.(TAZ.est,TAZ.firm),GCD]]
    esttemp <- esttemp[!is.na(Distance)]
    return(esttemp[Distance<distthresh])
  } else {
    return(data.table(firmid=NA,estid=NA))
  }
}
ram_to_use <- 40*(1024**3)



for (naics2 in NAICS2){
  sizelist <- list()
  
  for (i in 1:12) {
    print(paste0("NAICS2: ",naics2, " Size: ",i))
    
    estgroup <- estgroups[i] 
    
    #what are the corresponding esizcats and fsizecats
    esizecats <- esize_fsize[EstablishmentSizeNum == estgroup]$EstablishmentSize
    fsizecats <- esize_fsize[EstablishmentSizeNum >= estgroup]$FirmSize
    
    
    sametaznaicslist <- list()
    difftaznaicslist <- list()
      
      
      
      
      for(j in c("n6", "n4", "n2")){
        print(j)
        threshlist <- list()
        
        
        #get the establishments and firms
        esttemp  <- est   [n2 == naics2 & esizecat %in% esizecats & assigned == 0]
        firmtemp <- firm  [n2 == naics2 & fsizecat %in% fsizecats & selected == 0]
        if(esttemp[,.N] > 0 & firmtemp[,.N] > 0){
          print("Running Same TAZ Pairing....")
          nFirms <- firmtemp[,.N]
          nEst <- esttemp[,.N]
          print(paste0("Total Firms: ",nFirms," Total Est: ",nEst ))
          # First is to pair firm and establishments in the same TAZ
          if(j=="n6"){
            estfirmtemp <- custommerge(esttemp,
                                       firmtemp[,.(naics,TAZ, CountyFIPS, FAF4, firmid,fsizecat,selected,n4,n2)],bycode = c("naics","TAZ","CountyFIPS","FAF4"),distthresh = Inf)
            estfirmtemp <- estfirmtemp[!is.na(firmid)]
            
            uniquePairs <- estfirmtemp[,{uniqueEst <- length(unique(estid)); estid <- unique(estid); uniqueFirm <- length(unique(firmid)); firmid <- unique(firmid); firmid <- customSample(uniqueFirm,uniqueEst,firmid);data.table(estid,firmid)},by=c("naics","CountyFIPS","FAF4","TAZ")] #;print(paste0("Firm: ",uniqueFirm," Est: ", uniqueEst," Firmselected: ", length(firmid[!is.na(firmid)])))
            uniquePairs <- uniquePairs[!is.na(firmid)]
            estfirmtemp <- cbind(estfirmtemp[,.(naics.firm=naics,naics.est=naics,CountyFIPS.firm=CountyFIPS,CountyFIPS.est=CountyFIPS,FAF4.firm=FAF4,FAF4.est=FAF4,TAZ.firm=TAZ,TAZ.est=TAZ)],estfirmtemp[,setdiff(colnames(estfirmtemp),c("naics","TAZ","CountyFIPS","FAF4")),with=FALSE])
            
          } else if(j=="n4"){
            estfirmtemp <- custommerge(esttemp,
                                       firmtemp[,.(naics,TAZ, CountyFIPS, FAF4, firmid,fsizecat,selected,n4,n2)],bycode = c("n4","TAZ","CountyFIPS","FAF4"),distthresh = Inf)
            estfirmtemp <- estfirmtemp[!is.na(firmid)]
            
            uniquePairs <- estfirmtemp[,{uniqueEst <- length(unique(estid)); estid <- unique(estid); uniqueFirm <- length(unique(firmid)); firmid <- unique(firmid); firmid <- customSample(uniqueFirm,uniqueEst,firmid);data.table(estid,firmid)},by=c("CountyFIPS","FAF4","n4","TAZ")] #;print(paste0("Firm: ",uniqueFirm," Est: ", uniqueEst," Firmselected: ", length(firmid[!is.na(firmid)])))
            
            uniquePairs <- uniquePairs[!is.na(firmid)]
            estfirmtemp <- cbind(estfirmtemp[,.(n4.firm=n4,n4.est=n4,CountyFIPS.firm=CountyFIPS,CountyFIPS.est=CountyFIPS,FAF4.firm=FAF4,FAF4.est=FAF4,TAZ.firm=TAZ,TAZ.est=TAZ)],estfirmtemp[,setdiff(colnames(estfirmtemp),c("n4","TAZ","CountyFIPS","FAF4")),with=FALSE])
          } else {
            estfirmtemp <- custommerge(esttemp,
                                       firmtemp[,.(naics,TAZ, CountyFIPS, FAF4, firmid,fsizecat,selected,n4,n2)],bycode = c("n2","TAZ","CountyFIPS","FAF4"),distthresh = Inf)
            estfirmtemp <- estfirmtemp[!is.na(firmid)]
            
            uniquePairs <- estfirmtemp[,{uniqueEst <- length(unique(estid)); estid <- unique(estid); uniqueFirm <- length(unique(firmid)); firmid <- unique(firmid); firmid <- customSample(uniqueFirm,uniqueEst,firmid);data.table(estid,firmid)},by=c("CountyFIPS","FAF4","n2","TAZ")] #;print(paste0("Firm: ",uniqueFirm," Est: ", uniqueEst," Firmselected: ", length(firmid[!is.na(firmid)])))
            
            uniquePairs <- uniquePairs[!is.na(firmid)]
            estfirmtemp <- cbind(estfirmtemp[,.(n2.firm=n2,n2.est=n2,CountyFIPS.firm=CountyFIPS,CountyFIPS.est=CountyFIPS,FAF4.firm=FAF4,FAF4.est=FAF4,TAZ.firm=TAZ,TAZ.est=TAZ)],estfirmtemp[,setdiff(colnames(estfirmtemp),c("n2","TAZ","CountyFIPS","FAF4")),with=FALSE])
          }
          
          
          sametaznaicslist[[match(j,c("n6","n4","n2"))]]  <- estfirmtemp[uniquePairs[,.(estid,firmid)],on=.(estid,firmid)]
          nFirstFirms <- nFirstEst <- uniquePairs[,.N]
          rm(uniquePairs)
          gc()
          # firm[selected == -1 & firmid %in% unique(estfirmtemp$firmid), selected := 0L]
          beforeFirms <- firm[selected==1L,.N]
          beforeEst <- est[assigned==1L,.N]
          firm[firmid %in% unique(sametaznaicslist[[match(j,c("n6","n4","n2"))]]$firmid), selected := 1L]
          est[estid %in% unique(sametaznaicslist[[match(j,c("n6","n4","n2"))]]$estid), assigned := 1L]
          afterFirms <- firm[selected==1L,.N]
          afterEst <- est[assigned==1L,.N]
          
          print(paste0("Firms/Est Selected: ", nFirstFirms))
          print(paste0("Firms Marked: ", afterFirms-beforeFirms, " Est Marked: ", afterEst-beforeEst))
          print(paste0("Firms Remaining: ", nFirms-nFirstFirms," Est Remaining:", nEst-nFirstEst))
          print("End Same TAZ Pairing...")
        }
        
        for (thresh in distancethreshold){
          esttemp  <- est   [n2 == naics2 & esizecat %in% esizecats & assigned == 0]
          firmtemp <- firm  [n2 == naics2 & fsizecat %in% fsizecats & selected == 0]
          if(esttemp[,.N] > 0 & firmtemp[,.N] > 0){
            print("Starting Distance Based Pairing...")
            nFirms <- firmtemp[,.N]
            nEst <- esttemp[,.N]
            print(paste0("Total Firms: ", nFirms," Total Est: ", nEst))
            if(j=="n6"){
              maxsplit <- ceiling(firmtemp[,.(Nfirm=.N),by=.(naics)][esttemp[,.(Nest=.N),by=.(naics)],.(Nnaics=Nfirm*i.Nest),on="naics"][,sum(Nnaics,na.rm=TRUE)/5e8])
              if(maxsplit>0){
                suppressWarnings(esttemp[,splitn:=1:maxsplit])
                suppressWarnings(firmtemp[,splitn:=1:maxsplit])
                splitcomb <- cbind(rep(1:maxsplit,each=maxsplit),rep(1:maxsplit,times=maxsplit))
                print(paste0("Maximum Split: ", maxsplit, " Combinations: ", nrow(splitcomb)))
                
                estfirmtemp <- rbindlist(apply(splitcomb,1,custommergesplit,esttemp,firmtemp[,.(naics,TAZ, CountyFIPS, FAF4, firmid,fsizecat,selected,n4,n2,splitn)],c("naics"),thresh))
                estfirmtemp <- estfirmtemp[!is.na(firmid)]
              }
              # estfirmtemp <- merge(esttemp,
              #                      firmtemp[,.(naics,TAZ, CountyFIPS, FAF4, firmid,fsizecat,selected)], by = c("naics"),all.x=TRUE,allow.cartesian=TRUE,suffixes = c(".est",".firm"))
            } else if (j=="n4"){
              maxsplit <- ceiling(firmtemp[,.(Nfirm=.N),by=.(n4)][esttemp[,.(Nest=.N),by=.(n4)],.(Nnaics=Nfirm*i.Nest),on="n4"][,sum(Nnaics,na.rm=TRUE)/5e8])
              if(maxsplit>0){
                suppressWarnings(esttemp[,splitn:=1:maxsplit])
                suppressWarnings(firmtemp[,splitn:=1:maxsplit])
                splitcomb <- cbind(rep(1:maxsplit,each=maxsplit),rep(1:maxsplit,times=maxsplit))
                print(paste0("Maximum Split: ", maxsplit, " Combinations: ", nrow(splitcomb)))
                estfirmtemp <- rbindlist(apply(splitcomb,1,custommergesplit,esttemp,firmtemp[,.(naics,TAZ, CountyFIPS, FAF4, firmid,fsizecat,selected,n4,n2,splitn)],c("n4"),thresh))
                estfirmtemp <- estfirmtemp[!is.na(firmid)]
              }
              # estfirmtemp <- merge(esttemp,
              #                      firmtemp[,.(naics,TAZ, CountyFIPS, FAF4, firmid,fsizecat,selected,n4)], by = c("n4"),all.x=TRUE,allow.cartesian=TRUE,suffixes = c(".est",".firm"))
            } else {
              maxsplit <- ceiling(firmtemp[,.(Nfirm=.N),by=.(n2)][esttemp[,.(Nest=.N),by=.(n2)],.(Nnaics=Nfirm*i.Nest),on="n2"][,sum(Nnaics,na.rm=TRUE)/5e8])
              if(maxsplit>0){
                suppressWarnings(esttemp[,splitn:=1:maxsplit])
                suppressWarnings(firmtemp[,splitn:=1:maxsplit])
                splitcomb <- cbind(rep(1:maxsplit,each=maxsplit),rep(1:maxsplit,times=maxsplit))
                print(paste0("Maximum Split: ", maxsplit, " Combinations: ", nrow(splitcomb)))
                estfirmtemp <- rbindlist(apply(splitcomb,1,custommergesplit,esttemp,firmtemp[,.(naics,TAZ, CountyFIPS, FAF4, firmid,fsizecat,selected,n4,n2,splitn)],c("n2"),thresh))
                estfirmtemp <- estfirmtemp[!is.na(firmid)]
              }
              # estfirmtemp <- merge(esttemp,
              #                      firmtemp[,.(naics,TAZ, CountyFIPS, FAF4, firmid,fsizecat,selected,n2)], by = c("n2"),all.x=TRUE,allow.cartesian=TRUE,suffixes = c(".est",".firm"))
            }
            
            # estfirmtemp[,Distance:=TAZDistances[.(TAZ.est,TAZ.firm),GCD]]
            # estfirmtemp <- estfirmtemp[!is.na(Distance)]
            
            # uniqueestid <- unique(estfirmtemp$estid)
            # nuniqueest <- length(uniqueestid)
            # uniquefirmid <- unique(estfirmtemp$firmid)
            # nuniquefirm <- length(uniquefirmid) 
            
            print(paste0("NAICS2: ", naics2))
            print(paste0("Group: ", i))
            # print(paste0("Total Firms: ",nuniquefirm," Total Est: ",nuniqueest))
            print(paste0("Distance Threshold: ", thresh))
            # print(paste0("Total Combinations: ",estfirmtemp[,.N]," Combinations passed: ",estfirmtemp[Distance<thresh,.N]))
            # estfirmtemp <- estfirmtemp[Distance < thresh]
            
            gc()
            # if(i==9) browser()
            # system.time(uniquePairs <- ConnectFirmsToEstablishments(nFirms = nuniquefirm, nEst = nuniqueest, Firms = uniquefirmid, Est = uniqueestid, Pairs = estfirmtemp[,.(estid,firmid,selected,Distance)]))
            if(j=="n6" & maxsplit>0){
              print(system.time(uniquePairs <- estfirmtemp[,{ConnectFirmsToEstablishments2(nFirms = length(unique(firmid)), nEst = length(unique(estid)), Pairs = .SD[,.(estid,firmid,Distance)])},by=.(naics)]))#print(paste0("NAICS: ",unique(naics)));
              # uniquePairs[,setdiff(colnames(uniquePairs),c("firmid","estid")):=NULL]
              # print(paste0("Total Firms Remaining: ", max(0,nuniquefirm-length(uniquePairs$firmid))," Total Est Remaining: ", max(0,nuniqueest-length(uniquePairs$estid))))
              estfirmtemp <- cbind(estfirmtemp[,.(naics.firm=naics,naics.est=naics)],estfirmtemp[,setdiff(colnames(estfirmtemp),c("naics")),with=FALSE])
              
            } else if (j=="n4" & maxsplit>0){
              print(system.time(uniquePairs <- estfirmtemp[,{ConnectFirmsToEstablishments2(nFirms = length(unique(firmid)), nEst = length(unique(estid)), Pairs = .SD[,.(estid,firmid,Distance)])},by=.(n4)])) #print(paste0("NAICS: ",unique(n4)));
              # uniquePairs[,setdiff(colnames(uniquePairs),c("firmid","estid")):=NULL]
              # print(paste0("Total Firms Remaining: ", max(0,nuniquefirm-length(uniquePairs$firmid))," Total Est Remaining: ", max(0,nuniqueest-length(uniquePairs$estid))))
              estfirmtemp <- cbind(estfirmtemp[,.(n4.firm=n4,n4.est=n4)],estfirmtemp[,setdiff(colnames(estfirmtemp),c("n4")),with=FALSE])
            } else if ( maxsplit>0) {
              print(system.time(uniquePairs <- estfirmtemp[,{ConnectFirmsToEstablishments2(nFirms = length(unique(firmid)), nEst = length(unique(estid)), Pairs = .SD[,.(estid,firmid,Distance)])},by=.(n2)])) #print(paste0("NAICS: ",unique(n2)));
              # uniquePairs[,setdiff(colnames(uniquePairs),c("firmid","estid")):=NULL]
              # print(paste0("Total Firms Remaining: ", max(0,nuniquefirm-length(uniquePairs$firmid))," Total Est Remaining: ", max(0,nuniqueest-length(uniquePairs$estid))))
              estfirmtemp <- cbind(estfirmtemp[,.(n2.firm=n2,n2.est=n2)],estfirmtemp[,setdiff(colnames(estfirmtemp),c("n2")),with=FALSE])
            }
            
            # print(system.time(uniquePairs <- ConnectFirmsToEstablishments2(nFirms = nuniquefirm, nEst = nuniqueest, Pairs = estfirmtemp[,.(estid,firmid,Distance)])))
            if(maxsplit>0){
              nSecondFirms <- nSecondEst <- uniquePairs[,.N]
              beforeFirms <- firm[selected==1L,.N]
              beforeEst <- est[assigned==1L,.N]
              threshlist[[match(thresh,distancethreshold)]] <- estfirmtemp[uniquePairs[,.(estid,firmid)],on=.(estid,firmid)]
              firm[firmid %in% unique(threshlist[[match(thresh,distancethreshold)]]$firmid), selected := 1L]
              est[estid %in% unique(threshlist[[match(thresh,distancethreshold)]]$estid), assigned := 1L]
              afterFirms <- firm[selected==1L,.N]
              afterEst <- est[assigned==1L,.N]
              print(paste0("Firms/Est Selected: ", nSecondFirms))
              print(paste0("Firms Marked: ", firmdiff <- afterFirms-beforeFirms," Est Marked: ", estdiff <- afterEst-beforeEst))
              if(firmdiff!=estdiff) browser()
              print(paste0("Firms Remaining: ", nFirms-nSecondFirms," Est Remaining: ", nEst - nSecondEst))
              rm(uniquePairs)
              gc()
            }
          }
        }
        print("Finished Pairing on Different TAZ..")
        difftaznaicslist[[match(j,c("n6","n4","n2"))]] <- rbindlist(threshlist,use.names = TRUE)
      }
    sametazlist <- rbindlist(sametaznaicslist,use.names = TRUE)
    difftazlist <- rbindlist(difftaznaicslist,use.names = TRUE)
    if(sametazlist[,.N]>0) {
      sametazlist[,Distance:=TAZDistances[.(TAZ.firm,TAZ.est),GCD]] 
    } else if (length(colnames(sametazlist))>0) {
      sametazlist[,Distance:=NA]
    }
    # sizelist[[i]] <- estfirmtemp[uniquePairs,on=.(estid,firmid)]
      sizelist[[i]] <- rbindlist(list(sametazlist,difftazlist),use.names=TRUE)
    # firm[selected == -1 & firmid %in% unique(estfirmtemp$firmid), selected := 0L]
    # firm[firmid %in% unique(sizelist[[i]]$firmid), selected := 1L]
    # est[estid %in% unique(sizelist[[i]]$estid), assigned := 1L]
  }
  
  naicslist[[naics2]] <- rbindlist(sizelist,use.names = TRUE)
  save(naicslist,firm,est,file = paste0("./firmByNaics/",naics2,".RData"))
}

load("./firmByNaics/allfirms.RData")
# allfirms <- rbindlist(naicslist)
allfirms[,StateFIPS.est:=StateFIPS]
allfirms[,StateFIPS:=NULL]
allfirms[firm,StateFIPS.firm:=i.StateFIPS,on="firmid"]
### Assigning remaining firms
firm[firmid %in% allfirms[naics.est!=naics.firm,firmid],diffNaics:=1L]
firm[is.na(diffNaics),diffNaics:=0L]
firm[firmid %in% allfirms[Distance>200,firmid],dist200:=1L]
firm[is.na(dist200),dist200:=0L]
firm[,selected2:=0L]

# While all the establishments are not assigned
# Firm Conditions
## 1. Distance greater than 200 miles and different naics
## 2. Different naics
## 3. Distance greater than 200 miles
## 4. Any remaining firm.
naicslist <- list()
for (naics2 in NAICS2){
  sizelist <- list()
  
  for (i in 1:12) {
    print(paste0("NAICS2: ",naics2, " Size: ",i))
    
    estgroup <- estgroups[i] 
    
    #what are the corresponding esizcats and fsizecats
    esizecats <- esize_fsize[EstablishmentSizeNum == estgroup]$EstablishmentSize
    fsizecats <- esize_fsize[EstablishmentSizeNum >= estgroup]$FirmSize
    
    samefaflist <- data.table()
    difffaflist <- data.table()
    
    #get the establishments and firms
    esttemp  <- est[n2 == naics2 & esizecat %in% esizecats & assigned == 0]
    
    while(esttemp[,.N]>0){
      # Pair the firms with different naics (establishments) and distance between firm and establishment greater than 200 miles.
      firmtemp <- firm[n2 == naics2 & fsizecat %in% fsizecats & selected2==0 & dist200==1 & diffNaics==1]
      print(paste0("Establishments remaining: ", est[n2==naics2 & assigned==0,.N]))
      print("Running SAME FAF ZONES")
      print("Running: Diff NAICS and Dist > 200")
      estfirmtemp <- custommerge(esttemp,firmtemp[,.(naics,TAZ,CountyFIPS,FAF4,firmid,fsizecat,selected=selected2,n4,n2)],bycode = c("naics","FAF4"),distthresh = Inf)
      estfirmtemp <- estfirmtemp[!is.na(firmid)]
      print(paste0("Total Firms: ",firmtemp[,.N]," Total Est: ",esttemp[,.N], " Total Combinatios: ", estfirmtemp[,.N]))
      if(estfirmtemp[,.N] > 0){
        uniquePairs <- estfirmtemp[,{uniqueEst <- length(unique(estid)); estid <- unique(estid); uniqueFirm <- length(unique(firmid)); firmid <- unique(firmid); firmid <- customSample(uniqueFirm,uniqueEst,firmid);data.table(estid,firmid)},by=c("naics","FAF4")] #;print(paste0("Firm: ",uniqueFirm," Est: ", uniqueEst," Firmselected: ", length(firmid[!is.na(firmid)])))
        uniquePairs <- uniquePairs[!is.na(firmid)]
        estfirmtemp <- cbind(estfirmtemp[,.(naics.firm=naics,naics.est=naics,FAF4.firm=FAF4,FAF4.est=FAF4)],estfirmtemp[,setdiff(colnames(estfirmtemp),c("naics","FAF4")),with=FALSE])
        samefaflist <- rbindlist(list(samefaflist,estfirmtemp[uniquePairs[,.(estid,firmid)],on=.(estid,firmid)]),use.names = TRUE)
        est[estid %in% uniquePairs[,estid],assigned:=1]
        firm[firmid %in% uniquePairs[,firmid],selected2:=1]
        rm(estfirmtemp,uniquePairs)
        gc()
      } # Done running for diff naics and dist > 200
      print(paste0("Establishments remaining: ", est[n2==naics2 & assigned==0,.N]))
      print("Running SAME FAF ZONES")
      print("Running: Diff NAICS")
      # Get the remaining establishments and firms
      esttemp  <- est[n2 == naics2 & esizecat %in% esizecats & assigned == 0]
      firmtemp <- firm[n2 == naics2 & fsizecat %in% fsizecats & selected2==0 & diffNaics==1]
      estfirmtemp <- custommerge(esttemp,firmtemp[,.(naics,TAZ,CountyFIPS,FAF4,firmid,fsizecat,selected=selected2,n4,n2)],bycode = c("naics","FAF4"),distthresh = Inf)
      estfirmtemp <- estfirmtemp[!is.na(firmid)]
      print(paste0("Total Firms: ",firmtemp[,.N]," Total Est: ",esttemp[,.N], " Total Combinatios: ", estfirmtemp[,.N]))
      if(estfirmtemp[,.N] > 0){
        uniquePairs <- estfirmtemp[,{uniqueEst <- length(unique(estid)); estid <- unique(estid); uniqueFirm <- length(unique(firmid)); firmid <- unique(firmid); firmid <- customSample(uniqueFirm,uniqueEst,firmid);data.table(estid,firmid)},by=c("naics","FAF4")] #;print(paste0("Firm: ",uniqueFirm," Est: ", uniqueEst," Firmselected: ", length(firmid[!is.na(firmid)])))
        uniquePairs <- uniquePairs[!is.na(firmid)]
        estfirmtemp <- cbind(estfirmtemp[,.(naics.firm=naics,naics.est=naics,FAF4.firm=FAF4,FAF4.est=FAF4)],estfirmtemp[,setdiff(colnames(estfirmtemp),c("naics","FAF4")),with=FALSE])
        samefaflist <- rbindlist(list(samefaflist,estfirmtemp[uniquePairs[,.(estid,firmid)],on=.(estid,firmid)]),use.names = TRUE)
        est[estid %in% uniquePairs[,estid],assigned:=1]
        firm[firmid %in% uniquePairs[,firmid],selected2:=1]
        rm(estfirmtemp,uniquePairs)
        gc()
      } # Done running for diff naics
      print(paste0("Establishments remaining: ", est[n2==naics2 & assigned==0,.N]))
      print("Running SAME FAF ZONES")
      print("Running: Dist>200")
      esttemp  <- est[n2 == naics2 & esizecat %in% esizecats & assigned == 0]
      firmtemp <- firm[n2 == naics2 & fsizecat %in% fsizecats & selected2==0 & dist200==1]
      estfirmtemp <- custommerge(esttemp,firmtemp[,.(naics,TAZ,CountyFIPS,FAF4,firmid,fsizecat,selected=selected2,n4,n2)],bycode = c("naics","FAF4"),distthresh = Inf)
      estfirmtemp <- estfirmtemp[!is.na(firmid)]
      print(paste0("Total Firms: ",firmtemp[,.N]," Total Est: ",esttemp[,.N], " Total Combinatios: ", estfirmtemp[,.N]))
      if(estfirmtemp[,.N] > 0){
        uniquePairs <- estfirmtemp[,{uniqueEst <- length(unique(estid)); estid <- unique(estid); uniqueFirm <- length(unique(firmid)); firmid <- unique(firmid); firmid <- customSample(uniqueFirm,uniqueEst,firmid);data.table(estid,firmid)},by=c("naics","FAF4")] #;print(paste0("Firm: ",uniqueFirm," Est: ", uniqueEst," Firmselected: ", length(firmid[!is.na(firmid)])))
        uniquePairs <- uniquePairs[!is.na(firmid)]
        estfirmtemp <- cbind(estfirmtemp[,.(naics.firm=naics,naics.est=naics,FAF4.firm=FAF4,FAF4.est=FAF4)],estfirmtemp[,setdiff(colnames(estfirmtemp),c("naics","FAF4")),with=FALSE])
        samefaflist <- rbindlist(list(samefaflist,estfirmtemp[uniquePairs[,.(estid,firmid)],on=.(estid,firmid)]),use.names = TRUE)
        est[estid %in% uniquePairs[,estid],assigned:=1]
        firm[firmid %in% uniquePairs[,firmid],selected2:=1]
        rm(estfirmtemp,uniquePairs)
        gc()
      } # Done running for dist > 200
      print(paste0("Establishments remaining: ", est[n2==naics2 & assigned==0,.N]))
      print("Running Different FAF ZONES")
      print("Running: Diff NAICS and Dist > 200")
      
      esttemp  <- est[n2 == naics2 & esizecat %in% esizecats & assigned == 0]
      firmtemp <- firm[n2 == naics2 & fsizecat %in% fsizecats & selected2==0 & diffNaics==1 & dist200==1]
      maxsplit <- ceiling(firmtemp[,.(Nfirm=.N),by=.(naics)][esttemp[,.(Nest=.N),by=.(naics)],.(Nnaics=Nfirm*i.Nest),on="naics"][,sum(Nnaics,na.rm=TRUE)/5e8])
      
      if(maxsplit>0){
        suppressWarnings(esttemp[,splitn:=1:maxsplit])
        suppressWarnings(firmtemp[,splitn:=1:maxsplit])
        splitcomb <- cbind(rep(1:maxsplit,each=maxsplit),rep(1:maxsplit,times=maxsplit))
        print(paste0("Maximum Split: ", maxsplit, " Combinations: ", nrow(splitcomb)))
        estfirmtemp <- rbindlist(apply(splitcomb,1,custommergesplit,esttemp,firmtemp[,.(naics,TAZ, CountyFIPS, FAF4, firmid,fsizecat,selected=selected2,n4,n2,splitn)],c("naics"),Inf))
        estfirmtemp <- estfirmtemp[!is.na(firmid)]
        print(paste0("Total Firms: ",firmtemp[,.N]," Total Est: ",esttemp[,.N], " Total Combinatios: ", estfirmtemp[,.N]))
        if(estfirmtemp[,.N] > 0){
          estfirmtemp[allfirms[,AverageDistance:=mean(Distance),.(firmid)],weights:=1/i.AverageDistance,on=.(firmid)]
          print(system.time(uniquePairs <- estfirmtemp[,{ConnectFirmsToEstablishments2(nFirms = length(unique(firmid)), nEst = length(unique(estid)), Pairs = .SD[,.(estid,firmid,Distance=Distance*weights)])},by=.(naics)]))
          uniquePairs <- uniquePairs[!is.na(firmid)]
          estfirmtemp[,weights:=NULL]
          estfirmtemp[,c("naics.firm","naics.est"):=naics]
          estfirmtemp[,naics:=NULL]
          difffaflist <- rbindlist(list(difffaflist,estfirmtemp[uniquePairs[,.(estid,firmid)],on=.(estid,firmid)]),use.names = TRUE)
          est[estid %in% uniquePairs[,estid],assigned:=1]
          firm[firmid %in% uniquePairs[,firmid],selected2:=1]
          rm(estfirmtemp,uniquePairs)
          gc()
        } # Done running for diff naics and dist > 200
      }
      print(paste0("Establishments remaining: ", est[n2==naics2 & assigned==0,.N]))
      print("Running Different FAF ZONES")
      print("Running: Diff NAICS")
      
      esttemp  <- est[n2 == naics2 & esizecat %in% esizecats & assigned == 0]
      firmtemp <- firm[n2 == naics2 & fsizecat %in% fsizecats & selected2==0 & diffNaics==1]
      maxsplit <- ceiling(firmtemp[,.(Nfirm=.N),by=.(naics)][esttemp[,.(Nest=.N),by=.(naics)],.(Nnaics=Nfirm*i.Nest),on="naics"][,sum(Nnaics,na.rm=TRUE)/5e8])
      
      if(maxsplit>0){
        suppressWarnings(esttemp[,splitn:=1:maxsplit])
        suppressWarnings(firmtemp[,splitn:=1:maxsplit])
        splitcomb <- cbind(rep(1:maxsplit,each=maxsplit),rep(1:maxsplit,times=maxsplit))
        print(paste0("Maximum Split: ", maxsplit, " Combinations: ", nrow(splitcomb)))
        estfirmtemp <- rbindlist(apply(splitcomb,1,custommergesplit,esttemp,firmtemp[,.(naics,TAZ, CountyFIPS, FAF4, firmid,fsizecat,selected=selected2,n4,n2,splitn)],c("naics"),Inf))
        estfirmtemp <- estfirmtemp[!is.na(firmid)]
        print(paste0("Total Firms: ",firmtemp[,.N]," Total Est: ",esttemp[,.N], " Total Combinatios: ", estfirmtemp[,.N]))
        if(estfirmtemp[,.N] > 0){
          estfirmtemp[allfirms[,AverageDistance:=mean(Distance),.(firmid)],weights:=1/i.AverageDistance,on=.(firmid)]
          print(system.time(uniquePairs <- estfirmtemp[,{ConnectFirmsToEstablishments2(nFirms = length(unique(firmid)), nEst = length(unique(estid)), Pairs = .SD[,.(estid,firmid,Distance=Distance*weights)])},by=.(naics)]))
          uniquePairs <- uniquePairs[!is.na(firmid)]
          estfirmtemp[,weights:=NULL]
          estfirmtemp[,c("naics.firm","naics.est"):=naics]
          estfirmtemp[,naics:=NULL]
          difffaflist <- rbindlist(list(difffaflist,estfirmtemp[uniquePairs[,.(estid,firmid)],on=.(estid,firmid)]),use.names = TRUE)
          est[estid %in% uniquePairs[,estid],assigned:=1]
          firm[firmid %in% uniquePairs[,firmid],selected2:=1]
          rm(estfirmtemp,uniquePairs)
          gc()
        } # Done running for diff naics
      }
      print(paste0("Establishments remaining: ", est[n2==naics2 & assigned==0,.N]))
      print("Running Different FAF ZONES")
      print("Running: Dist > 200")
      
      esttemp  <- est[n2 == naics2 & esizecat %in% esizecats & assigned == 0]
      firmtemp <- firm[n2 == naics2 & fsizecat %in% fsizecats & selected2==0 & dist200==1]
      maxsplit <- ceiling(firmtemp[,.(Nfirm=.N),by=.(naics)][esttemp[,.(Nest=.N),by=.(naics)],.(Nnaics=Nfirm*i.Nest),on="naics"][,sum(Nnaics,na.rm=TRUE)/5e8])
      
      if(maxsplit>0){
        suppressWarnings(esttemp[,splitn:=1:maxsplit])
        suppressWarnings(firmtemp[,splitn:=1:maxsplit])
        splitcomb <- cbind(rep(1:maxsplit,each=maxsplit),rep(1:maxsplit,times=maxsplit))
        print(paste0("Maximum Split: ", maxsplit, " Combinations: ", nrow(splitcomb)))
        estfirmtemp <- rbindlist(apply(splitcomb,1,custommergesplit,esttemp,firmtemp[,.(naics,TAZ, CountyFIPS, FAF4, firmid,fsizecat,selected=selected2,n4,n2,splitn)],c("naics"),Inf))
        estfirmtemp <- estfirmtemp[!is.na(firmid)]
        print(paste0("Total Firms: ",firmtemp[,.N]," Total Est: ",esttemp[,.N], " Total Combinatios: ", estfirmtemp[,.N]))
        if(estfirmtemp[,.N] > 0){
          estfirmtemp[allfirms[,AverageDistance:=mean(Distance),.(firmid)],weights:=1/i.AverageDistance,on=.(firmid)]
          print(system.time(uniquePairs <- estfirmtemp[,{ConnectFirmsToEstablishments2(nFirms = length(unique(firmid)), nEst = length(unique(estid)), Pairs = .SD[,.(estid,firmid,Distance=Distance*weights)])},by=.(naics)]))
          uniquePairs <- uniquePairs[!is.na(firmid)]
          estfirmtemp[,weights:=NULL]
          estfirmtemp[,c("naics.firm","naics.est"):=naics]
          estfirmtemp[,naics:=NULL]
          difffaflist <- rbindlist(list(difffaflist,estfirmtemp[uniquePairs[,.(estid,firmid)],on=.(estid,firmid)]),use.names = TRUE)
          est[estid %in% uniquePairs[,estid],assigned:=1]
          firm[firmid %in% uniquePairs[,firmid],selected2:=1]
          rm(estfirmtemp,uniquePairs)
          gc()
        } # Done running for dist > 200
      }
      print(paste0("Establishments remaining: ", est[n2==naics2 & assigned==0,.N]))
      print("Running SAME FAF ZONES")
      print("Running: Firms with one establishment")
      # Get the remaining establishments and firms
      esttemp  <- est[n2 == naics2 & esizecat %in% esizecats & assigned == 0]
      firmtemp <- firm[n2 == naics2 & fsizecat %in% fsizecats & selected2==0]
      estfirmtemp <- custommerge(esttemp,firmtemp[,.(naics,TAZ,CountyFIPS,FAF4,firmid,fsizecat,selected=selected2,n4,n2)],bycode = c("naics","FAF4"),distthresh = Inf)
      estfirmtemp <- estfirmtemp[!is.na(firmid)]
      print(paste0("Total Firms: ",firmtemp[,.N]," Total Est: ",esttemp[,.N], " Total Combinatios: ", estfirmtemp[,.N]))
      if(estfirmtemp[,.N] > 0){
        uniquePairs <- estfirmtemp[,{uniqueEst <- length(unique(estid)); estid <- unique(estid); uniqueFirm <- length(unique(firmid)); firmid <- unique(firmid); firmid <- customSample(uniqueFirm,uniqueEst,firmid);data.table(estid,firmid)},by=c("naics","FAF4")] #;print(paste0("Firm: ",uniqueFirm," Est: ", uniqueEst," Firmselected: ", length(firmid[!is.na(firmid)])))
        uniquePairs <- uniquePairs[!is.na(firmid)]
        estfirmtemp <- cbind(estfirmtemp[,.(naics.firm=naics,naics.est=naics,FAF4.firm=FAF4,FAF4.est=FAF4)],estfirmtemp[,setdiff(colnames(estfirmtemp),c("naics","FAF4")),with=FALSE])
        samefaflist <- rbindlist(list(samefaflist,estfirmtemp[uniquePairs[,.(estid,firmid)],on=.(estid,firmid)]),use.names = TRUE)
        est[estid %in% uniquePairs[,estid],assigned:=1]
        firm[firmid %in% uniquePairs[,firmid],selected2:=1]
        rm(estfirmtemp,uniquePairs)
        gc()
      } # Done running firms with one establishment
      print(paste0("Establishments remaining: ", est[n2==naics2 & assigned==0,.N]))
      print("Running Different FAF ZONES")
      print("Running: Firms with one establishment")
      
      esttemp  <- est[n2 == naics2 & esizecat %in% esizecats & assigned == 0]
      firmtemp <- firm[n2 == naics2 & fsizecat %in% fsizecats & selected2==0]
      maxsplit <- ceiling(firmtemp[,.(Nfirm=.N),by=.(naics)][esttemp[,.(Nest=.N),by=.(naics)],.(Nnaics=Nfirm*i.Nest),on="naics"][,sum(Nnaics,na.rm=TRUE)/5e8])
      
      if(maxsplit>0){
        suppressWarnings(esttemp[,splitn:=1:maxsplit])
        suppressWarnings(firmtemp[,splitn:=1:maxsplit])
        splitcomb <- cbind(rep(1:maxsplit,each=maxsplit),rep(1:maxsplit,times=maxsplit))
        print(paste0("Maximum Split: ", maxsplit, " Combinations: ", nrow(splitcomb)))
        estfirmtemp <- rbindlist(apply(splitcomb,1,custommergesplit,esttemp,firmtemp[,.(naics,TAZ, CountyFIPS, FAF4, firmid,fsizecat,selected=selected2,n4,n2,splitn)],c("naics"),Inf))
        estfirmtemp <- estfirmtemp[!is.na(firmid)]
        print(paste0("Total Firms: ",firmtemp[,.N]," Total Est: ",esttemp[,.N], " Total Combinatios: ", estfirmtemp[,.N]))
        if(estfirmtemp[,.N] > 0){
          estfirmtemp[allfirms[,AverageDistance:=mean(Distance),.(firmid)],weights:=1/i.AverageDistance,on=.(firmid)]
          print(system.time(uniquePairs <- estfirmtemp[,{ConnectFirmsToEstablishments2(nFirms = length(unique(firmid)), nEst = length(unique(estid)), Pairs = .SD[,.(estid,firmid,Distance=Distance*weights)])},by=.(naics)]))
          uniquePairs <- uniquePairs[!is.na(firmid)]
          estfirmtemp[,weights:=NULL]
          estfirmtemp[,c("naics.firm","naics.est"):=naics]
          estfirmtemp[,naics:=NULL]
          difffaflist <- rbindlist(list(difffaflist,estfirmtemp[uniquePairs[,.(estid,firmid)],on=.(estid,firmid)]),use.names = TRUE)
          est[estid %in% uniquePairs[,estid],assigned:=1]
          firm[firmid %in% uniquePairs[,firmid],selected2:=1]
          rm(estfirmtemp,uniquePairs)
          gc()
        } # Done running firms with one establishment
      }
      print(paste0("Establishments remaining: ", est[n2==naics2 & assigned==0,.N]))
      
      print("Running SAME FAF ZONES")
      print("Running: Firms with more than one establishment")
      # Get the remaining establishments and firms
      esttemp  <- est[n2 == naics2 & esizecat %in% esizecats & assigned == 0]
      firmtemp <- firm[n2 == naics2 & fsizecat %in% fsizecats]
      estfirmtemp <- custommerge(esttemp,firmtemp[,.(naics,TAZ,CountyFIPS,FAF4,firmid,fsizecat,selected=selected2,n4,n2)],bycode = c("naics","FAF4"),distthresh = Inf)
      estfirmtemp <- estfirmtemp[!is.na(firmid)]
      print(paste0("Total Firms: ",firmtemp[,.N]," Total Est: ",esttemp[,.N], " Total Combinatios: ", estfirmtemp[,.N]))
      if(estfirmtemp[,.N] > 0){
        uniquePairs <- estfirmtemp[,{uniqueEst <- length(unique(estid)); estid <- unique(estid); uniqueFirm <- length(unique(firmid)); firmprobs <- unique(.SD[,.(firmid,prob=1/pmax(selected,1))]); firmid <- firmprobs$firmid; prob=firmprobs$prob; firmid <- customSample(uniqueFirm,uniqueEst,firmid,prob = prob,replace = TRUE);data.table(estid,firmid)},by=c("naics","FAF4")] #;print(paste0("Firm: ",uniqueFirm," Est: ", uniqueEst," Firmselected: ", length(firmid[!is.na(firmid)])))
        uniquePairs <- uniquePairs[!is.na(firmid)]
        estfirmtemp <- cbind(estfirmtemp[,.(naics.firm=naics,naics.est=naics,FAF4.firm=FAF4,FAF4.est=FAF4)],estfirmtemp[,setdiff(colnames(estfirmtemp),c("naics","FAF4")),with=FALSE])
        samefaflist <- rbindlist(list(samefaflist,estfirmtemp[uniquePairs[,.(estid,firmid)],on=.(estid,firmid)]),use.names = TRUE)
        est[estid %in% uniquePairs[,estid],assigned:=1]
        firm[uniquePairs[,.N,.(firmid)],selected2:=selected2+i.N,on=.(firmid)]
        # firm[firmid %in% uniquePairs[,firmid],selected2:=selected2+1]
        rm(estfirmtemp,uniquePairs)
        gc()
      } # Done running firms with one establishment
      print(paste0("Establishments remaining: ", est[n2==naics2 & assigned==0,.N]))
      
      print("Running Different FAF ZONES")
      print("Running: Firms with more than one establishment")
      
      esttemp  <- est[n2 == naics2 & esizecat %in% esizecats & assigned == 0]
      firmtemp <- firm[n2 == naics2 & fsizecat %in% fsizecats]
      maxsplit <- ceiling(firmtemp[,.(Nfirm=.N),by=.(naics)][esttemp[,.(Nest=.N),by=.(naics)],.(Nnaics=Nfirm*i.Nest),on="naics"][,sum(Nnaics,na.rm=TRUE)/5e8])
      
      if(maxsplit>0){
        suppressWarnings(esttemp[,splitn:=1:maxsplit])
        suppressWarnings(firmtemp[,splitn:=1:maxsplit])
        splitcomb <- cbind(rep(1:maxsplit,each=maxsplit),rep(1:maxsplit,times=maxsplit))
        print(paste0("Maximum Split: ", maxsplit, " Combinations: ", nrow(splitcomb)))
        estfirmtemp <- rbindlist(apply(splitcomb,1,custommergesplit,esttemp,firmtemp[,.(naics,TAZ, CountyFIPS, FAF4, firmid,fsizecat,selected=selected2,n4,n2,splitn)],c("naics"),Inf))
        estfirmtemp <- estfirmtemp[!is.na(firmid)]
        print(paste0("Total Firms: ",firmtemp[,.N]," Total Est: ",esttemp[,.N], " Total Combinatios: ", estfirmtemp[,.N]))
        if(estfirmtemp[,.N] > 0){
          estfirmtemp[allfirms[,AverageDistance:=mean(Distance),.(firmid)],weights:=1/i.AverageDistance,on=.(firmid)]
          print(system.time(uniquePairs <- estfirmtemp[,{ConnectFirmsToEstablishments2(nFirms = length(unique(firmid)), nEst = length(unique(estid)), Pairs = .SD[,.(estid,firmid,Distance=Distance*weights*pmax(selected,1))])},by=.(naics)]))
          uniquePairs <- uniquePairs[!is.na(firmid)]
          estfirmtemp[,weights:=NULL]
          estfirmtemp[,c("naics.firm","naics.est"):=naics]
          estfirmtemp[,naics:=NULL]
          difffaflist <- rbindlist(list(difffaflist,estfirmtemp[uniquePairs[,.(estid,firmid)],on=.(estid,firmid)]),use.names = TRUE)
          est[estid %in% uniquePairs[,estid],assigned:=1]
          firm[firmid %in% uniquePairs[,firmid],selected2:=selected2+1]
          rm(estfirmtemp,uniquePairs)
          gc()
        } # Done running firms with more than one establishment
      }
      print(paste0("Establishments remaining: ", est[n2==naics2 & assigned==0,.N]))
      esttemp  <- est[n2 == naics2 & esizecat %in% esizecats & assigned == 0]
      firmtemp <- firm[n2 == naics2 & fsizecat %in% fsizecats]
      
      
      if(firm[n2 == naics2 & fsizecat %in% fsizecats & selected2==0,.N]<1){
        print("All Firms have more than two establishments")
        print("Running Different FAF ZONES")
        # print("Running: Firms with more than one establishment")
        esttemp  <- est[n2 == naics2 & esizecat %in% esizecats & assigned == 0]
        firmtemp <- firm[n2 == naics2 & fsizecat %in% fsizecats]
        maxsplit <- ceiling(firmtemp[,.(Nfirm=.N),by=.(naics)][esttemp[,.(Nest=.N),by=.(naics)],.(Nnaics=Nfirm*i.Nest),on="naics"][,sum(Nnaics,na.rm=TRUE)/5e8])
        
        if(maxsplit>0){
          suppressWarnings(esttemp[,splitn:=1:maxsplit])
          suppressWarnings(firmtemp[,splitn:=1:maxsplit])
          splitcomb <- cbind(rep(1:maxsplit,each=maxsplit),rep(1:maxsplit,times=maxsplit))
          print(paste0("Maximum Split: ", maxsplit, " Combinations: ", nrow(splitcomb)))
          estfirmtemp <- rbindlist(apply(splitcomb,1,custommergesplit,esttemp,firmtemp[,.(naics,TAZ, CountyFIPS, FAF4, firmid,fsizecat,selected=selected2,n4,n2,splitn)],c("naics"),Inf))
          estfirmtemp <- estfirmtemp[!is.na(firmid)]
          print(paste0("Total Firms: ",firmtemp[,.N]," Total Est: ",esttemp[,.N], " Total Combinatios: ", estfirmtemp[,.N]))
          if(estfirmtemp[,.N] > 0){
            estfirmtemp[allfirms[,AverageDistance:=mean(Distance),.(firmid)],weights:=1/i.AverageDistance,on=.(firmid)]
            # uniquePairs <- estfirmtemp[,{uniqueEst <- length(unique(estid)); estid <- unique(estid); uniqueFirm <- length(unique(firmid)); firmid <- unique(firmid); firmid <- customSample(uniqueFirm,uniqueEst,firmid,prob = 1/pmax(selected,1),replace = TRUE);data.table(estid,firmid)},by=c("naics")]
            print(system.time(uniquePairs <- estfirmtemp[,{ConnectFirmsToEstablishments2(nFirms = length(unique(firmid)), nEst = length(unique(estid)), Pairs = .SD[,.(estid,firmid,Distance=Distance*weights*pmax(selected,1))],maxmatch = FALSE)},by=.(naics)]))
            uniquePairs <- uniquePairs[!is.na(firmid)]
            estfirmtemp[,weights:=NULL]
            estfirmtemp[,c("naics.firm","naics.est"):=naics]
            estfirmtemp[,naics:=NULL]
            difffaflist <- rbindlist(list(difffaflist,estfirmtemp[uniquePairs[,.(estid,firmid)],on=.(estid,firmid)]),use.names = TRUE)
            est[estid %in% uniquePairs[,estid],assigned:=1]
            firm[uniquePairs[,.N,firmid],selected2:=selected2+i.N,on=.(firmid)]
            # firm[firmid %in% uniquePairs[,firmid],selected2:=selected2+1]
            rm(estfirmtemp,uniquePairs)
            gc()
          } # Done running firms with more than one establishment
        }
      }
      esttemp  <- est[n2 == naics2 & esizecat %in% esizecats & assigned == 0]
    }
    if(samefaflist[,.N]>0) {
      samefaflist[,Distance:=TAZDistances[.(TAZ.firm,TAZ.est),GCD]] 
    } else if (length(colnames(samefaflist))>0) {
      samefaflist[,Distance:=NA]
    }
    sizelist[[i]] <- rbindlist(list(samefaflist,difffaflist),use.names=TRUE)
  }
  naicslist[[naics2]] <- rbindlist(sizelist,use.names = TRUE)
  save(naicslist,firm,est,file = paste0("./firmByNaics/",naics2,"_second.RData"))
}

