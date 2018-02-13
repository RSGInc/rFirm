# In order to make sure all firms are assigned:
# 1 Find at least one establishment for each firm in the same county and naics
# 2 For the establishments with no firm found, find the closest firm county to the establishment county in the following order:
##  1. Same 6 digit NAICS
##  2. Same 4 digit NAICS
##  3. Same 2 digit NAICS

#####################################################
#####################################################
## Load the required libraries
#####################################################
#####################################################

library(data.table)
library(rgdal)
library(igraph)
library(Matrix)

library(rFreight, lib.loc = "pkgs")

#----------------------------------------------------------------------------------------------
## Allocate a single establishment to each firm as its "headquarters" (or only estasblishment)

###########################################
## Define Functions
###########################################

# Customized function to sample firms and establishments
# nFirms: A positive number of firms
# nEst: A positive number of establishments
# Firms: A vector of firms
# prob: A vector of probability weights for obtaining the elements of the vector being sampled.
# replace: Should sampling be with replacement?

# Returns: A vector of firms
customSample <- function(nFirm,nEst,Firms,prob=NULL,replace=FALSE){
  # Check the length of firms and establishments passed to the function
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
} #end customSample


# Function to match firms with establishments
# nFirms: A positive number of firms
# nEst: A positive number of establishments
# Pairs: A data.table with firmid, estid, and the distance between them
# maxmatch: Should the matching problem be set to maximum match?

# Returns a data.table with matched firmid and estid
ConnectFirmsToEstablishments <- function(nFirms,nEst,Pairs,maxmatch=TRUE){
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
        browser()
        # print(paste0("nFirms: ", nFirms, " nEst: ", nEst, " Combinations: ", Pairs[,.N]))
        print(paste0("Firm: ",nFirms," Est: ", nEst," Firmselected: ",nEst))
        return(Pairs[,.(estid,firmid)])
      } else {
        browser
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
} #end ConnectFirmsToEstablishments

# Function to merge firm and establishment by splitting into groups
# splits: A vector of length 2 indicating the split number for est and firm resp.
# est: A data.table of establishments with required columns
# bycode: A vector of shared column names in est and firm to merge on.
# distthresh: A numeric indicating the distance threshold to filter out the firms 
# and establishments pair

# Returns a merged table of firms and establishments
custommergesplit <- function(splits,est,firm,bycode,distthresh = Inf){
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
} #end custommergesplit

# Function to merge firm and establishment w/o splitting into groups
# est: A data.table of establishments with required columns
# bycode: A vector of shared column names in est and firm to merge on.
# distthresh: A numeric indicating the distance threshold to filter out the firms 
# and establishments pair

# Returns a merged table of firms and establishments
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
} #end custommerge

# Function to convert a factor to numeric
FactorToNumeric <- function(x) return(as.numeric(as.character(x))) #end


##################################################################
## Load data
#############################################################


# Load in the example files and correspodences
est <- fread("outputs/cbp.establishments_final.csv")
firm <- fread("outputs/cbp.firms_final.csv")
esize_fsize <- fread("inputs/corresp_estsize_firmsize.csv")
setkey(esize_fsize, EstablishmentSizeNum, FirmSizeNum)

unzip("inputs/NUMA_Polygon.zip")
nfmtaz <- readOGR(".","NUMA_Polygon")
file.remove("NUMA_Polygon.shp")
file.remove("NUMA_Polygon.shx")
file.remove("NUMA_Polygon.dbf")
file.remove("NUMA_Polygon.prj")
NUMAZones <- data.table(nfmtaz@data)
NUMAZones <- NUMAZones[, .(FID_1, STATEFP1_1, GEOID10_1, LONG = FactorToNumeric(INTPTLON_1), LAT = FactorToNumeric(INTPTLAT_1))]
TAZDistances <- rFreight::pairsGCD(NUMAZones[,.(FID_1,LONG,LAT)])
setnames(TAZDistances,c("oFID_1","dFID_1"),c("oTAZ","dTAZ"))
setkey(TAZDistances,oTAZ,dTAZ)


##################################################################
## Modify data
#############################################################

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

# Has the firm been selected
# Has the establishment been assigned
firm[, selected := 0L]
est[, assigned:=0L]

# Set the seed
set.seed(151)

### Logic
## 1. Get the unique establishments and firm by the naics group and the size
## 2. Create a list of unique firms and unique establishments.
## 3. Loop through the firm and for each firm select the closest establishment.
## 4. Mark the establishments and firms as assigned/selected if matched.

# Create a list to store the matched pairs
naicslist <- list()
# A vector of 2 digit NAICS code to loop over
NAICS2 <- unique(est$n2)

# Create a sequence of distance thresholds to loop over
# This helps in reducing run time and the size of the problem
distancethreshold <- c(100,250,500,1000,Inf) # For sub optimal solution to maximum matching problem

est[,n4:=substr(naics,1,4)]
firm[,n4:=substr(naics,1,4)]

employee_size_categories = 1:12

## For debugging/testing
if (exists("DEBUG")) {
  if (DEBUG) {
    print("####### Running DEBUG mode ########")
    NAICS2 <- NAICS2[1] #use only first
  } 
}

# Loop over 2-digit naics to avoid memory issue
for (naics2 in NAICS2){
  # List to store the results of looping over different size category
  sizelist <- list()
  # Loop over employee size category
  for (i in employee_size_categories) {
    print(paste0("NAICS2: ",naics2, " Size: ",i))
    
    estgroup <- estgroups[i] 
    
    #what are the corresponding esizcats and fsizecats
    esizecats <- esize_fsize[EstablishmentSizeNum == estgroup]$EstablishmentSize
    fsizecats <- esize_fsize[EstablishmentSizeNum >= estgroup]$FirmSize
    
    # List to store results of same or different taz pairing.
    sametaznaicslist <- list()
    difftaznaicslist <- list()
      
    # Loop over naics type code for merging
      for(j in c("n6", "n4", "n2")){
        print(j)
        # List to store the results of looping over distance threshold
        threshlist <- list()
        
        
        #get the establishments and firms
        esttemp  <- est   [n2 == naics2 & esizecat %in% esizecats & assigned == 0]
        firmtemp <- firm  [n2 == naics2 & fsizecat %in% fsizecats & selected == 0]
        if(esttemp[,.N] > 0 & firmtemp[,.N] > 0){
          print("Running Same TAZ Pairing....")
          nFirms <- firmtemp[,.N]
          nEst <- esttemp[,.N]
          print(paste0("Total Firms: ",nFirms," Total Est: ",nEst ))
          # First, pair firm and establishments in the same TAZ
          if(j=="n6"){
            # Merge
            estfirmtemp <- custommerge(esttemp,
                                       firmtemp[,.(naics,TAZ, CountyFIPS, FAF4, firmid,fsizecat,selected,n4,n2)],
                                       bycode = c("naics","TAZ","CountyFIPS","FAF4"),
                                       distthresh = Inf)
            estfirmtemp <- estfirmtemp[!is.na(firmid)]
            
            # Find unique pairs
            uniquePairs <- estfirmtemp[,{uniqueEst <- length(unique(estid)); 
            estid <- unique(estid); 
            uniqueFirm <- length(unique(firmid)); 
            firmid <- unique(firmid); 
            firmid <- customSample(uniqueFirm,
                                   uniqueEst,
                                   firmid);
            data.table(estid,firmid)},by=c("naics","CountyFIPS","FAF4","TAZ")] #;print(paste0("Firm: ",uniqueFirm," Est: ", uniqueEst," Firmselected: ", length(firmid[!is.na(firmid)])))
            uniquePairs <- uniquePairs[!is.na(firmid)]
            
            # Modify estfirmtemp to contain specific columns
            estfirmtemp <- cbind(estfirmtemp[,.(naics.firm=naics,naics.est=naics,CountyFIPS.firm=CountyFIPS,CountyFIPS.est=CountyFIPS,FAF4.firm=FAF4,FAF4.est=FAF4,TAZ.firm=TAZ,TAZ.est=TAZ)],estfirmtemp[,setdiff(colnames(estfirmtemp),c("naics","TAZ","CountyFIPS","FAF4")),with=FALSE])
            
          } else if(j=="n4"){
            # Merge
            estfirmtemp <- custommerge(esttemp,
                                       firmtemp[,.(naics,TAZ, CountyFIPS, FAF4, firmid,fsizecat,selected,n4,n2)],
                                       bycode = c("n4","TAZ","CountyFIPS","FAF4"),
                                       distthresh = Inf)
            estfirmtemp <- estfirmtemp[!is.na(firmid)]
            
            # Find unique pairs
            uniquePairs <- estfirmtemp[,{uniqueEst <- length(unique(estid)); 
            estid <- unique(estid); 
            uniqueFirm <- length(unique(firmid)); 
            firmid <- unique(firmid); 
            firmid <- customSample(uniqueFirm,
                                   uniqueEst,
                                   firmid);
            data.table(estid,firmid)}, by=c("CountyFIPS","FAF4","n4","TAZ")] #;print(paste0("Firm: ",uniqueFirm," Est: ", uniqueEst," Firmselected: ", length(firmid[!is.na(firmid)])))
            uniquePairs <- uniquePairs[!is.na(firmid)]
            
            # Modify estfirmtemp to contain specific columns
            estfirmtemp <- cbind(estfirmtemp[,.(n4.firm=n4,n4.est=n4,CountyFIPS.firm=CountyFIPS,CountyFIPS.est=CountyFIPS,FAF4.firm=FAF4,FAF4.est=FAF4,TAZ.firm=TAZ,TAZ.est=TAZ)],estfirmtemp[,setdiff(colnames(estfirmtemp),c("n4","TAZ","CountyFIPS","FAF4")),with=FALSE])
          } else {
            # Merge
            estfirmtemp <- custommerge(esttemp,
                                       firmtemp[,.(naics,TAZ, CountyFIPS, FAF4, firmid,fsizecat,selected,n4,n2)],
                                       bycode = c("n2","TAZ","CountyFIPS","FAF4"),
                                       distthresh = Inf)
            estfirmtemp <- estfirmtemp[!is.na(firmid)]
            
            # Find unique pairs
            uniquePairs <- estfirmtemp[,{uniqueEst <- length(unique(estid)); 
            estid <- unique(estid); 
            uniqueFirm <- length(unique(firmid)); 
            firmid <- unique(firmid); 
            firmid <- customSample(uniqueFirm,
                                   uniqueEst,
                                   firmid);
            data.table(estid,firmid)},by=c("CountyFIPS","FAF4","n2","TAZ")] #;print(paste0("Firm: ",uniqueFirm," Est: ", uniqueEst," Firmselected: ", length(firmid[!is.na(firmid)])))
            uniquePairs <- uniquePairs[!is.na(firmid)]
            
            # Modify estfirmtemp to contain specific columns
            estfirmtemp <- cbind(estfirmtemp[,.(n2.firm=n2,n2.est=n2,CountyFIPS.firm=CountyFIPS,CountyFIPS.est=CountyFIPS,FAF4.firm=FAF4,FAF4.est=FAF4,TAZ.firm=TAZ,TAZ.est=TAZ)],estfirmtemp[,setdiff(colnames(estfirmtemp),c("n2","TAZ","CountyFIPS","FAF4")),with=FALSE])
          } #end if j
          
          # Store the results in sametaznaicslist
          sametaznaicslist[[match(j,c("n6","n4","n2"))]]  <- estfirmtemp[uniquePairs[,.(estid,firmid)],on=.(estid,firmid)]
          nFirstFirms <- nFirstEst <- uniquePairs[,.N]
          rm(uniquePairs)
          gc()
          
          # Calculate some summaries
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
        } #end if esttemp[,.N] > 0 & firmtemp[,.N] > 0
        
        # If not in same taz then loop of distance threshold values
        for (thresh in distancethreshold){
          # Collect the remaining establishments and firms
          esttemp  <- est   [n2 == naics2 & esizecat %in% esizecats & assigned == 0]
          firmtemp <- firm  [n2 == naics2 & fsizecat %in% fsizecats & selected == 0]
          if(esttemp[,.N] > 0 & firmtemp[,.N] > 0){
            print("Starting Distance Based Pairing...")
            nFirms <- firmtemp[,.N]
            nEst <- esttemp[,.N]
            print(paste0("Total Firms: ", nFirms," Total Est: ", nEst))
            if(j=="n6"){
              # Estimate the maximum number of splits
              maxsplit <- ceiling(firmtemp[,.(Nfirm=.N),by=.(naics)][esttemp[,.(Nest=.N),by=.(naics)],.(Nnaics=Nfirm*i.Nest),on="naics"][,sum(Nnaics,na.rm=TRUE)/5e8])
              if(maxsplit>0){
                suppressWarnings(esttemp[,splitn:=1:maxsplit])
                suppressWarnings(firmtemp[,splitn:=1:maxsplit])
                # Find the combination of splits
                splitcomb <- cbind(rep(1:maxsplit,each=maxsplit),rep(1:maxsplit,times=maxsplit))
                print(paste0("Maximum Split: ", maxsplit, " Combinations: ", nrow(splitcomb)))
                
                # Merge
                estfirmtemp <- rbindlist(apply(splitcomb,1,custommergesplit,esttemp,firmtemp[,.(naics,TAZ, CountyFIPS, FAF4, firmid,fsizecat,selected,n4,n2,splitn)],c("naics"),thresh))
                estfirmtemp <- estfirmtemp[!is.na(firmid)]
              } # else there might not exist any merge in this category
            } else if (j=="n4"){
              # Estimate the maximum number of splits
              maxsplit <- ceiling(firmtemp[,.(Nfirm=.N),by=.(n4)][esttemp[,.(Nest=.N),by=.(n4)],.(Nnaics=Nfirm*i.Nest),on="n4"][,sum(Nnaics,na.rm=TRUE)/5e8])
              if(maxsplit>0){
                suppressWarnings(esttemp[,splitn:=1:maxsplit])
                suppressWarnings(firmtemp[,splitn:=1:maxsplit])
                # Find the combination of splits
                splitcomb <- cbind(rep(1:maxsplit,each=maxsplit),rep(1:maxsplit,times=maxsplit))
                print(paste0("Maximum Split: ", maxsplit, " Combinations: ", nrow(splitcomb)))
                estfirmtemp <- rbindlist(apply(splitcomb,1,custommergesplit,esttemp,firmtemp[,.(naics,TAZ, CountyFIPS, FAF4, firmid,fsizecat,selected,n4,n2,splitn)],c("n4"),thresh))
                estfirmtemp <- estfirmtemp[!is.na(firmid)]
              } # else there might not exist any merge in this category
            } else {
              # Estimate the maximum number of splits
              maxsplit <- ceiling(firmtemp[,.(Nfirm=.N),by=.(n2)][esttemp[,.(Nest=.N),by=.(n2)],.(Nnaics=Nfirm*i.Nest),on="n2"][,sum(Nnaics,na.rm=TRUE)/5e8])
              if(maxsplit>0){
                suppressWarnings(esttemp[,splitn:=1:maxsplit])
                suppressWarnings(firmtemp[,splitn:=1:maxsplit])
                # Find the combination of splits
                splitcomb <- cbind(rep(1:maxsplit,each=maxsplit),rep(1:maxsplit,times=maxsplit))
                print(paste0("Maximum Split: ", maxsplit, " Combinations: ", nrow(splitcomb)))
                estfirmtemp <- rbindlist(apply(splitcomb,1,custommergesplit,esttemp,firmtemp[,.(naics,TAZ, CountyFIPS, FAF4, firmid,fsizecat,selected,n4,n2,splitn)],c("n2"),thresh))
                estfirmtemp <- estfirmtemp[!is.na(firmid)]
              } # else there might not exist any merge in this category
            } # end if j
            

            
            print(paste0("NAICS2: ", naics2))
            print(paste0("Group: ", i))
            # print(paste0("Total Firms: ",nuniquefirm," Total Est: ",nuniqueest))
            print(paste0("Distance Threshold: ", thresh))
            # print(paste0("Total Combinations: ",estfirmtemp[,.N]," Combinations passed: ",estfirmtemp[Distance<thresh,.N]))
            
            gc()
            # Find maximum matching using Djikstra's Algorithm
            if(j=="n6" & maxsplit>0){
              print(system.time(uniquePairs <- estfirmtemp[,{ConnectFirmsToEstablishments(nFirms = length(unique(firmid)), nEst = length(unique(estid)), Pairs = .SD[,.(estid,firmid,Distance)])},by=.(naics)]))#print(paste0("NAICS: ",unique(naics)));
              # print(paste0("Total Firms Remaining: ", max(0,nuniquefirm-length(uniquePairs$firmid))," Total Est Remaining: ", max(0,nuniqueest-length(uniquePairs$estid))))
              estfirmtemp <- cbind(estfirmtemp[,.(naics.firm=naics,naics.est=naics)],estfirmtemp[,setdiff(colnames(estfirmtemp),c("naics")),with=FALSE])
              
            } else if (j=="n4" & maxsplit>0){
              print(system.time(uniquePairs <- estfirmtemp[,{ConnectFirmsToEstablishments(nFirms = length(unique(firmid)), nEst = length(unique(estid)), Pairs = .SD[,.(estid,firmid,Distance)])},by=.(n4)])) #print(paste0("NAICS: ",unique(n4)));
              # print(paste0("Total Firms Remaining: ", max(0,nuniquefirm-length(uniquePairs$firmid))," Total Est Remaining: ", max(0,nuniqueest-length(uniquePairs$estid))))
              estfirmtemp <- cbind(estfirmtemp[,.(n4.firm=n4,n4.est=n4)],estfirmtemp[,setdiff(colnames(estfirmtemp),c("n4")),with=FALSE])
            } else if ( maxsplit>0) {
              print(system.time(uniquePairs <- estfirmtemp[,{ConnectFirmsToEstablishments(nFirms = length(unique(firmid)), nEst = length(unique(estid)), Pairs = .SD[,.(estid,firmid,Distance)])},by=.(n2)])) #print(paste0("NAICS: ",unique(n2)));
              # print(paste0("Total Firms Remaining: ", max(0,nuniquefirm-length(uniquePairs$firmid))," Total Est Remaining: ", max(0,nuniqueest-length(uniquePairs$estid))))
              estfirmtemp <- cbind(estfirmtemp[,.(n2.firm=n2,n2.est=n2)],estfirmtemp[,setdiff(colnames(estfirmtemp),c("n2")),with=FALSE])
            } #end if j==" " & maxsplit>0
            
            # Mark firms and establishments paired as selected/assigned
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
            } # end if maxsplit > 0
          } # end if esttemp[,.N] > 0 & firmtemp[,.N] > 0
        } #end for thresh
        print("Finished Pairing on Different TAZ..")
        difftaznaicslist[[match(j,c("n6","n4","n2"))]] <- rbindlist(threshlist,use.names = TRUE)
      } #end for j
    sametazlist <- rbindlist(sametaznaicslist,use.names = TRUE)
    difftazlist <- rbindlist(difftaznaicslist,use.names = TRUE)
    if(sametazlist[,.N]>0) {
      sametazlist[,Distance:=TAZDistances[.(TAZ.firm,TAZ.est),GCD]] 
    } else if (length(colnames(sametazlist))>0) {
      sametazlist[,Distance:=NA]
    } #end if sametazlist[,.N]>0
    
      sizelist[[i]] <- rbindlist(list(sametazlist,difftazlist),use.names=TRUE)
  } #end for i (size category)
  
  naicslist[[naics2]] <- rbindlist(sizelist,use.names = TRUE)
  # save(naicslist,firm,est,file = paste0("./firmByNaics/",naics2,".RData"))
} # end for naics2

rm(sizelist, sametazlist, difftazlist, sametaznaicslist, difftaznaicslist)

# load("./firmByNaics/allfirms.RData")
allfirms <- rbindlist(naicslist)
allfirms[,StateFIPS.est:=StateFIPS]
allfirms[,StateFIPS:=NULL]
allfirms[firm,StateFIPS.firm:=i.StateFIPS,on="firmid"]
### Assigning remaining firms
# Mark firms that whose establishment do not have the same 6 digit naics code
firm[firmid %in% allfirms[naics.est!=naics.firm,firmid],diffNaics:=1L]
firm[is.na(diffNaics),diffNaics:=0L]
# Mark firms whose establishments are more than 200 miles away
firm[firmid %in% allfirms[Distance>200,firmid],dist200:=1L]
firm[is.na(dist200),dist200:=0L]
# A dummy variable indicating if a new establishment is matched with the firms
firm[,selected2:=0L]

# While all the establishments are not assigned match them with firms with
# following priorities
# Firm Conditions
## 1. Distance greater than 200 miles and different naics w.r.t establishments
## 2. Different naics w.r.t establishments
## 3. Distance greater than 200 miles w.r.t establishments
## 4. Any remaining firm.
naicslist <- list()
gc()
#Loop over 2 digit naics code
for (naics2 in NAICS2){
  sizelist <- list()
  # Loop over employee size category
  for (i in employee_size_categories) {
    print(paste0("NAICS2: ",naics2, " Size: ",i))
    
    estgroup <- estgroups[i] 
    
    #what are the corresponding esizcats and fsizecats
    esizecats <- esize_fsize[EstablishmentSizeNum == estgroup]$EstablishmentSize
    fsizecats <- esize_fsize[EstablishmentSizeNum >= estgroup]$FirmSize
    
    samefaflist <- data.table()
    difffaflist <- data.table()
    
    #get the establishments and firms
    esttemp  <- est[n2 == naics2 & esizecat %in% esizecats & assigned == 0]
    
    # While there exist an establishment
    while(esttemp[,.N]>0){
      # Pair the firms with different naics (establishments) and distance between firm and establishment greater than 200 miles.
      firmtemp <- firm[n2 == naics2 & fsizecat %in% fsizecats & selected2==0 & dist200==1 & diffNaics==1]
      print(paste0("Establishments remaining: ", est[n2==naics2 & assigned==0,.N]))
      print("Running SAME FAF ZONES")
      print("Running: Diff NAICS and Dist > 200")
      #Merge
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
      # Merge
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
      # Merge
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
      # Find the maximum split
      maxsplit <- ceiling(firmtemp[,.(Nfirm=.N),by=.(naics)][esttemp[,.(Nest=.N),by=.(naics)],.(Nnaics=Nfirm*i.Nest),on="naics"][,sum(Nnaics,na.rm=TRUE)/5e8])
      
      if(maxsplit>0){
        suppressWarnings(esttemp[,splitn:=1:maxsplit])
        suppressWarnings(firmtemp[,splitn:=1:maxsplit])
        splitcomb <- cbind(rep(1:maxsplit,each=maxsplit),rep(1:maxsplit,times=maxsplit))
        print(paste0("Maximum Split: ", maxsplit, " Combinations: ", nrow(splitcomb)))
        # Merge
        estfirmtemp <- rbindlist(apply(splitcomb,1,custommergesplit,esttemp,firmtemp[,.(naics,TAZ, CountyFIPS, FAF4, firmid,fsizecat,selected=selected2,n4,n2,splitn)],c("naics"),Inf))
        estfirmtemp <- estfirmtemp[!is.na(firmid)]
        print(paste0("Total Firms: ",firmtemp[,.N]," Total Est: ",esttemp[,.N], " Total Combinatios: ", estfirmtemp[,.N]))
        if(estfirmtemp[,.N] > 0){
          estfirmtemp[allfirms[,AverageDistance:=mean(Distance),.(firmid)],weights:=1/i.AverageDistance,on=.(firmid)]
          print(system.time(uniquePairs <- estfirmtemp[,{ConnectFirmsToEstablishments(nFirms = length(unique(firmid)), nEst = length(unique(estid)), Pairs = .SD[,.(estid,firmid,Distance=Distance*weights)])},by=.(naics)]))
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
          print(system.time(uniquePairs <- estfirmtemp[,{ConnectFirmsToEstablishments(nFirms = length(unique(firmid)), nEst = length(unique(estid)), Pairs = .SD[,.(estid,firmid,Distance=Distance*weights)])},by=.(naics)]))
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
          print(system.time(uniquePairs <- estfirmtemp[,{ConnectFirmsToEstablishments(nFirms = length(unique(firmid)), nEst = length(unique(estid)), Pairs = .SD[,.(estid,firmid,Distance=Distance*weights)])},by=.(naics)]))
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
          print(system.time(uniquePairs <- estfirmtemp[,{ConnectFirmsToEstablishments(nFirms = length(unique(firmid)), nEst = length(unique(estid)), Pairs = .SD[,.(estid,firmid,Distance=Distance*weights)])},by=.(naics)]))
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
          print(system.time(uniquePairs <- estfirmtemp[,{ConnectFirmsToEstablishments(nFirms = length(unique(firmid)), nEst = length(unique(estid)), Pairs = .SD[,.(estid,firmid,Distance=Distance*weights*pmax(selected,1))])},by=.(naics)]))
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
            print(system.time(uniquePairs <- estfirmtemp[,{ConnectFirmsToEstablishments(nFirms = length(unique(firmid)), nEst = length(unique(estid)), Pairs = .SD[,.(estid,firmid,Distance=Distance*weights*pmax(selected,1))],maxmatch = FALSE)},by=.(naics)]))
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
  # save(naicslist,firm,est,file = paste0("./firmByNaics/",naics2,"_second.RData"))
}

allfirms2 <- rbindlist(naicslist)
allfirms2[,StateFIPS.est:=StateFIPS]
allfirms2[,StateFIPS:=NULL]
allfirms2[firm,StateFIPS.firm:=i.StateFIPS,on="firmid"]

allfirms2 <- rbindlist(list(allfirms,allfirms2),use.names = TRUE,fill = TRUE)
allfirms2[,c("selected","assigned","AverageDistance"):=NULL]


# Assign foreign firms
rm(naicslist)
gc()
# Since the information on foreign firms is limited we will just
# randomly match firms to the establishments.
estfirmforeign <- custommerge(estforeign, firmforeign ,bycode = c("naics","FAF4","TAZ","CountyFIPS","StateFIPS","n2","emp"),distthresh = Inf)
estfirmforeign[,":="(naics.firm=naics,naics.est=naics,
                     FAF4.firm=FAF4, FAF4.est=FAF4,
                     TAZ.firm=TAZ, TAZ.est = TAZ,
                     CountyFIPS.firm = CountyFIPS, CountyFIPS.est = CountyFIPS,
                     StateFIPS.firm = StateFIPS, StateFIPS.est = StateFIPS,
                     n2.firm=n2, n2.est=n2,
                     n4.firm=substr(naics,1,4),n4.est=substr(naics,1,4))]
estfirmforeign[,c("naics","FAF4","TAZ","CountyFIPS","StateFIPS","n2"):=NULL]

# Merge all the tables
allfirms3 <- rbindlist(list(allfirms2,estfirmforeign),use.names = TRUE, fill = TRUE)



# Save the final table
dir.create("outputs", showWarnings=FALSE)
fwrite(allfirms3, "outputs/FirmsandEstablishments.csv")
saveRDS(allfirms3, "outputs/FirmsandEstablishments.rds")





