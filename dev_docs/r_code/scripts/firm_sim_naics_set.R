firm_sim_naics_set <- function(producers,consumers,Firms,prefweights){
  #output summaries -- add this to the output functions to replace old ones
  sctgcat <- data.table(SCTG = prefweights$SCTG, SCTG_Name = prefweights$Commodity_SCTG_desc)
  #Sumamrize CBP, Producers and Consumers
  firms_sum <- list()
  
  #CBP
  firms_sum[["firms"]] <- nrow(Firms)
  firms_sum[["employment"]] <- sum(Firms$Emp)
  firms_sum[["firmsempbysctg"]] <- merge(sctgcat,Firms[,list(Establishments=.N,Employment=sum(Emp)),by=SCTG],"SCTG",all.y=T)
  
  #IO
  # firms_sum[["total_value"]] <- sum(io$ProVal)
  # firms_sum[["Industry_NAICS_Make"]] <- length(unique(io$Industry_NAICS6_Make))
  # firms_sum[["Industry_NAICS_Use"]] <- length(unique(io$Industry_NAICS6_Use))
  
  #producers
  firms_sum[["producers"]] <- nrow(producers)
  firms_sum[["producers_emp"]] <- sum(producers$Size)
  firms_sum[["producers_cap"]] <- sum(producers$OutputCapacityTons)
  producers_summary <- merge(sctgcat,producers[,list(Producers=.N,Employment=sum(Size),OutputCapacity=sum(OutputCapacityTons,na.rm=TRUE)),by=SCTG],"SCTG",all.y=T)
  firms_sum[["producersempbysctg"]] <- producers_summary
  # producers_domfor <- producers[,list(Producers=.N,Employment=sum(Size),OutputCapacity=sum(OutputCapacityTons)),by=Zone]
  # producers_domfor[,DomFor:=ifelse(Zone <= 273,"Domestic","Foreign")]
  # producers_domfor <- producers_domfor[,list(Producers=sum(Producers),Employment=sum(Employment),OutputCapacity=sum(OutputCapacity)),by=DomFor]
  # firms_sum[["producersdomfor"]] <- producers_domfor
  
  #consumers
  firms_sum[["consumers"]] <- length(unique(consumers$BuyerID))
  firms_sum[["consumption_pairs"]] <- nrow(consumers)
  firms_sum[["threshold"]] <- BASE_PROVALTHRESHOLD
  firms_sum[["consumer_inputs"]] <- sum(consumers$PurchaseAmountTons)
  consumers_summary <- merge(sctgcat,consumers[,list(Consumers=.N,InputRequirements=sum(PurchaseAmountTons,na.rm=TRUE)),by=SCTG],"SCTG",all.y=T)
  firms_sum[["consumersbysctg"]] <- consumers_summary
  # consumers_domfor <- consumers[,list(Consumers=.N,Employment=sum(Size),ConsumptionValue=sum(ConVal),InputRequirements=sum(PurchaseAmountTons)),by=Zone]
  # consumers_domfor[,DomFor:=ifelse(Zone <= 273,"Domestic","Foreign")]
  # consumers_domfor <- consumers_domfor[,list(Consumers=sum(Consumers),ConsumptionValue=sum(ConsumptionValue),InputRequirements=sum(InputRequirements)),by=DomFor]
  # firms_sum[["consumers_domfor"]] <- consumers_domfor
  
  #matching consumers and suppliers -- by SCTG category
  setnames(producers_summary,"SCTG","Commodity")
  setnames(consumers_summary,"SCTG","Commodity")
  match_summary <- merge(producers_summary[,list(Commodity,SCTG_Name,Producers,OutputCapacity)],
                         consumers_summary[,list(Commodity,Consumers,InputRequirements)],
                         "Commodity")
  setcolorder(match_summary,c("Commodity","SCTG_Name","Producers","Consumers","OutputCapacity","InputRequirements"))
  match_summary[,Ratio_OutputInput:=OutputCapacity/InputRequirements]
  match_summary[,Possible_Matches:=as.numeric(Producers) * as.numeric(Consumers)]
  firms_sum[["matches"]] <- match_summary
  
  #matching consumers and suppliers -- by NAICS codes
  producers_summary_naics <- producers[,list(Producers=.N,Employment=sum(Size),OutputCapacity=sum(OutputCapacityTons,na.rm=TRUE)),by=OutputCommodity]
  firms_sum[["producersempbynaics"]] <- data.frame(producers_summary_naics) #so it prints all rows
  consumers_summary_naics <- consumers[,list(Consumers=.N,Employment=sum(Size),InputRequirements=sum(PurchaseAmountTons)),by=InputCommodity]
  firms_sum[["consumersbynaics"]] <- data.frame(consumers_summary_naics) #so it prints all rows
  setnames(producers_summary_naics,"OutputCommodity","NAICS")
  setnames(consumers_summary_naics,"InputCommodity","NAICS")
  match_summary_naics <- merge(producers_summary_naics[,list(NAICS,Producers,OutputCapacity)],
                               consumers_summary_naics[,list(NAICS,Consumers,InputRequirements)],
                               "NAICS",all=TRUE)
  setcolorder(match_summary_naics,c("NAICS","Producers","Consumers","OutputCapacity","InputRequirements"))
  match_summary_naics[,Ratio_OutputInput:=OutputCapacity/InputRequirements]
  match_summary_naics[,Possible_Matches:=as.numeric(Producers) * as.numeric(Consumers)]
  firms_sum[["matches_naics"]] <- data.frame(match_summary_naics) #so it prints all rows
  
  #raw io data summary for comparison
  # io_sum_make <- io_sum[,list(ProVal=sum(ProVal)),by=Industry_NAICS6_Make]
  # firms_sum[["io_sum_make_naics"]] <- data.frame(io_sum_make)
  # io_sum_make <- merge(io_sum_make,c_n6_n6io_sctg[!duplicated(Industry_NAICS6_Make),list(Industry_NAICS6_Make,Commodity_SCTG)],by="Industry_NAICS6_Make",all.x=TRUE)
  # io_sum_make <- io_sum_make[Commodity_SCTG>0,list(ProVal=sum(ProVal)),by=Commodity_SCTG]
  # firms_sum[["io_sum_make_sctg"]] <- data.frame(io_sum_make[order(Commodity_SCTG)])
  
  #output
  capture.output(print(firms_sum),file=file.path(SCENARIO_OUTPUT_PATH,"firm_syn.txt" ))
  
  #Get number of (none NA) matches by NAICS, and check for imbalance in producers and consumers
  naics_set <- firms_sum$matches_naics[!is.na(firms_sum$matches_naics[,"Possible_Matches"]),c("NAICS","Producers","Consumers","Possible_Matches")]
  
  COMBINATIONTHRSHOLD <- 3500000
  CONSPRODRATIOLIMIT <- 1000000
  naics_set$ConsProd_Ratio <- naics_set$Consumers/naics_set$Producers
  naics_set$Split_Prod <- ifelse(naics_set$ConsProd_Ratio < CONSPRODRATIOLIMIT,TRUE,FALSE)
  
  #calculate group sizes for each commodity so that all groups are less than threshold
  #Either cut both consumers and producers or just consumers (with all producers in each group)
  
  
  naics_set[,c("nProducers","nConsumers","nMatches","rev_CPRatio","groups")] <- do.call(rbind,lapply(naics_set$NAICS,function(x) calcSampleGroups(naics_set$Consumers[naics_set$NAICS==x],naics_set$Producers[naics_set$NAICS==x],COMBINATIONTHRSHOLD,naics_set$Split_Prod[naics_set$NAICS==x],CONSPRODRATIOLIMIT)))
  
  naics_set <- data.table(naics_set)
  save(naics_set,file = file.path(SCENARIO_OUTPUT_PATH,"naics_set.Rdata"))
  
  # progressNextStep("Creating producer and consumers lists")
  
  #key the tables for faster subsetting on naics codes
  setkey(consumers,InputCommodity)
  setkey(producers,OutputCommodity)
  
  for (naics in naics_set$NAICS) {
    # Construct data.tables for just the current commodity
    consc <- consumers[naics,]
    prodc <- producers[naics,]
    #write the tables to an R data file
    save(consc,prodc, file = file.path(SCENARIO_OUTPUT_PATH,paste0(naics, ".Rdata")))
  }
  return(data.table(naics_set))
}


calcSampleGroups <- function(ncons,nprod,cthresh,sprod,cprl){
  ngroups <- 1L
  nconst <- as.numeric(ncons)
  nprodt <- as.numeric(nprod)
  nSuppliersPerBuyer <- 20L
  if(sprod){
    while (nconst * nSuppliersPerBuyer > cthresh) {
      ngroups <- ngroups + 1L
      nconst <- as.numeric(ceiling(as.numeric(ncons) / ngroups))
      nprodt <- as.numeric(ceiling(as.numeric(nprod) / ngroups))
    }
  } else {
    while ((nconst * nSuppliersPerBuyer > cthresh) | (nconst/nprodt > cprl)) {
      ngroups <- ngroups + 1L
      nconst <- as.numeric(ceiling(as.numeric(ncons) / ngroups))
    }
  }
  return(c(nprodt,nconst,nconst * nprodt,nconst/nprodt, ngroups))
}