
# Identify special firms: warehouses, producers, and makers subsample
firm_sim_types <- function(Firms) {
  
  # Warehouses
  # NAICS 481 air, 482 rail, 483 water, 493 warehouse and storage
  Firms[, Warehouse := 0L]
  Firms[TAZ1 %in% BASE_TAZ1_HALO_STATES & n3 %in% c("481", "482", "483", "493"), Warehouse := 1L]
  
  # Producers and Makers
  # Create a flag to make sure at least one firm is used as a maker for each zone and commodity combination
  Producers <- Firms[!is.na(SCTG)]
  Producers[Producers[,  .I[sample(x = .N, size = 1)], by = .(TAZ1, FAF4, NAICS6_Make, SCTG)]$V1, MustKeep := 1L]
  Producers[is.na(MustKeep), MustKeep := 0]
  
  # Create a sample of "makers" from the full producers table based on the following rules:
  # 1. Keep all individual businesses inside the region plus halo states
  # 2. Keep all large businesses throughout the US
  # 3. Keep those identified as "MustKeep" above
  Makers <- Producers[TAZ1 %in% BASE_TAZ1_HALO_STATES | as.integer(esizecat) >= 6 | MustKeep == 1]
  
  # Add maker and producer flags back to list of firms
  Firms[Producers, Producer := 1L, on = "BusID"]
  Firms[Makers, Maker := 1L, on = "BusID"]
  Firms[is.na(Producer), Producer := 0L]
  Firms[is.na(Maker), Maker := 0L]
  
  # Return results
  return(Firms[, .(BusID, StateFIPS, CountyFIPS, FAF4, TAZ1, TAZ2,
                   SCTG, naics, NAICS6_Make, Industry10, Industry5, Model_EmpCat,
                   esizecat, Emp, Warehouse, Producer, Maker)])
  
}
