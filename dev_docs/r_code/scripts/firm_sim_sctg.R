
# Simulate Production Commodities, choose a single commodity for firms making more than one
firm_sim_sctg <- function(Firms) {
  
  # Look up all the SCTGs each firm can produce
  # Note: not every firm produces a transportable SCTG commodity
  Firms.SCTG <- merge(Firms[, .(BusID, NAICS6_Make)], NAICS2007io_to_SCTG[, .(NAICS6_Make = NAICSio, SCTG, Proportion)],
                      by = "NAICS6_Make", all.x = TRUE, allow.cartesian = TRUE)
  
  # Some firms can potentially make more than one SCTG, simulate exactly one
  Firms.SCTG[!is.na(SCTG), SCTGSelected := .I == .I[sample(length(.I), size = 1, prob = Proportion)], by = BusID]
  # Firms.SCTG[!is.na(SCTG),SCTGSelected:= TRUE]
  Firms.SCTG <- Firms.SCTG[SCTGSelected == TRUE]
  Firms[Firms.SCTG, SCTG := i.SCTG, on = "BusID"]
  # Firms <- merge(Firms,Firms.SCTG[,.(BusID,SCTG)],by="BusID",all.x=TRUE)
  
  # Identify firms who make 2+ commodities (especially wholesalers) and
  # simulate a specific commodity for them based on probability thresholds for multiple commodities
  Firms[, temprand := runif(.N)]
  
  # TODO: are these still appropriate correspodences given the I/O data for the current project?
  Firms[naics == 211111, SCTG := c(16L, 19L)      [1 + findInterval(temprand,   0.45       )]] # Crude Petroleum and Natural Gas Extraction: Crude petroleum; Coal and petroleum products, n.e.c.
  Firms[naics == 324110, SCTG := c(17L, 18L, 19L) [1 + findInterval(temprand, c(0.25, 0.50))]] # Petroleum Refineries: Gasoline and aviation turbine fuel; Fuel oils; Coal and petroleum products, n.e.c.
  
  # Firms.4233 <- CJ(n4="4233",SCTG=c(10L, 11L, 12L, 25L, 26L))
  Firms[n4 == "4233", SCTG := c(10L, 11L, 12L, 25L, 26L)[1 + findInterval(temprand, c(0.10, 0.20, 0.80, 0.90))]] # Lumber and Other Construction Materials Merchant Wholesalers
  Firms[n4 == "4235", SCTG := c(13L, 14L, 31L, 32L)     [1 + findInterval(temprand, c(0.25, 0.50, 0.75      ))]] # Metal and Mineral (except Petroleum) Merchant Wholesalers
  Firms[n4 == "4247", SCTG := c(16L, 17L, 18L, 19L)     [1 + findInterval(temprand, c(0.25, 0.50, 0.75      ))]] # Petroleum and Petroleum Products Merchant Wholesalers
  Firms[n4 == "4246", SCTG := c(20L, 21L, 22L, 23L)     [1 + findInterval(temprand, c(0.25, 0.50, 0.75      ))]] # Chemical and Allied Products Merchant Wholesalers
  Firms[n4 == "4245", SCTG := c(1L,  2L,  3L,  4L)      [1 + findInterval(temprand, c(0.25, 0.50, 0.75      ))]] # Farm Product Raw Material Merchant Wholesalers
  Firms[n4 == "4244", SCTG := c(5L,  6L,  7L,  9L)      [1 + findInterval(temprand, c(0.25, 0.50, 0.75      ))]] # Grocery and Related Product Wholesalers
  Firms[n4 == "4241", SCTG := c(27L, 28L, 29L)          [1 + findInterval(temprand, c(0.33, 0.67            ))]] # Paper and Paper Product Merchant Wholesalers 
  Firms[n4 == "4237", SCTG := c(15L, 33L)               [1 + findInterval(temprand,   0.50                   )]] # Hardware, and Plumbing and Heating Equipment and Supplies Merchant Wholesalers
  Firms[n4 == "4251", SCTG := c(35L, 38L)               [1 + findInterval(temprand,   0.50                   )]] # Wholesale Electronic Markets and Agents and Brokers
  Firms[n4 == "4236", SCTG := c(35L, 38L)               [1 + findInterval(temprand,   0.50                   )]] # Electrical and Electronic Goods Merchant Wholesalers
  Firms[n4 == "4231", SCTG := c(36L, 37L)               [1 + findInterval(temprand,   0.50                   )]] # Motor Vehicle and Motor Vehicle Parts and Supplies Merchant Wholesalers
  
  Firms[n4 == "4248", SCTG :=   8L ] # Beer, Wine, and Distilled Alcoholic Beverage Merchant Wholesalers
  Firms[n4 == "4242", SCTG :=   21L] # Drugs and Druggists Sundries Merchant Wholesalers
  Firms[n4 == "4234", SCTG :=   24L] # Professional and Commercial Equipment and Supplies Merchant Wholesalers
  Firms[n4 == "4243", SCTG :=   30L] # Apparel, Piece Goods, and Notions Merchant Wholesalers
  Firms[n4 == "4238", SCTG :=   34L] # Machinery, Equipment, and Supplies Merchant Wholesalers
  Firms[n4 == "4232", SCTG :=   39L] # Furniture and Home Furnishing Merchant Wholesalers
  Firms[n4 == "4239", SCTG :=   40L] # Miscellaneous Durable Goods Merchant Wholesalers
  Firms[n4 == "4249", SCTG :=   40L] # Miscellaneous Nondurable Goods Merchant Wholesalers
  Firms[, temprand := NULL]
  Firms[n2 == "42", NAICS6_Make := paste0(n4, "00")]
  
  # Return results
  Firms <- Firms[, .(BusID, StateFIPS, CountyFIPS, FAF4, TAZ1, TAZ2,
                     SCTG, naics, n4, n3, n2, NAICS6_Make,
                     Industry10, Industry5, Model_EmpCat, esizecat, Emp)]
  setkey(Firms, BusID)
  return(Firms)
  
}
