
# Enumerate Firms
firm_sim_enumerate <- function(EstablishmentsIndustryTAZ, EmploymentCategories, c_naics_industry) {
  
  ### Enumerate firms and merge with correspondences
  
  # Aggregate the employment data by zones, NAICS, and firm size category
  # 1='1-19', 2='20-99', 3='100-249', 4='250-499', 5='500-999', 6='1,000-2,499', 7='2,500-4,999', 8='Over 5,000'
  # This may not do anything if the cbp data was properly formatted
  # Firms <- EstablishmentsIndustryTAZ[, .(e1 = sum(e1), e2 = sum(e2), e3 = sum(e3), e4 = sum(e4),
  #                                        e5 = sum(e5), e6 = sum(e6), e7 = sum(e7), e8 = sum(e8)),
  #                                    by = .(naics, TAZ1, CountyFIPS, StateFIPS, FAF4)]
  

  Firms <- EstablishmentsIndustryTAZ
  
  # Look up NAICS2007io classifications
  Firms[NAICS2007_to_NAICS2007io[, .(naics = NAICS, NAICSio)], NAICS6_Make := i.NAICSio, on = "naics"]
  
  # Derive 2, 3, and 4 digit NAICS codes
  Firms[, n2 := factor(substr(naics, 1, 2))]
  Firms[, n3 := factor(substr(naics, 1, 3))]
  Firms[, n4 := factor(substr(naics, 1, 4))]
  
  # Melt to create separate rows for each firm size category
  # Firms <- melt(Firms, measure.vars = paste0("e", 1:8), variable.name = "esizecat", value.name = "est")
  
  # Label esizecat
  Firms[, esizecat := factor(esizecat, levels = paste0("e", 1:12), ordered = TRUE, labels = EmploymentCategories$Label)]
  
  # Estimate the number of employees by looking up the range midpoints
  # Firms[, Emp := EmploymentCategories$Midpoint[as.integer(esizecat)]]
  
  # Add 10 level industry classification
  Firms[c_naics_industry[, .(n3 = NAICS3, Industry10=industry)], Industry10 := i.Industry10, on = "n3"]
  
  # Add 5 level industry classification
  Firms[, Industry5 := Industry10]
  Firms[Industry5 %in% c("Agriculture", "Construction", "Manufacturing"), Industry5 := "Industrial"]
  Firms[Industry5 %in% c("Government", "Health", "Hotel & Real Estate", "Other Services"), Industry5 := "Service"]
  Firms[, Industry5 := factor(as.character(Industry5))] # drops old levels and reorders
  
  # Enumerate the agent businesses using the est variable (number of establishments)
  # Firms <- Firms[rep(seq_len(Firms[, .N]), est)]
  
  # Create a business ID variable
  # Firms[, BusID := .I]
  
  # Set table structure
  Firms <- Firms[, .(BusID=estid, StateFIPS, FAF4, CountyFIPS, TAZ1=TAZ, naics, n4, n3, n2, NAICS6_Make, 
                     Industry10, Industry5, esizecat, Emp=emp)]
  setkey(Firms, BusID)
  
  # Return results
  return(Firms)
  
}
