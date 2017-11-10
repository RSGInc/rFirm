
# Master function for executing the synthesis of firms.
firm_sim <- function(EstablishmentsIndustryTAZ) {
  
  # Begin progress tracking
  progressStart(action = "Simulating...", task = "Firms", dir = SCENARIO_LOG_PATH, subtasks = FALSE)
  
  # Enumerate firms
  Firms <- firm_sim_enumerate(EstablishmentsIndustryTAZ = EstablishmentsIndustryTAZ, EmploymentCategories = EmploymentCategories,
                              c_naics_industry = c_naics_industry)
  progressUpdate(prop = 0.2, dir = SCENARIO_LOG_PATH)
  gc()
  
  # Allocate firms from TAZ1 (large zones) to TAZ2 (small zones) for region with small zones
  Firms <- firm_sim_tazallocation(Firms = Firms, SocioEconomicsTAZ2 = SocioEconomicsTAZ2, 
                                  c_taz1_taz2 = c_taz1_taz2, c_naics_empcat = c_naics_empcat)
  progressUpdate(prop = 0.6, dir = SCENARIO_LOG_PATH)
  gc()

  # Scale firms to TAZ1 (large zones) to TAZ2 (small zones)for this scenario
  Firms <- firm_sim_scale_employees(Firms = Firms, EmploymentCategories = EmploymentCategories,
                                    c_naics_empcat = c_lehd_naics_empcat, c_taz1_taz2 = c_taz1_taz2, 
                                    c_taz1_fips = c_taz1_fips, c_taz1_faf4 = c_taz1_faf4, 
                                    SocioEconomicsTAZ2 = SocioEconomicsTAZ2,
                                    SocioEconomicsTAZ1 = NULL)
  ### Dropped some establishments where the employment found is 0.
  progressUpdate(prop = 0.7, dir = SCENARIO_LOG_PATH)
  gc()
  
  # Simulate production commodities
  Firms <- firm_sim_sctg(Firms = Firms)
  ### Missing SCTG for
  # Industry10       N
  # 1:             Agriculture   10480
  # 2:           Manufacturing   53041
  # 3:              Government  508914
  # 4:            Construction  657610
  # 5:                  Retail 1063090
  # 6: Transportation Handling  218142
  # 7:          Other Services 2337724
  # 8:     Hotel & Real Estate  425323
  # 9:                  Health  850010
  # 
  # 
  # Industry5       N
  # 1:              Industrial  721131
  # 2:                 Service 4121971
  # 3:                  Retail 1063090
  # 4: Transportation Handling  218142
  progressUpdate(prop = 0.8, dir = SCENARIO_LOG_PATH)
  gc()
  
  # Identify warehouses, producers, and maker sample
  Firms <- firm_sim_types(Firms = Firms)
  progressUpdate(prop = 0.9, dir = SCENARIO_LOG_PATH)
  gc()
  
  # Identify input commodities needed for each firm
  FirmInputOutputPairs <- firm_sim_iopairs(Firms = Firms, InputOutputValues = InputOutputValues)
  progressUpdate(prop = 0.92, dir = SCENARIO_LOG_PATH)
  gc()
  
  producers <- firm_sim_producers(Firms = Firms, InputOutputValues = InputOutputValues, unitcost = unitcost)
  progressUpdate(prop = 0.94, dir = SCENARIO_LOG_PATH)
  gc()
  
  consumers <- firm_sim_consumers(Firms = Firms, FirmInputOutputPairs = FirmInputOutputPairs, unitcost = unitcost, prefweights = prefweights)
  progressUpdate(prop = 0.96, dir = SCENARIO_LOG_PATH)
  gc()
  
  naics_set <- firm_sim_naics_set(producers = producers, consumers = consumers, Firms = Firms, prefweights = prefweights)
  progressUpdate(prop = 0.99, dir = SCENARIO_LOG_PATH)
  gc()
  
  # End progress tracking
  progressEnd(dir = SCENARIO_LOG_PATH)
  
  # Return results
  return(list(AllFirms = Firms, FirmInputOutputPairs = FirmInputOutputPairs, Producers = producers, Consumers = consumers, naics_set = naics_set))

}
