# This script calls other scripts that performs opertions in following order:
# 1. Firm synthesis
# 2. Establishment synthesis
# 3. Firms and establishment matching

# Install the required packages
requiredPackages <- c("data.table", "rgdal", "igraph", "Matrix")
missingPackages <- !requiredPackages %in% rownames(installed.packages())
if(any(missingPackages)){
  install.packages(requiredPackages[missingPackages])
}

####################################################
### Firm Synthesis
####################################################
source("NFM_Firmsynthesis.R")

####################################################
### Establishment Synthesis
####################################################
source("NFM_EstablishmentSynthesis.R")

####################################################
### Firms and establishment matching
####################################################
DEBUG = TRUE #run only the first industry + all employee size categories
source("NFM_Firmsynthesis_Allocate_Est_Firms.R")
