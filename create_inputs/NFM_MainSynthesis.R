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
source("./dev/NFM_Firmsynthesis.R")

####################################################
### Establishment Synthesis
####################################################
source("./dev/NFM_EstablishmentSynthesis.R")

####################################################
### Firms and establishment matching
####################################################
# This code takes ~ 5 days to run
source("./dev/NFM_Firmsynthesis_Allocate_Est_Firms.R")