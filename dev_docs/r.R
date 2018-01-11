
install.packages('data.table')

setwd("~/work/afreight/national_freight")
library("data.table")


NAICS2007_to_NAICS2007io    <- read.csv ("./data/NAICS2007_to_NAICS2007io.csv")
NAICS2007_to_NAICS2007io <- data.table(NAICS2007_to_NAICS2007io)


firms_establishments    <- read.csv ("./data/test_FirmsandEstablishments.csv")
firms_establishments <- data.table(firms_establishments)


Firms <- firms_establishments

# Look up NAICS2007io classifications
Firms[NAICS2007_to_NAICS2007io[, .(naics = NAICS, NAICSio)], NAICS6_Make := i.NAICSio, on = "naics"]




c_naics_empcat <- data.table(read.csv("./data/corresp_naics_empcat.csv"))

c_taz1_taz2 <- data.table(read.csv("./data/corresp_taz1_taz2.csv"))
