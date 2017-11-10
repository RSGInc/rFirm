SYSTEM_PKGS <- c("bit", "bit64", "data.table", "fastcluster", "parallel", "pscl",
                  "rFreight", "TSP", "sp", "rgdal", "rgeos","reshape")

SYSTEM_REPORT_PKGS <- c("DT", "flexdashboard", "leaflet", "geojsonio", "htmltools", "htmlwidgets",
                        "knitr", "mapview", "plotly", "RColorBrewer", "rgdal", "rgeos",
                        "rmarkdown", "scales", "stringr", "jsonlite", "pander")

SYSTEM_APP_PATH       <- getwd()
SYSTEM_RFREIGHT_PATH  <- file.path(SYSTEM_APP_PATH, "lib", "rFreight_0.1-23.zip")
SYSTEM_PKGS_PATH      <- file.path(SYSTEM_APP_PATH, "lib", "pkgs", "library")
SYSTEM_DATA_PATH      <- file.path(SYSTEM_APP_PATH, "lib", "data")
SYSTEM_TEMPLATES_PATH <- file.path(SYSTEM_APP_PATH, "lib", "templates")
SYSTEM_SCRIPTS_PATH   <- file.path(SYSTEM_APP_PATH, "lib", "scripts")
SYSTEM_PMG_PATH       <- file.path(SYSTEM_APP_PATH, "lib", "PMG")
SYSTEM_FIRMSYN_OUTPUTNAME <- "1.Firms.RData"
SYSTEM_SCM_OUTPUTNAME     <- "2.AnnualShipments.RData"
SYSTEM_FTTM_OUTPUTNAME    <- "3.FreightTruckTrips.RData"
SYSTEM_CVTM_OUTPUTNAME    <- "4.CommercialVehicleTrips.RData"
SYSTEM_TT_OUTPUTNAME      <- "5.TripTables.RData"
