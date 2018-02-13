## ----initialize, echo=FALSE----------------------------------------------

set.seed(1987)
suppressPackageStartupMessages(library(data.table))
suppressPackageStartupMessages(library(rFreight))


## ----open_vignette, eval=FALSE-------------------------------------------
#  
#  library(rFreight)
#  vignette("How to Use rFreight")
#  

## ----createapptemplate, eval=FALSE---------------------------------------
#  
#  # Create a fresh application template on your desktop
#  createAppTemplate(dir = "C:/Users/jeff.keller/Desktop")
#  

## ----codeexample_progresslog, eval=FALSE---------------------------------
#  
#  # Start logging progress
#  progressStart(action = "Simulating...", task = "Example")
#  
#  # ... 1st Calculation ...
#  
#  progressUpdate(prop = 1/3)
#  
#  # ... 2nd Calculation ...
#  
#  progressUpdate(prop = 2/3)
#  
#  # ... 3rd Calculation ...
#  
#  progressUpdate(prop = 3/3)
#  
#  # End logging progress
#  progressEnd()
#  

## ----example_logfile, eval=FALSE-----------------------------------------
#  
#  PUT EXAMPLE LOG FILE HERE
#  

## ----adjust_progress_pct-------------------------------------------------

TimeToComplete <- c(5, 25, 20) # seconds
NewPercentages <- cumsum(TimeToComplete/sum(TimeToComplete))
NewPercentages


## ----bad_varname, eval=FALSE---------------------------------------------
#  
#  x<-18
#  

## ----good_varname, eval=FALSE--------------------------------------------
#  
#  # Establish the age threshold of an adult
#  AdultAge<-18
#  

## ----name_errors, error=TRUE---------------------------------------------

MyVariable <- "Hello World!"
print(MYVariable) # returns an error


## ----literal_names-------------------------------------------------------

# Original name
DollarsPerTon <- 1:10

# Custom name
`Value per Ton ($)` <- 1:10

par(mfrow = c(1,2))
plot(DollarsPerTon)
plot(`Value per Ton ($)`)


## ----HIDDEN_cleanup, echo=FALSE------------------------------------------
par(mfrow = c(1,1))


## ----good_table_names, eval=FALSE----------------------------------------
#  
#  # Good Variable Names
#  Firms
#  Business
#  Establishments
#  

## ----bad_table_names, eval=FALSE-----------------------------------------
#  
#  # Poor Variable Names
#  Bus
#  Est
#  agents
#  

## ----datatable_print_example---------------------------------------------

# Create a data.table with 1000 rows
DT <- data.table(X = 1:1000,
                 Y = sample(LETTERS, size = 1000, replace = TRUE),
                 Z = rnorm(1000))
DT # same as print(DT)


## ----datatable_structure, eval=FALSE-------------------------------------
#  
#  DT[i, j, by]
#  

## ----datatable_subset1---------------------------------------------------

DT[X < 3]


## ----datatable_subset2---------------------------------------------------

myLetters <- c("R", "S", "G")
DT[X < 50 & Y %in% myLetters]


## ----datatable_select----------------------------------------------------

DT[, .(X, Y)]


## ----datatable_assignment------------------------------------------------

DT[, NewVariable := X * Z]
DT


## ----datatable_groupby1--------------------------------------------------

# Get the mean of Z for each Y
DT[, .(MeanZ = mean(Z)), by = Y]


## ----datatable_groupby2--------------------------------------------------

DT[, MeanZ := mean(Z), by = Y]
DT


