PMGParameters <- data.table(
  pmglogging = 1, #1: True, 0: FALSE
  RandomSeed = 41, # random starting seed for the PMGs
  IMax = 6, # number of iterations # Default was 6
  Verbose = 0, # want lots of detail about tradebots? # 0:FALSE	1:TRUE
  DynamicAlternatePayoffs = 1, # recalculate alternate payoffs every iteration based on updated expected payoffs
  ClairvoyantInitialExpectedPayoffs = 0, # should initial expected tradeoffs know size of other traders?
  SellersRankOffersByOrderSize = 0, # should sellers accept offers based on order size instead of expected payoff?
  InitExpPayoff = 1.1, # multiplier to goose initial expected tradeoff to encourage experimentation with other traders
  Temptation    = 0.6,
  BothCoop      = 1.0,
  BothDefect    = 0.6,
  Sucker        = 1.0,
  RefusalPayoff = 0.5, # amount to downgrade expected payoff of seller who outright refuses a trade offer by buyer
  WallflowerPayoff = 0.0, # negative payoff to sellers for not participating
  BuyersIgnoreSoldOutSellers = 1, #buyers don't try to trade with sold out sellers
  IgnoreSoldOutSellersMinBuyerSellerRatio = 100, #ratio at which buyers don't try to trade with sold out sellers
  RawFastParser = 1 #faster reading of input files but does less checks (ok for use with R)
)

PMGParameters <- melt.data.table(PMGParameters,measure.vars = colnames(PMGParameters))
setnames(PMGParameters,c("variable","value"),c("Variable","Value"))
saveRDS(PMGParameters,file = "./lib/data/PMGParameters.rds")





































