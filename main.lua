-- DEX Aggregator - Main Entry Point
local Logger = require('dex.utils.logger')
Logger = Logger.createLogger("Main")
local DexHandlers = require('dex.dexhandlers')

-- This will store our application Components
Components = Components or {}


Handlers.add("Initialize",
  Handlers.utils.hasMatchingTag("Action", "Initialize"),
  DexHandlers.handleInitMessage)

Handlers.add("Reset",
  Handlers.utils.hasMatchingTag("Action", "Reset"),
  DexHandlers.handleResetMessage)

Handlers.add("Status",
  Handlers.utils.hasMatchingTag("Action", "Status"),
  DexHandlers.handleStatus)

Handlers.add("TokenList",
  Handlers.utils.hasMatchingTag("Action", "TokenList"),
  DexHandlers.handleTokenList)

Handlers.add("PoolList",
  Handlers.utils.hasMatchingTag("Action", "PoolList"),
  DexHandlers.handlePoolList)

Handlers.add("Quote",
  Handlers.utils.hasMatchingTag("Action", "Quote"),
  DexHandlers.handleQuote)

Handlers.add("FindPaths",
  Handlers.utils.hasMatchingTag("Action", "FindPaths"),
  DexHandlers.handleFindPaths)

Handlers.add("FindRoute",
  Handlers.utils.hasMatchingTag("Action", "FindRoute"),
  DexHandlers.handleFindRoute)

Handlers.add("CalculateOutput",
  Handlers.utils.hasMatchingTag("Action", "CalculateOutput"),
  DexHandlers.handleCalculateOutput)

Handlers.add("FindArbitrage",
  Handlers.utils.hasMatchingTag("Action", "FindArbitrage"),
  DexHandlers.handleFindArbitrage)

Handlers.add("RefreshReserves",
  Handlers.utils.hasMatchingTag("Action", "RefreshReserves"),
  DexHandlers.handleRefreshReserves)

Handlers.add("CollectData",
  Handlers.utils.hasMatchingTag("Action", "CollectData"),
  DexHandlers.handleCollectData)

Handlers.add("PollingCycle",
  Handlers.utils.hasMatchingTag("Action", "PollingCycle"),
  DexHandlers.handlePollingCycle)



-- Initialize handlers module with Components
DexHandlers.init(Components)
