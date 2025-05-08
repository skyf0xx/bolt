Components = Components or {}
Db = Db or {}


-- DEX Aggregator - Main Entry Point
local Logger = require('dex.utils.logger')
Logger = Logger.createLogger("Main")
local DexHandlers = require('dex.dexhandlers')


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

Handlers.add("GetQuote",
  Handlers.utils.hasMatchingTag("Action", "GetQuote"),
  DexHandlers.handleGetQuote)

Handlers.add("FindPaths",
  Handlers.utils.hasMatchingTag("Action", "FindPaths"),
  DexHandlers.handleFindPaths)

Handlers.add("FindArbitrage",
  Handlers.utils.hasMatchingTag("Action", "FindArbitrage"),
  DexHandlers.handleFindArbitrage)

Handlers.add("RefreshReserves",
  Handlers.utils.hasMatchingTag("Action", "RefreshReserves"),
  DexHandlers.handleRefreshReserves)

Handlers.add("CollectData",
  Handlers.utils.hasMatchingTag("Action", "CollectData"),
  DexHandlers.handleCollectData)

Handlers.add("FlushCollectors",
  Handlers.utils.hasMatchingTag("Action", "FlushCollectors"),
  DexHandlers.handleFlushCollectors)

Handlers.add("UpdateTokenInfo",
  Handlers.utils.hasMatchingTag("Action", "UpdateTokenInfo"),
  DexHandlers.handleUpdateTokenInfo)

Handlers.add("BuildGraph",
  Handlers.utils.hasMatchingTag("Action", "BuildGraph"),
  DexHandlers.handleBuildGraph)

Handlers.add("PollingCycle",
  Handlers.utils.hasMatchingTag("Action", "PollingCycle"),
  DexHandlers.handlePollingCycle)
