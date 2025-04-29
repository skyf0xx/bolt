-- DEX Aggregator - Main Entry Point
local Constants = require('dex.utils.constants')
local Logger = require('dex.utils.logger')
Logger = Logger.createLogger("Main")
local DexHandlers = require('dex.dexhandlers')
local Init = require('dex.init')
local Schema = require('dex.db.schema')

-- This will store our application components
local components = {}

-- Initialize the system on startup
local function initialize()
  Logger.info("Starting DEX Aggregator v" .. Constants.VERSION)

  -- Setup database
  local db, dbErr = Schema.init(Constants.DB.FILENAME)
  if not db then
    Logger.error("Database initialization failed", { error = dbErr })
    return false
  end

  -- Setup schema
  local success, schemaErr = Schema.setupSchema(db)
  if not success then
    Logger.error("Schema setup failed", { error = schemaErr })
    return false
  end

  -- Initialize all components
  components = Init.setupComponents(db)

  return true
end

-- Register all message handlers - now directly using the handler functions
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

-- Initialize once on process start
local success = initialize()
if success then
  Logger.info("DEX Aggregator initialization complete and handlers registered")
else
  Logger.error("DEX Aggregator initialization failed")
end

-- Initialize handlers module with components
DexHandlers.init(components)
