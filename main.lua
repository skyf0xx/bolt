-- DEX Aggregator - Main Entry Point
local Constants = require('arbitrage.constants')
local Logger = require('arbitrage.logger').createLogger("Main")
local Handlers = require('arbitrage.handlers')
local Init = require('arbitrage.init')
local Schema = require('arbitrage.db.schema')

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

  -- Initial data collection and graph building
  Init.collectInitialData(components, function(success, result)
    if success then
      Logger.info("Initial data collection complete")

      -- Start background reserve refresh
      components.poller.startBackgroundRefresh()
    else
      Logger.error("Initial data collection failed", { error = result })
    end
  end)

  return true
end

-- Register all message handlers - now directly using the handler functions
Handlers.add("Initialize",
  Handlers.utils.hasMatchingTag("Action", "Initialize"),
  Handlers.handleInitMessage)

Handlers.add("Reset",
  Handlers.utils.hasMatchingTag("Action", "Reset"),
  Handlers.handleResetMessage)

Handlers.add("Status",
  Handlers.utils.hasMatchingTag("Action", "Status"),
  Handlers.handleStatus)

Handlers.add("TokenList",
  Handlers.utils.hasMatchingTag("Action", "TokenList"),
  Handlers.handleTokenList)

Handlers.add("PoolList",
  Handlers.utils.hasMatchingTag("Action", "PoolList"),
  Handlers.handlePoolList)

Handlers.add("Quote",
  Handlers.utils.hasMatchingTag("Action", "Quote"),
  Handlers.handleQuote)

Handlers.add("FindPaths",
  Handlers.utils.hasMatchingTag("Action", "FindPaths"),
  Handlers.handleFindPaths)

Handlers.add("FindRoute",
  Handlers.utils.hasMatchingTag("Action", "FindRoute"),
  Handlers.handleFindRoute)

Handlers.add("CalculateOutput",
  Handlers.utils.hasMatchingTag("Action", "CalculateOutput"),
  Handlers.handleCalculateOutput)

Handlers.add("FindArbitrage",
  Handlers.utils.hasMatchingTag("Action", "FindArbitrage"),
  Handlers.handleFindArbitrage)

Handlers.add("RefreshReserves",
  Handlers.utils.hasMatchingTag("Action", "RefreshReserves"),
  Handlers.handleRefreshReserves)

Handlers.add("CollectData",
  Handlers.utils.hasMatchingTag("Action", "CollectData"),
  Handlers.handleCollectData)

Handlers.add("PollingCycle",
  Handlers.utils.hasMatchingTag("Action", "PollingCycle"),
  Handlers.handlePollingCycle)

-- Initialize once on process start
local success = initialize()
if success then
  Logger.info("DEX Aggregator initialization complete and handlers registered")
else
  Logger.error("DEX Aggregator initialization failed")
end

-- Initialize handlers module with components
Handlers.init(components)
