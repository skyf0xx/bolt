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

-- Register all message handlers
Handlers.add("Initialize",
  Handlers.utils.hasMatchingTag("Action", "Initialize"),
  function(msg) Handlers.handleRequest(msg) end)

Handlers.add("Reset",
  Handlers.utils.hasMatchingTag("Action", "Reset"),
  function(msg) Handlers.handleRequest(msg) end)

Handlers.add("Status",
  Handlers.utils.hasMatchingTag("Action", "Status"),
  function(msg) Handlers.handleRequest(msg) end)

Handlers.add("TokenList",
  Handlers.utils.hasMatchingTag("Action", "TokenList"),
  function(msg) Handlers.handleRequest(msg) end)

Handlers.add("PoolList",
  Handlers.utils.hasMatchingTag("Action", "PoolList"),
  function(msg) Handlers.handleRequest(msg) end)

Handlers.add("Quote",
  Handlers.utils.hasMatchingTag("Action", "Quote"),
  function(msg) Handlers.handleRequest(msg) end)

Handlers.add("FindPaths",
  Handlers.utils.hasMatchingTag("Action", "FindPaths"),
  function(msg) Handlers.handleRequest(msg) end)

Handlers.add("FindRoute",
  Handlers.utils.hasMatchingTag("Action", "FindRoute"),
  function(msg) Handlers.handleRequest(msg) end)

Handlers.add("CalculateOutput",
  Handlers.utils.hasMatchingTag("Action", "CalculateOutput"),
  function(msg) Handlers.handleRequest(msg) end)

Handlers.add("FindArbitrage",
  Handlers.utils.hasMatchingTag("Action", "FindArbitrage"),
  function(msg) Handlers.handleRequest(msg) end)

Handlers.add("RefreshReserves",
  Handlers.utils.hasMatchingTag("Action", "RefreshReserves"),
  function(msg) Handlers.handleRequest(msg) end)

Handlers.add("CollectData",
  Handlers.utils.hasMatchingTag("Action", "CollectData"),
  function(msg) Handlers.handleRequest(msg) end)

-- Initialize once on process start
local success = initialize()
if success then
  Logger.info("DEX Aggregator initialization complete and handlers registered")
else
  Logger.error("DEX Aggregator initialization failed")
end

-- Initialize handlers module with components
Handlers.init(components)
