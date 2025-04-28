local Constants = require('arbitrage.constants')
local Logger = require('arbitrage.logger').createLogger("Init")
local Utils = require('arbitrage.utils')
local Schema = require('arbitrage.db.schema')
local TokenRepository = require('arbitrage.db.token_repository')
local PoolRepository = require('arbitrage.db.pool_repository')
local Collector = require('arbitrage.collectors.collector')
local Graph = require('arbitrage.graph.graph')
local Builder = require('arbitrage.graph.builder')
local PathFinder = require('arbitrage.graph.path_finder')
local Poller = require('arbitrage.reserve.poller')
local Calculator = require('arbitrage.swap.calculator')
local QuoteGenerator = require('arbitrage.swap.quote_generator')

local Init = {}

-- Setup database and schema
function Init.setupDatabase(dbPath)
  Logger.info("Setting up database", { path = dbPath })

  -- Initialize database
  local db, dbErr = Schema.init(dbPath)
  if not db then
    Logger.error("Database initialization failed", { error = dbErr })
    return nil, "Database initialization failed: " .. dbErr
  end

  -- Setup schema
  local success, schemaErr = Schema.setupSchema(db)
  if not success then
    Logger.error("Schema setup failed", { error = schemaErr })
    return nil, "Schema setup failed: " .. schemaErr
  end

  Logger.info("Database setup complete")
  return db
end

-- Load pool list from configuration
function Init.getConfiguredPools()
  -- In a real implementation, this might load from a config file
  -- For now, returning a placeholder list
  return {
    -- Permaswap pools
    { source = Constants.SOURCE.PERMASWAP, address = "uzJSyA3VTsEm1aHL2rmPH-Of5lA0vB5Flu9lCn2pn7k" },
    -- Botega pools
    { source = Constants.SOURCE.BOTEGA,    address = "Ov64swLY1J...BRU" }
  }
end

-- Setup components and collect initial data
function Init.setupComponents(db)
  -- Initialize the collector with database connection
  local collector = Collector.init(db)

  -- Initialize poller
  local poller = Poller.init(db)

  -- Initialize calculator
  local calculator = Calculator.init(db, poller)

  -- Initialize quote generator
  local quoteGenerator = QuoteGenerator.init(db, calculator)

  -- Create graph instance
  local graph = Graph.new()

  -- Initialize path finder with graph
  local pathFinder = PathFinder.init(graph, db)

  return {
    collector = collector,
    poller = poller,
    calculator = calculator,
    quoteGenerator = quoteGenerator,
    graph = graph,
    pathFinder = pathFinder
  }
end

-- Collect initial data and build graph
function Init.collectInitialData(components, callback)
  local collector = components.collector
  local graph = components.graph

  -- Get configured pools
  local pools = Init.getConfiguredPools()

  -- Collect data from all configured pools
  Logger.info("Collecting initial data", { poolCount = #pools })

  collector.collectAll(pools, function(results)
    if #results.pools == 0 then
      Logger.warn("No pools collected")
      callback(false, "No pools collected")
      return
    end

    -- Save collected data to database
    collector.saveToDatabase(results, function(success, err)
      if not success then
        Logger.error("Failed to save data", { error = err })
        callback(false, err)
        return
      end

      -- After data is saved, build the graph
      Init.buildGraph(components, callback)
    end)
  end)
end

-- Build graph from database
function Init.buildGraph(components, callback)
  local db = components.collector.db
  local graph = components.graph

  -- Get all pools with token info
  local pools = PoolRepository.getPoolsWithTokenInfo(db)

  -- Get all tokens
  local tokens = TokenRepository.getAllTokens(db)

  Logger.info("Building graph", { pools = #pools, tokens = #tokens })

  -- Build graph from pools and tokens
  local success = graph.buildFromPools(pools, tokens)

  if success then
    Logger.info("Graph built successfully")
    callback(true)
  else
    Logger.error("Failed to build graph")
    callback(false, "Failed to build graph")
  end
end

-- Main initialization function
function Init.initialize(dbPath, callback)
  Logger.info("Starting DEX Aggregator initialization")

  -- Setup database
  local db, dbErr = Init.setupDatabase(dbPath)
  if not db then
    callback(false, dbErr)
    return
  end

  -- Setup components
  local components = Init.setupComponents(db)

  -- Collect initial data and build graph
  Init.collectInitialData(components, function(success, err)
    if not success then
      callback(false, err)
      return
    end

    -- Start background reserve refresh
    components.poller.startBackgroundRefresh()

    Logger.info("DEX Aggregator initialization complete")
    callback(true, components)
  end)
end

-- Handle incoming initialization message
function Init.handleInitMessage(msg)
  if msg.Action ~= "Initialize" then
    return
  end

  local dbPath = msg.DbPath or Constants.DB.FILENAME

  Init.initialize(dbPath, function(success, result)
    if success then
      msg.reply({
        Status = "Success",
        Components = "Initialized",
        Graph = {
          Tokens = result.graph.tokenCount,
          Pools = result.graph.edgeCount,
          Sources = result.graph.sources
        }
      })
    else
      msg.reply({
        Status = "Error",
        Error = result
      })
    end
  end)
end

-- Reset database (for testing)
function Init.resetDatabase(db, callback)
  Logger.warn("Resetting database - all data will be lost")

  local success, err = Schema.resetDatabase(db)
  if not success then
    Logger.error("Database reset failed", { error = err })
    callback(false, err)
    return
  end

  Logger.info("Database reset complete")
  callback(true)
end

-- Handle reset message (for testing)
function Init.handleResetMessage(msg)
  if msg.Action ~= "Reset" then
    return
  end

  local dbPath = msg.DbPath or Constants.DB.FILENAME

  -- Initialize database
  local db, dbErr = Schema.init(dbPath)
  if not db then
    msg.reply({
      Status = "Error",
      Error = "Database initialization failed: " .. dbErr
    })
    return
  end

  Init.resetDatabase(db, function(success, err)
    if success then
      msg.reply({
        Status = "Success",
        Message = "Database reset complete"
      })
    else
      msg.reply({
        Status = "Error",
        Error = err
      })
    end
  end)
end

-- Register message handlers
function Init.registerHandlers()
  Init.handleInitMessage()
  Init.handleResetMessage()
end

-- Export initialization functions
return Init
