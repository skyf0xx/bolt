local Constants = require('dex.utils.constants')
local Logger = require('dex.utils.logger')
Logger = Logger.createLogger("Init")
local Schema = require('dex.db.schema')
local TokenRepository = require('dex.db.token_repository')
local PoolRepository = require('dex.db.pool_repository')
local Collector = require('dex.collectors.collector')
local Graph = require('dex.graph.graph')
local PathFinder = require('dex.graph.path_finder')
local Poller = require('dex.reserve.poller')
local Calculator = require('dex.swap.calculator')
local QuoteGenerator = require('dex.swap.quote_generator')
local Utils = require('dex.utils.utils')

local Init = {}

-- Setup database and schema
function Init.setupDatabase()
  Logger.info("Setting up database")

  -- Initialize database (reusing existing connection if available)
  local db, dbErr = Schema.init()
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

-- Setup components and collect initial data
function Init.setupComponents(db, existingComponents)
  existingComponents = existingComponents or {}

  -- Reuse existing components or initialize new ones
  existingComponents.collector = existingComponents.collector or Collector.init(db)

  -- If poller exists, ensure it has the correct db reference
  if existingComponents.poller then
    existingComponents.poller.db = db
  else
    existingComponents.poller = Poller.init(db)
  end

  -- Initialize calculator with existing poller if available
  existingComponents.calculator = existingComponents.calculator or Calculator.init(db, existingComponents.poller)

  -- Initialize quote generator with existing calculator if available
  existingComponents.quoteGenerator = existingComponents.quoteGenerator or
      QuoteGenerator.init(db, existingComponents.collector)

  -- Reuse existing graph or create a new one
  existingComponents.graph = existingComponents.graph or Graph.new()

  -- Initialize path finder with existing graph if available
  existingComponents.pathFinder = existingComponents.pathFinder or PathFinder.init(existingComponents.graph, db)

  return existingComponents
end

-- Build graph from database
function Init.buildGraph(components, callback)
  components.graph:buildGraph(components, PoolRepository, TokenRepository, callback)
end

-- Main initialization function
function Init.initialize(callback)
  Logger.info("Starting DEX Aggregator initialization")

  -- Setup database (reusing existing connection if available)
  Db = Init.setupDatabase()
  if not Db then
    callback(false, "Database initialization failed")
    return
  end

  -- Setup components (preserving existing components)
  Components = Components or {}
  Components = Init.setupComponents(Db, Components)

  callback(true, { graph = Components.graph })
end

-- Handle incoming initialization message
function Init.handleInitMessage(msg)
  if msg.Action ~= "Initialize" then
    return
  end

  local forceReinit = msg.ForceReinit == true

  -- If we already have components and aren't forcing reinit, just report status
  if Components and Components.graph and Components.graph.initialized and not forceReinit then
    Logger.info("Using existing initialized components")
    msg.reply({
      Action = msg.Action .. "Response",
      Status = "Success",
      Components = "Reused",
      IsReused = 'true',
      Graph = Utils.jsonEncode({
        Tokens = Components.graph.tokenCount,
        Pools = Components.graph.edgeCount,
        Sources = Components.graph.sources
      })
    })
    return
  end

  Init.initialize(function(success, result)
    if success then
      msg.reply({
        Action = msg.Action .. "Response",
        Status = "Success",
        Components = "Initialized",
        IsReused = 'false',
        Graph = Utils.jsonEncode({
          Tokens = result.graph.tokenCount,
          Pools = result.graph.edgeCount,
          Sources = result.graph.sources
        })
      })
    else
      msg.reply({
        Action = msg.Action .. "Response",
        Status = "Error",
        Error = Utils.jsonEncode(result)
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


  -- Initialize database (use existing connection if available)
  local db = Schema.db or Schema.init()
  if not db then
    msg.reply({
      Action = msg.Action .. "Response",
      Status = "Error",
      Error = "Database initialization failed"
    })
    return
  end

  Init.resetDatabase(db, function(success, err)
    if success then
      -- Clear components to force reinitialization
      Db = db
      Components = nil

      msg.reply({
        Action = msg.Action .. "Response",
        Status = "Success",
        Message = "Database reset complete"
      })
    else
      msg.reply({
        Action = msg.Action .. "Response",
        Status = "Error",
        Error = Utils.jsonEncode(err)
      })
    end
  end)
end

-- Export initialization functions
return Init
