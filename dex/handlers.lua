local Constants = require('dex.utils.constants')
local Logger = require('dex.utils.logger')
Logger = Logger.createLogger("Handlers")
local Utils = require('dex.utils.utils')
local TokenRepository = require('dex.db.token_repository')
local PoolRepository = require('dex.db.pool_repository')
local Init = require('dex.init')

local Handlers = {}
local components = {}

-- Initialize handlers with required components
function Handlers.init(comps)
  components = comps
  Logger.info("Handlers initialized")
  return Handlers
end

-- Helper function to handle errors
function Handlers.handleError(msg, error, code)
  local errorCode = code or Constants.ERROR.UNKNOWN_ERROR
  Logger.error("Error in handler", { error = error, code = errorCode })

  msg.reply({
    Status = "Error",
    Error = error,
    Code = errorCode
  })
end

-- Handler for status request
function Handlers.handleStatus(msg)
  if not components.graph or not components.graph.initialized then
    Handlers.handleError(msg, "System not fully initialized", "ERR_NOT_INITIALIZED")
    return
  end

  local graphStats = components.graph.getStats()
  local pollerStats = components.poller.getCacheStats()
  local dbStats = {}

  if components.collector and components.collector.db then
    local tokenStats = TokenRepository.getTokenStatistics(components.collector.db)
    local poolStats = PoolRepository.getPoolStatistics(components.collector.db)

    dbStats = {
      tokens = tokenStats,
      pools = poolStats
    }
  end

  msg.reply({
    Status = "Success",
    Version = Constants.VERSION,
    AppName = Constants.APP_NAME,
    Graph = graphStats,
    Cache = pollerStats,
    Database = dbStats,
    Timestamp = os.time()
  })
end

-- Handler for token list request
function Handlers.handleTokenList(msg)
  if not components.collector or not components.collector.db then
    Handlers.handleError(msg, "Database not initialized", "ERR_DB_NOT_INITIALIZED")
    return
  end

  local query = msg.Query or ""

  local tokens = TokenRepository.searchTokens(components.collector.db, query)

  msg.reply({
    Status = "Success",
    Tokens = tokens,
    Count = #tokens,
    Query = query
  })
end

-- Handler for pool list request
function Handlers.handlePoolList(msg)
  if not components.collector or not components.collector.db then
    Handlers.handleError(msg, "Database not initialized", "ERR_DB_NOT_INITIALIZED")
    return
  end

  local source = msg.Source
  local pools

  if source then
    pools = PoolRepository.getPoolsBySource(components.collector.db, source)
  else
    pools = PoolRepository.getPoolsWithTokenInfo(components.collector.db)
  end

  msg.reply({
    Status = "Success",
    Pools = pools,
    Count = #pools,
    Source = source or "all"
  })
end

-- Handler for quote requests
function Handlers.handleQuote(msg)
  if not components.graph or not components.graph.initialized then
    Handlers.handleError(msg, "Graph not initialized", "ERR_GRAPH_NOT_INITIALIZED")
    return
  end

  -- Validate request
  if not msg.SourceToken or not msg.TargetToken or not msg.Amount then
    Handlers.handleError(msg, "Missing required parameters: SourceToken, TargetToken, Amount", "ERR_INVALID_PARAMS")
    return
  end

  local sourceTokenId = msg.SourceToken
  local targetTokenId = msg.TargetToken
  local amount = msg.Amount
  local options = {
    maxHops = msg.MaxHops or Constants.PATH.MAX_PATH_LENGTH,
    maxPaths = msg.MaxPaths or Constants.PATH.MAX_PATHS_TO_RETURN
  }

  Logger.info("Processing quote request", {
    source = sourceTokenId,
    target = targetTokenId,
    amount = amount
  })

  -- Find the best quote
  components.quoteGenerator.findBestQuote(
    sourceTokenId,
    targetTokenId,
    amount,
    components.pathFinder,
    options,
    function(quoteResults, err)
      if not quoteResults or not quoteResults.best_quote then
        Handlers.handleError(msg, err or "No viable path found", Constants.ERROR.PATH_NOT_FOUND)
        return
      end

      msg.reply({
        Status = "Success",
        Quote = quoteResults.best_quote,
        AlternativeQuotes = quoteResults.quotes,
        QuoteCount = quoteResults.quote_count
      })
    end
  )
end

-- Handler for finding paths
function Handlers.handleFindPaths(msg)
  if not components.graph or not components.graph.initialized then
    Handlers.handleError(msg, "Graph not initialized", "ERR_GRAPH_NOT_INITIALIZED")
    return
  end

  if not msg.SourceToken or not msg.TargetToken then
    Handlers.handleError(msg, "Missing required parameters: SourceToken, TargetToken", "ERR_INVALID_PARAMS")
    return
  end

  local sourceTokenId = msg.SourceToken
  local targetTokenId = msg.TargetToken
  local maxHops = msg.MaxHops or Constants.PATH.MAX_PATH_LENGTH

  local paths = components.pathFinder.findAllPaths(sourceTokenId, targetTokenId, maxHops)

  msg.reply({
    Status = "Success",
    Paths = paths,
    Count = #paths,
    SourceToken = sourceTokenId,
    TargetToken = targetTokenId,
    MaxHops = maxHops
  })
end

-- Handler for finding best route
function Handlers.handleFindRoute(msg)
  if not components.graph or not components.graph.initialized then
    Handlers.handleError(msg, "Graph not initialized", "ERR_GRAPH_NOT_INITIALIZED")
    return
  end

  if not msg.SourceToken or not msg.TargetToken or not msg.Amount then
    Handlers.handleError(msg, "Missing required parameters: SourceToken, TargetToken, Amount", "ERR_INVALID_PARAMS")
    return
  end

  local sourceTokenId = msg.SourceToken
  local targetTokenId = msg.TargetToken
  local amount = msg.Amount

  components.pathFinder.findBestRoute(sourceTokenId, targetTokenId, amount, function(result, err)
    if not result then
      Handlers.handleError(msg, err or "No viable route found", Constants.ERROR.PATH_NOT_FOUND)
      return
    end

    msg.reply({
      Status = "Success",
      Route = result,
      SourceToken = sourceTokenId,
      TargetToken = targetTokenId,
      InputAmount = amount,
      OutputAmount = result.outputAmount
    })
  end)
end

function Handlers.handlePollingCycle(msg)
  if not components.poller then
    Handlers.handleError(msg, "Poller not initialized", "ERR_POLLER_NOT_INITIALIZED")
    return
  end

  components.poller.executePollingCycle(msg)
end

-- Handler for calculating swap output
function Handlers.handleCalculateOutput(msg)
  if not components.calculator then
    Handlers.handleError(msg, "Calculator not initialized", "ERR_CALCULATOR_NOT_INITIALIZED")
    return
  end

  if not msg.PoolId or not msg.TokenIn or not msg.Amount then
    Handlers.handleError(msg, "Missing required parameters: PoolId, TokenIn, Amount", "ERR_INVALID_PARAMS")
    return
  end

  local poolId = msg.PoolId
  local tokenIn = msg.TokenIn
  local amount = msg.Amount

  components.calculator.calculateSwapOutput(poolId, tokenIn, amount, function(result, err)
    if not result then
      Handlers.handleError(msg, err or "Calculation failed", Constants.ERROR.CALCULATION_FAILED)
      return
    end

    msg.reply({
      Status = "Success",
      Result = result
    })
  end)
end

-- Handler for finding arbitrage opportunities
function Handlers.handleFindArbitrage(msg)
  if not components.pathFinder then
    Handlers.handleError(msg, "PathFinder not initialized", "ERR_PATHFINDER_NOT_INITIALIZED")
    return
  end

  local startTokenId = msg.StartToken or Constants.PATH.DEFAULT_ARBITRAGE_TOKEN
  local inputAmount = msg.Amount or Constants.NUMERIC.DEFAULT_SWAP_INPUT

  components.pathFinder.findArbitrageOpportunities(startTokenId, inputAmount, function(result, err)
    if err then
      msg.reply({
        Status = "Partial",
        Error = err,
        Opportunities = result.opportunities or {}
      })
      return
    end

    msg.reply({
      Status = "Success",
      Opportunities = result.opportunities,
      Count = #(result.opportunities),
      StartToken = startTokenId,
      InputAmount = inputAmount
    })
  end)
end

-- Handler for refreshing reserves
function Handlers.handleRefreshReserves(msg)
  if not components.poller then
    Handlers.handleError(msg, "Poller not initialized", "ERR_POLLER_NOT_INITIALIZED")
    return
  end

  local poolIds = msg.PoolIds
  local forceFresh = msg.ForceFresh or false

  if poolIds and #poolIds > 0 then
    -- Refresh specific pools
    components.poller.pollMultiplePools(poolIds, forceFresh, function(results)
      msg.reply({
        Status = "Success",
        Refreshed = Utils.tableSize(results.reserves),
        Failed = Utils.tableSize(results.errors),
        Errors = results.errors
      })
    end)
  else
    -- Refresh stale reserves
    local maxAge = msg.MaxAge or Constants.TIME.RESERVE_CACHE_EXPIRY
    local batchSize = msg.BatchSize or Constants.OPTIMIZATION.BATCH_SIZE

    components.poller.refreshStaleReserves(maxAge, batchSize, function(result)
      msg.reply({
        Status = "Success",
        Refreshed = result.refreshed,
        Failed = result.failed,
        Errors = result.results and result.results.errors or {}
      })
    end)
  end
end

-- Handler for data collection
function Handlers.handleCollectData(msg)
  if not components.collector then
    Handlers.handleError(msg, "Collector not initialized", "ERR_COLLECTOR_NOT_INITIALIZED")
    return
  end

  local source = msg.Source
  local poolAddresses = msg.PoolAddresses

  if not source or not poolAddresses or #poolAddresses == 0 then
    Handlers.handleError(msg, "Missing required parameters: Source, PoolAddresses", "ERR_INVALID_PARAMS")
    return
  end

  components.collector.collectFromDex(source, poolAddresses, function(results)
    -- Save to database if requested
    if msg.SaveToDb and components.collector.db then
      components.collector.saveToDatabase(results, function(success, err)
        if not success then
          msg.reply({
            Status = "Partial",
            Error = "Data collected but save failed: " .. err,
            Pools = #results.pools,
            Tokens = #results.tokens,
            Reserves = Utils.tableSize(results.reserves),
            Errors = results.errors
          })
          return
        end

        -- Rebuild graph if requested
        if msg.RebuildGraph and components.graph then
          Init.buildGraph(components, function(success, err)
            if not success then
              msg.reply({
                Status = "Partial",
                Error = "Data saved but graph rebuild failed: " .. err,
                Pools = #results.pools,
                Tokens = #results.tokens,
                Reserves = Utils.tableSize(results.reserves)
              })
              return
            end

            msg.reply({
              Status = "Success",
              Pools = #results.pools,
              Tokens = #results.tokens,
              Reserves = Utils.tableSize(results.reserves),
              GraphRebuilt = true
            })
          end)
        else
          msg.reply({
            Status = "Success",
            Pools = #results.pools,
            Tokens = #results.tokens,
            Reserves = Utils.tableSize(results.reserves),
            Saved = true
          })
        end
      end)
    else
      msg.reply({
        Status = "Success",
        Pools = #results.pools,
        Tokens = #results.tokens,
        Reserves = Utils.tableSize(results.reserves),
        Errors = results.errors
      })
    end
  end)
end

-- Note: we're removing the handleRequest method that had the switch-case
-- and just exporting the individual handler functions

-- These are the initialization handlers
Handlers.handleInitMessage = Init.handleInitMessage
Handlers.handleResetMessage = Init.handleResetMessage

return Handlers
