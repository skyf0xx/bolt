local Constants = require('dex.utils.constants')
local Logger = require('dex.utils.logger')
Logger = Logger.createLogger("DexHandlers")
local Utils = require('dex.utils.utils')
local TokenRepository = require('dex.db.token_repository')
local PoolRepository = require('dex.db.pool_repository')
local Init = require('dex.init')

local DexHandlers = {}



-- Helper function to handle errors
function DexHandlers.handleError(msg, error, code)
  local errorCode = code or Constants.ERROR.UNKNOWN_ERROR
  Logger.error("Error in handler", { error = error, code = errorCode })

  msg.reply({
    Action = msg.Action .. "Response",
    Status = "Error",
    Error = tostring(error),
    Code = tostring(errorCode)
  })
end

-- Handler for updating token information from blockchain
function DexHandlers.handleUpdateTokenInfo(msg)
  if not Components.collector or not Components.collector.db then
    DexHandlers.handleError(msg, "Database not initialized", "ERR_DB_NOT_INITIALIZED")
    return
  end

  local SqlAccessor = require('dex.utils.sql_accessor')
  local Utils = require('dex.utils.utils')
  local TokenRepository = require('dex.db.token_repository')
  local Logger = require('dex.utils.logger')
  Logger = Logger.createLogger("TokenUpdater")

  local batchSize = msg.BatchSize or 10
  local forceUpdate = msg.ForceUpdate or false

  Logger.info("Starting token information update", { batchSize = batchSize, forceUpdate = forceUpdate })

  -- Get all tokens from database
  local tokens = TokenRepository.getAllTokens(Components.collector.db)

  if #tokens == 0 then
    msg.reply({
      Action = msg.Action .. "Response",
      Status = "Success",
      Message = "No tokens found to update",
      Count = "0"
    })
    return
  end

  -- Track progress
  local pendingTokens = math.min(batchSize, #tokens)
  local totalTokens = #tokens
  local processedTokens = 0
  local updatedTokens = 0
  local failedTokens = 0
  local errors = {}

  Logger.info("Updating token information", { count = pendingTokens, total = totalTokens })

  -- Function to update a token safely
  local function updateTokenSafely(db, token, newInfo)
    local currentTime = os.time()

    -- Prepare new token data
    local updatedToken = {
      id = token.id,
      symbol = newInfo.Ticker or token.symbol,
      name = newInfo.Name or token.name,
      decimals = tonumber(newInfo.Denomination) or token.decimals,
      logo_url = newInfo.Logo or token.logo_url
    }

    -- Check if there are actual changes
    local hasChanges =
        updatedToken.symbol ~= token.symbol or
        updatedToken.name ~= token.name or
        updatedToken.decimals ~= token.decimals or
        updatedToken.logo_url ~= token.logo_url

    if not hasChanges and not forceUpdate then
      return true, "No changes required"
    end

    -- Directly use SqlAccessor to execute the update
    local sql = [[
      UPDATE tokens
      SET symbol = ?, name = ?, decimals = ?, logo_url = ?, updated_at = ?
      WHERE id = ?
    ]]

    local params = {
      updatedToken.symbol,
      updatedToken.name,
      updatedToken.decimals,
      updatedToken.logo_url,
      currentTime,
      token.id
    }

    return SqlAccessor.execute(db, sql, params)
  end

  -- Process tokens in the current batch
  for i = 1, pendingTokens do
    local token = tokens[i]

    -- Call the blockchain to get token info
    ao.send({
      Target = token.id,
      Action = "Info"
    }).onReply(function(response)
      processedTokens = processedTokens + 1

      if response.Error then
        failedTokens = failedTokens + 1
        errors[token.id] = response.Error
        Logger.warn("Failed to get token info", { id = token.id, error = response.Error })
      else
        -- Update token with received information
        local success, err = updateTokenSafely(Components.collector.db, token, response)

        if success then
          updatedTokens = updatedTokens + 1
          Logger.info("Updated token info", {
            id = token.id,
            symbol = response.Ticker,
            name = response.Name
          })
        else
          failedTokens = failedTokens + 1
          errors[token.id] = err
          Logger.error("Failed to update token info", { id = token.id, error = err })
        end
      end

      -- If all tokens in this batch are processed, send response
      if processedTokens >= pendingTokens then
        msg.reply({
          Action = msg.Action .. "Response",
          Status = "Success",
          Processed = tostring(processedTokens),
          Total = tostring(totalTokens),
          Updated = tostring(updatedTokens),
          Failed = tostring(failedTokens),
          Errors = Utils.jsonEncode(errors),
          RemainingTokens = tostring(totalTokens - processedTokens)
        })
      end
    end)
  end
end

-- Handler for status request
function DexHandlers.handleStatus(msg)
  if not Components.graph or not Components.graph.initialized then
    DexHandlers.handleError(msg, "System not fully initialized", "ERR_NOT_INITIALIZED")
    return
  end

  local graphStats = Components.graph:getStats()
  local pollerStats = Components.poller.getCacheStats()
  local dbStats = {}

  if Components.collector and Components.collector.db then
    local tokenStats = TokenRepository.getTokenStatistics(Components.collector.db)
    local poolStats = PoolRepository.getPoolStatistics(Components.collector.db)

    dbStats = {
      tokens = tokenStats,
      pools = poolStats
    }
  end

  msg.reply({
    Action = msg.Action .. "Response",
    Status = "Success",
    Version = Constants.VERSION,
    AppName = Constants.APP_NAME,
    Graph = Utils.jsonEncode(graphStats),
    Cache = Utils.jsonEncode(pollerStats),
    Database = Utils.jsonEncode(dbStats),
    Timestamp = tostring(os.time())
  })
end

-- Handler for token list request
function DexHandlers.handleTokenList(msg)
  if not Components.collector or not Components.collector.db then
    DexHandlers.handleError(msg, "Database not initialized", "ERR_DB_NOT_INITIALIZED")
    return
  end

  local query = msg.Query or ""

  local tokens = TokenRepository.searchTokens(Components.collector.db, query)

  msg.reply({
    Action = msg.Action .. "Response",
    Status = "Success",
    Tokens = Utils.jsonEncode(tokens),
    Count = tostring(#tokens),
    Query = query
  })
end

-- Handler for pool list request
function DexHandlers.handlePoolList(msg)
  if not Components.collector or not Components.collector.db then
    DexHandlers.handleError(msg, "Database not initialized", "ERR_DB_NOT_INITIALIZED")
    return
  end

  local source = msg.Source
  local pools

  if source then
    pools = PoolRepository.getPoolsBySource(Components.collector.db, source)
  else
    pools = PoolRepository.getPoolsWithTokenInfo(Components.collector.db)
  end

  msg.reply({
    Action = msg.Action .. "Response",
    Status = "Success",
    Pools = Utils.jsonEncode(pools),
    Count = tostring(#pools),
    Source = source or "all"
  })
end

-- Handler for quote requests
function DexHandlers.handleGetQuote(msg)
  if not Components.graph or not Components.graph.initialized then
    DexHandlers.handleError(msg, "Graph not initialized", "ERR_GRAPH_NOT_INITIALIZED")
    return
  end

  -- Validate request
  if not msg.SourceToken or not msg.TargetToken or not msg.AmountIn then
    DexHandlers.handleError(msg, "Missing required parameters: SourceToken, TargetToken, Amount", "ERR_INVALID_PARAMS")
    return
  end

  local sourceTokenId = msg.SourceToken
  local targetTokenId = msg.TargetToken
  local amountIn = msg.AmountIn
  local options = {
    maxHops = msg.MaxHops or Constants.PATH.MAX_PATH_LENGTH,
    maxPaths = msg.MaxPaths or Constants.PATH.MAX_PATHS_TO_RETURN
  }

  Logger.info("Processing quote request", {
    source = sourceTokenId,
    target = targetTokenId,
    amountIn = amountIn
  })

  -- Find the best quote
  Components.quoteGenerator.findBestQuote(
    sourceTokenId,
    targetTokenId,
    amountIn,
    Components.pathFinder,
    options,
    function(quoteResults, err)
      if not quoteResults or not quoteResults.best_quote then
        DexHandlers.handleError(msg, err or "No viable path found", Constants.ERROR.PATH_NOT_FOUND)
        return
      end

      msg.reply({
        Action = msg.Action .. "Response",
        Status = "Success",
        Quote = tostring(quoteResults.best_quote),
        AlternativeQuotes = Utils.jsonEncode(quoteResults.quotes),
        QuoteCount = tostring(quoteResults.quote_count)
      })
    end
  )
end

-- Handler for finding paths
function DexHandlers.handleFindPaths(msg)
  if not Components.graph or not Components.graph.initialized then
    DexHandlers.handleError(msg, "Graph not initialized", "ERR_GRAPH_NOT_INITIALIZED")
    return
  end

  if not msg.SourceToken or not msg.TargetToken then
    DexHandlers.handleError(msg, "Missing required parameters: SourceToken, TargetToken", "ERR_INVALID_PARAMS")
    return
  end

  local sourceTokenId = msg.SourceToken
  local targetTokenId = msg.TargetToken
  local maxHops = msg.MaxHops or Constants.PATH.MAX_PATH_LENGTH

  local paths = Components.pathFinder.findAllPaths(sourceTokenId, targetTokenId, maxHops)

  msg.reply({
    Action = msg.Action .. "Response",
    Status = "Success",
    Paths = Utils.jsonEncode(paths),
    Count = tostring(#paths),
    SourceToken = sourceTokenId,
    TargetToken = targetTokenId,
    MaxHops = tostring(maxHops)
  })
end

-- Handler for finding best route
function DexHandlers.handleFindRoute(msg)
  if not Components.graph or not Components.graph.initialized then
    DexHandlers.handleError(msg, "Graph not initialized", "ERR_GRAPH_NOT_INITIALIZED")
    return
  end

  if not msg.SourceToken or not msg.TargetToken or not msg.AmountIn then
    DexHandlers.handleError(msg, "Missing required parameters: SourceToken, TargetToken, Amount", "ERR_INVALID_PARAMS")
    return
  end

  local sourceTokenId = msg.SourceToken
  local targetTokenId = msg.TargetToken
  local amountIn = msg.AmountIn

  Components.pathFinder.findBestRoute(sourceTokenId, targetTokenId, amountIn, function(result, err)
    if not result then
      DexHandlers.handleError(msg, err or "No viable route found", Constants.ERROR.PATH_NOT_FOUND)
      return
    end

    msg.reply({
      Action = msg.Action .. "Response",
      Status = "Success",
      Route = Utils.jsonEncode(result),
      SourceToken = sourceTokenId,
      TargetToken = targetTokenId,
      InputAmount = tostring(amountIn),
      OutputAmount = tostring(result.outputAmount)
    })
  end)
end

function DexHandlers.handlePollingCycle(msg)
  if not Components.poller then
    DexHandlers.handleError(msg, "Poller not initialized", "ERR_POLLER_NOT_INITIALIZED")
    return
  end

  Components.poller.executePollingCycle(msg)
end

-- Handler for calculating swap output
function DexHandlers.handleCalculateOutput(msg)
  if not Components.calculator then
    DexHandlers.handleError(msg, "Calculator not initialized", "ERR_CALCULATOR_NOT_INITIALIZED")
    return
  end

  if not msg.PoolId or not msg.TokenIn or not msg.AmountIn then
    DexHandlers.handleError(msg, "Missing required parameters: PoolId, TokenIn, Amount", "ERR_INVALID_PARAMS")
    return
  end

  local poolId = msg.PoolId
  local tokenIn = msg.TokenIn
  local amountIn = msg.AmountIn

  Components.calculator.calculateSwapOutput(poolId, tokenIn, amountIn, function(result, err)
    if not result then
      DexHandlers.handleError(msg, err or "Calculation failed", Constants.ERROR.CALCULATION_FAILED)
      return
    end

    msg.reply({
      Action = msg.Action .. "Response",
      Status = "Success",
      Result = Utils.jsonEncode(result)
    })
  end)
end

-- Handler for finding arbitrage opportunities
function DexHandlers.handleFindArbitrage(msg)
  if not Components.pathFinder then
    DexHandlers.handleError(msg, "PathFinder not initialized", "ERR_PATHFINDER_NOT_INITIALIZED")
    return
  end

  local startTokenId = msg.StartToken or Constants.PATH.DEFAULT_ARBITRAGE_TOKEN
  local inputAmount = msg.Amount or Constants.NUMERIC.DEFAULT_SWAP_INPUT

  Components.pathFinder.findArbitrageOpportunities(startTokenId, inputAmount, function(result, err)
    if err then
      msg.reply({
        Action = msg.Action .. "Response",
        Status = "Partial",
        Error = tostring(err),
        Opportunities = Utils.jsonEncode(result.opportunities or {})
      })
      return
    end

    msg.reply({
      Action = msg.Action .. "Response",
      Status = "Success",
      Opportunities = Utils.jsonEncode(result.opportunities),
      Count = tostring(#(result.opportunities)),
      StartToken = startTokenId,
      InputAmount = inputAmount
    })
  end)
end

-- Handler for refreshing reserves
function DexHandlers.handleRefreshReserves(msg)
  if not Components.poller then
    DexHandlers.handleError(msg, "Poller not initialized", "ERR_POLLER_NOT_INITIALIZED")
    return
  end

  local poolIds = msg.PoolIds
  local forceFresh = msg.ForceFresh or false

  if poolIds and #poolIds > 0 then
    -- Refresh specific pools
    Components.poller.pollMultiplePools(poolIds, forceFresh, function(results)
      msg.reply({
        Action = msg.Action .. "Response",
        Status = "Success",
        Refreshed = tostring(Utils.tableSize(results.reserves)),
        Failed = tostring(Utils.tableSize(results.errors)),
        Errors = Utils.jsonEncode(results.errors)
      })
    end)
  else
    -- Refresh stale reserves
    local maxAge = msg.MaxAge or Constants.TIME.RESERVE_CACHE_EXPIRY
    local batchSize = msg.BatchSize or Constants.OPTIMIZATION.BATCH_SIZE

    Components.poller.refreshStaleReserves(maxAge, batchSize, function(result)
      msg.reply({
        Action = msg.Action .. "Response",
        Status = "Success",
        Refreshed = tostring(result.refreshed),
        Failed = tostring(result.failed),
        Errors = Utils.jsonEncode(result.results and result.results.errors or {})
      })
    end)
  end
end

-- Handler for data collection
function DexHandlers.handleCollectData(msg)
  if not Components.collector then
    DexHandlers.handleError(msg, "Collector not initialized", "ERR_COLLECTOR_NOT_INITIALIZED")
    return
  end

  local source = msg.Source
  local poolAddresses = Utils.jsonDecode(msg.PoolAddresses)

  if not source or not poolAddresses or #poolAddresses == 0 then
    DexHandlers.handleError(msg, "Missing required parameters: Source, PoolAddresses", "ERR_INVALID_PARAMS")
    return
  end

  Components.collector.collectFromDex(source, poolAddresses, function(results)
    -- Save to database if requested
    if msg.SaveToDb and Components.collector.db then
      Components.collector.saveToDatabase(results, function(success, err)
        if not success then
          msg.reply({
            Action = msg.Action .. "Response",
            Status = "Partial",
            Error = "Data collected but save failed: " .. tostring(err),
            Pools = tostring(#results.pools),
            Tokens = tostring(#results.tokens),
            Reserves = tostring(Utils.tableSize(results.reserves)),
            Errors = Utils.jsonEncode(results.errors)
          })
          return
        end

        Logger.info("Saved to database")

        -- Rebuild graph if requested
        if msg.RebuildGraph and Components.graph then
          Logger.info("Rebuilding graph")

          -- This is the key fix: use the proper function to rebuild the graph
          Init.buildGraph(Components, function(success, err)
            if not success then
              msg.reply({
                Action = msg.Action .. "Response",
                Status = "Partial",
                Error = "Data saved but graph rebuild failed: " .. tostring(err),
                Pools = tostring(#results.pools),
                Tokens = tostring(#results.tokens),
                Reserves = tostring(Utils.tableSize(results.reserves))
              })
              return
            end

            msg.reply({
              Action = msg.Action .. "Response",
              Status = "Success",
              Pools = tostring(#results.pools),
              Tokens = tostring(#results.tokens),
              Reserves = tostring(Utils.tableSize(results.reserves)),
              GraphRebuilt = "true"
            })
          end)
        else
          -- No graph rebuild requested
          Logger.info("Skipping graph build")
          msg.reply({
            Action = msg.Action .. "Response",
            Status = "Success",
            Pools = tostring(#results.pools),
            Tokens = tostring(#results.tokens),
            Reserves = tostring(Utils.tableSize(results.reserves)),
            Saved = "true"
          })
        end
      end)
    else
      msg.reply({
        Action = msg.Action .. "Response",
        Status = "Success",
        Pools = tostring(#results.pools),
        Tokens = tostring(#results.tokens),
        Reserves = tostring(Utils.tableSize(results.reserves)),
        Errors = Utils.jsonEncode(results.errors)
      })
    end
  end)
end

-- These are the initialization handlers
DexHandlers.handleInitMessage = Init.handleInitMessage
DexHandlers.handleResetMessage = Init.handleResetMessage

return DexHandlers
