local Constants = require('arbitrage.constants')
local Logger = require('arbitrage.logger').createLogger("GraphBuilder")
local Utils = require('arbitrage.utils')
local Graph = require('arbitrage.graph.graph')
local TokenRepository = require('arbitrage.db.token_repository')
local PoolRepository = require('arbitrage.db.pool_repository')

local GraphBuilder = {}

-- Initialize the graph builder with database connection
function GraphBuilder.init(db)
  GraphBuilder.db = db
  Logger.info("Graph builder initialized")
  return GraphBuilder
end

-- Build full graph from database
function GraphBuilder.buildGraph(callback)
  if not GraphBuilder.db then
    callback(nil, "Database not initialized")
    return
  end

  local graph = Graph.new()

  -- Get all tokens
  local tokens = TokenRepository.getAllTokens(GraphBuilder.db)
  if not tokens then
    callback(nil, "Failed to retrieve tokens from database")
    return
  end

  Logger.info("Building graph with tokens", { count = #tokens })

  -- Add all tokens to graph
  for _, token in ipairs(tokens) do
    graph:addToken(token)
  end

  -- Get all pools with token information
  local pools = PoolRepository.getPoolsWithTokenInfo(GraphBuilder.db)
  if not pools then
    callback(nil, "Failed to retrieve pools from database")
    return
  end

  Logger.info("Adding pools to graph", { count = #pools })

  -- Add all pools to graph
  for _, pool in ipairs(pools) do
    graph:addPool({
      id = pool.id,
      source = pool.source,
      token_a_id = pool.token_a.id,
      token_b_id = pool.token_b.id,
      fee_bps = pool.fee_bps,
      status = pool.status
    })
  end

  -- Get all reserves
  local reserves = PoolRepository.getAllReserves(GraphBuilder.db)

  -- Track reserve data in graph for quick access (optional extension)
  graph.reserves = reserves

  Logger.info("Graph built successfully", graph:getStats())
  callback(graph)
end

-- Build partial graph for specific tokens (for optimization)
function GraphBuilder.buildPartialGraph(tokenIds, maxHops, callback)
  if not GraphBuilder.db then
    callback(nil, "Database not initialized")
    return
  end

  if not tokenIds or #tokenIds == 0 then
    callback(nil, "No token IDs provided")
    return
  end

  maxHops = maxHops or Constants.PATH.MAX_PATH_LENGTH
  local graph = Graph.new()

  -- Get tokens
  local tokens = TokenRepository.getTokensByIds(GraphBuilder.db, tokenIds)
  if not tokens or #tokens == 0 then
    callback(nil, "No tokens found with provided IDs")
    return
  end

  -- Add original tokens to graph
  for _, token in ipairs(tokens) do
    graph:addToken(token)
  end

  -- Find all connected pools within maxHops
  local visitedTokens = {}
  local visitedPools = {}
  local tokenQueue = {}

  -- Initialize queue with starting tokens
  for _, tokenId in ipairs(tokenIds) do
    visitedTokens[tokenId] = true
    table.insert(tokenQueue, { id = tokenId, depth = 0 })
  end

  -- BFS to find all relevant pools and tokens
  while #tokenQueue > 0 do
    local currentToken = table.remove(tokenQueue, 1)

    -- Skip if we've reached max depth
    if currentToken.depth >= maxHops then
      goto continue
    end

    -- Get all pools involving this token
    local pools = PoolRepository.getPoolsByToken(GraphBuilder.db, currentToken.id)

    for _, pool in ipairs(pools) do
      -- Skip already visited pools
      if visitedPools[pool.id] then
        goto nextPool
      end

      visitedPools[pool.id] = true

      -- Add pool to graph
      graph:addPool(pool)

      -- Get the other token in the pair
      local otherTokenId = pool.token_a_id
      if otherTokenId == currentToken.id then
        otherTokenId = pool.token_b_id
      end

      -- If not visited, add to queue
      if not visitedTokens[otherTokenId] then
        visitedTokens[otherTokenId] = true

        -- Get token data and add to graph
        local token = TokenRepository.getToken(GraphBuilder.db, otherTokenId)
        if token then
          graph:addToken(token)

          -- Add to queue for further exploration
          table.insert(tokenQueue, { id = otherTokenId, depth = currentToken.depth + 1 })
        end
      end

      ::nextPool::
    end

    ::continue::
  end

  -- Get reserves for all pools in the graph
  local poolIds = {}
  for _, pool in ipairs(graph:getPools()) do
    table.insert(poolIds, pool.id)
  end

  local reserves = {}
  for _, poolId in ipairs(poolIds) do
    local poolReserves = PoolRepository.getReserves(GraphBuilder.db, poolId)
    if poolReserves then
      reserves[poolId] = poolReserves
    end
  end

  -- Attach reserves to graph
  graph.reserves = reserves

  Logger.info("Partial graph built successfully", {
    tokens = graph.tokenCount,
    pools = graph.edgeCount,
    originTokens = #tokenIds,
    maxHops = maxHops
  })

  callback(graph)
end

-- Build arbitrage-specific graph (cycles only)
function GraphBuilder.buildArbitrageGraph(tokenId, maxHops, callback)
  if not GraphBuilder.db then
    callback(nil, "Database not initialized")
    return
  end

  tokenId = tokenId or Constants.PATH.DEFAULT_ARBITRAGE_TOKEN
  maxHops = maxHops or Constants.PATH.MAX_PATH_LENGTH

  -- First build a partial graph centered on the token
  GraphBuilder.buildPartialGraph({ tokenId }, maxHops, function(graph, err)
    if not graph then
      callback(nil, err)
      return
    end

    -- Find all cycles starting from this token
    local cycles = graph:findCycles(tokenId, maxHops)

    Logger.info("Arbitrage graph built", {
      baseToken = tokenId,
      cycles = #cycles,
      maxHops = maxHops
    })

    callback({
      graph = graph,
      cycles = cycles,
      baseToken = tokenId
    })
  end)
end

-- Refresh the graph with latest data
function GraphBuilder.refreshGraph(graph, callback)
  if not GraphBuilder.db or not graph then
    callback(false, "Database or graph not initialized")
    return
  end

  -- Get all pools that need reserve refresh
  local pools = PoolRepository.getPoolsNeedingReserveRefresh(GraphBuilder.db)

  if #pools == 0 then
    Logger.info("No pools need reserve refresh")
    callback(true)
    return
  end

  Logger.info("Refreshing reserves for pools", { count = #pools })

  local pendingPools = #pools
  for _, pool in ipairs(pools) do
    local reserves = PoolRepository.getReserves(GraphBuilder.db, pool.id)

    if reserves then
      -- Update in-memory reserves
      if not graph.reserves then
        graph.reserves = {}
      end

      graph.reserves[pool.id] = reserves
    end

    pendingPools = pendingPools - 1
    if pendingPools == 0 then
      Logger.info("Graph reserves refreshed")
      callback(true)
    end
  end
end

-- Export graph data for visualization
function GraphBuilder.exportGraphData(graph)
  if not graph then
    return nil, "Graph not initialized"
  end

  return {
    nodes = graph:getTokens(),
    edges = graph:getPools(),
    stats = graph:getStats()
  }
end

-- Export graph in DOT format for visualization with Graphviz
function GraphBuilder.exportToDot(graph)
  if not graph then
    return nil, "Graph not initialized"
  end

  return graph:exportToDot()
end

return GraphBuilder
