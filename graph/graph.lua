local Constants = require('arbitrage.constants')
local Logger = require('arbitrage.logger').createLogger("Graph")
local Utils = require('arbitrage.utils')

local Graph = {}

-- Create a new Graph instance
function Graph.new()
  local instance = {
    nodes = {},     -- Map of token_id -> token data
    edges = {},     -- Map of token_id -> list of connections (pools)
    sources = {},   -- Track available sources (DEXes)
    poolsById = {}, -- Quick lookup of pools by ID
    initialized = false,
    tokenCount = 0,
    edgeCount = 0
  }

  setmetatable(instance, { __index = Graph })
  return instance
end

-- Add a token (node) to the graph
function Graph:addToken(token)
  if not token or not token.id then
    return false, "Invalid token data"
  end

  if not self.nodes[token.id] then
    self.nodes[token.id] = {
      id = token.id,
      symbol = token.symbol or "",
      name = token.name or "",
      decimals = token.decimals or Constants.NUMERIC.DECIMALS,
      logo_url = token.logo_url or ""
    }

    -- Initialize empty adjacency list for this token
    if not self.edges[token.id] then
      self.edges[token.id] = {}
    end

    self.tokenCount = self.tokenCount + 1
    return true
  end

  return false, "Token already exists"
end

-- Add a pool (edge) to the graph
function Graph:addPool(pool)
  if not pool or not pool.id or not pool.token_a_id or not pool.token_b_id then
    return false, "Invalid pool data"
  end

  -- Ensure tokens exist in the graph
  if not self.nodes[pool.token_a_id] then
    Logger.warn("Adding edge for non-existent token", { token = pool.token_a_id })
    self:addToken({ id = pool.token_a_id })
  end

  if not self.nodes[pool.token_b_id] then
    Logger.warn("Adding edge for non-existent token", { token = pool.token_b_id })
    self:addToken({ id = pool.token_b_id })
  end

  -- Initialize edge lists if they don't exist
  if not self.edges[pool.token_a_id] then
    self.edges[pool.token_a_id] = {}
  end

  if not self.edges[pool.token_b_id] then
    self.edges[pool.token_b_id] = {}
  end

  -- Add the pool as an edge in both directions (tokens can be swapped both ways)
  -- Direction A -> B
  table.insert(self.edges[pool.token_a_id], {
    connected_to = pool.token_b_id,
    pool_id = pool.id,
    source = pool.source,
    fee_bps = pool.fee_bps,
    status = pool.status or "active"
  })

  -- Direction B -> A
  table.insert(self.edges[pool.token_b_id], {
    connected_to = pool.token_a_id,
    pool_id = pool.id,
    source = pool.source,
    fee_bps = pool.fee_bps,
    status = pool.status or "active"
  })

  -- Track the pool by ID for quick lookup
  self.poolsById[pool.id] = {
    id = pool.id,
    source = pool.source,
    token_a_id = pool.token_a_id,
    token_b_id = pool.token_b_id,
    fee_bps = pool.fee_bps,
    status = pool.status or "active"
  }

  -- Track available sources
  if not Utils.tableContains(self.sources, pool.source) then
    table.insert(self.sources, pool.source)
  end

  self.edgeCount = self.edgeCount + 1
  return true
end

-- Build the graph from a list of pools
function Graph:buildFromPools(pools, tokens)
  -- First add all tokens
  if tokens then
    for _, token in ipairs(tokens) do
      self:addToken(token)
    end
  end

  -- Then add all pools
  for _, pool in ipairs(pools) do
    self:addPool(pool)
  end

  self.initialized = true
  Logger.info("Graph built successfully", {
    tokens = self.tokenCount,
    pools = self.edgeCount,
    sources = #self.sources
  })

  return true
end

-- Get all tokens in the graph
function Graph:getTokens()
  local tokens = {}
  for id, data in pairs(self.nodes) do
    table.insert(tokens, data)
  end
  return tokens
end

-- Get all pools in the graph
function Graph:getPools()
  local pools = {}
  for id, data in pairs(self.poolsById) do
    table.insert(pools, data)
  end
  return pools
end

-- Get a token by ID
function Graph:getToken(tokenId)
  return self.nodes[tokenId]
end

-- Get a pool by ID
function Graph:getPool(poolId)
  return self.poolsById[poolId]
end

-- Get all pools connecting two tokens (direct connections only)
function Graph:getDirectPools(tokenA, tokenB)
  local directPools = {}

  if not self.edges[tokenA] then
    return directPools
  end

  for _, edge in ipairs(self.edges[tokenA]) do
    if edge.connected_to == tokenB then
      table.insert(directPools, self.poolsById[edge.pool_id])
    end
  end

  return directPools
end

-- Get all tokens connected to a specific token
function Graph:getConnectedTokens(tokenId)
  local connected = {}

  if not self.edges[tokenId] then
    return connected
  end

  for _, edge in ipairs(self.edges[tokenId]) do
    if edge.status == "active" then
      table.insert(connected, {
        token = self.nodes[edge.connected_to],
        pool = self.poolsById[edge.pool_id]
      })
    end
  end

  return connected
end

-- Check if a path exists between two tokens
function Graph:hasPath(sourceTokenId, targetTokenId)
  if sourceTokenId == targetTokenId then
    return true, {}
  end

  if not self.nodes[sourceTokenId] or not self.nodes[targetTokenId] then
    return false, "One or both tokens do not exist in the graph"
  end

  -- Simple BFS to find any path
  local visited = {}
  local queue = { { tokenId = sourceTokenId, path = {} } }

  while #queue > 0 do
    local current = table.remove(queue, 1)
    local currentTokenId = current.tokenId

    if visited[currentTokenId] then
      goto continue
    end

    visited[currentTokenId] = true

    if currentTokenId == targetTokenId then
      return true, current.path
    end

    if self.edges[currentTokenId] then
      for _, edge in ipairs(self.edges[currentTokenId]) do
        if edge.status == "active" and not visited[edge.connected_to] then
          local newPath = Utils.deepCopy(current.path)
          table.insert(newPath, {
            from = currentTokenId,
            to = edge.connected_to,
            pool_id = edge.pool_id
          })

          table.insert(queue, {
            tokenId = edge.connected_to,
            path = newPath
          })
        end
      end
    end

    ::continue::
  end

  return false, "No path found"
end

-- Get graph statistics
function Graph:getStats()
  return {
    tokens = self.tokenCount,
    pools = self.edgeCount,
    sources = self.sources,
    initialized = self.initialized
  }
end

-- Find cyclic paths starting and ending at the same token (for arbitrage detection)
function Graph:findCycles(startTokenId, maxHops)
  maxHops = maxHops or Constants.PATH.MAX_PATH_LENGTH

  if not self.nodes[startTokenId] then
    return {}, "Start token does not exist in the graph"
  end

  local cycles = {}
  local function dfs(currentTokenId, path, visited, depth)
    if depth > maxHops then
      return
    end

    visited[currentTokenId] = true

    if self.edges[currentTokenId] then
      for _, edge in ipairs(self.edges[currentTokenId]) do
        if edge.status ~= "active" then
          goto continue
        end

        local nextTokenId = edge.connected_to

        -- If we've found a cycle back to the start token
        if nextTokenId == startTokenId and depth > 1 then
          local cyclePath = Utils.deepCopy(path)
          table.insert(cyclePath, {
            from = currentTokenId,
            to = nextTokenId,
            pool_id = edge.pool_id
          })

          table.insert(cycles, cyclePath)
          goto continue
        end

        -- Continue searching if token not visited
        if not visited[nextTokenId] then
          local newPath = Utils.deepCopy(path)
          table.insert(newPath, {
            from = currentTokenId,
            to = nextTokenId,
            pool_id = edge.pool_id
          })

          dfs(nextTokenId, newPath, Utils.deepCopy(visited), depth + 1)
        end

        ::continue::
      end
    end
  end

  dfs(startTokenId, {}, {}, 1)

  -- Limit number of cycles to analyze
  if #cycles > Constants.NUMERIC.MAX_CYCLES_TO_ANALYZE then
    Logger.warn("Too many cycles found, limiting results", {
      found = #cycles,
      limit = Constants.NUMERIC.MAX_CYCLES_TO_ANALYZE
    })

    local limitedCycles = {}
    for i = 1, Constants.NUMERIC.MAX_CYCLES_TO_ANALYZE do
      table.insert(limitedCycles, cycles[i])
    end

    return limitedCycles
  end

  return cycles
end

-- Clear the graph
function Graph:clear()
  self.nodes = {}
  self.edges = {}
  self.sources = {}
  self.poolsById = {}
  self.initialized = false
  self.tokenCount = 0
  self.edgeCount = 0

  Logger.info("Graph cleared")
  return true
end

-- Export graph to dot format for visualization (for debugging/analysis)
function Graph:exportToDot()
  local lines = { "digraph DEXGraph {" }

  -- Add nodes
  for id, token in pairs(self.nodes) do
    local label = token.symbol or id:sub(1, 8)
    table.insert(lines, string.format('  "%s" [label="%s"];', id, label))
  end

  -- Add edges
  for tokenId, connections in pairs(self.edges) do
    for _, edge in ipairs(connections) do
      local label = string.format("%s (%s, %sbps)",
        edge.pool_id:sub(1, 6),
        edge.source,
        edge.fee_bps)

      table.insert(lines, string.format('  "%s" -> "%s" [label="%s"];',
        tokenId, edge.connected_to, label))
    end
  end

  table.insert(lines, "}")
  return table.concat(lines, "\n")
end

return Graph
