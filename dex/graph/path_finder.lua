local Constants = require('dex.utils.constants')
local Logger = require('dex.utils.logger')
Logger = Logger.createLogger("PathFinder")
local Utils = require('dex.utils.utils')
local BigDecimal = require('dex.utils.big_decimal')
local Collector = require('dex.collectors.collector')

local PathFinder = {}

-- Initialize the PathFinder with a graph instance
function PathFinder.init(graph, db)
  PathFinder.graph = graph
  PathFinder.db = db
  Logger.info("PathFinder initialized")
  return PathFinder
end

-- Find all possible paths between two tokens with a maximum number of hops
function PathFinder.findAllPaths(sourceTokenId, targetTokenId, maxHops)
  maxHops = maxHops or Constants.PATH.MAX_PATH_LENGTH

  if not PathFinder.graph or not PathFinder.graph.initialized then
    return {}, "Graph not initialized"
  end

  if sourceTokenId == targetTokenId then
    return {}, "Source and target tokens are the same"
  end

  if not PathFinder.graph.nodes[sourceTokenId] then
    return {}, "Source token not found in graph"
  end

  if not PathFinder.graph.nodes[targetTokenId] then
    return {}, "Target token not found in graph"
  end

  local paths = {}
  local visited = {}

  -- DFS implementation for path finding
  local function dfs(currentTokenId, path, depth)
    if depth > maxHops then
      return
    end

    visited[currentTokenId] = true

    -- If we reached the target token, add the path to results
    if currentTokenId == targetTokenId then
      table.insert(paths, Utils.deepCopy(path))
      visited[currentTokenId] = false
      return
    end

    -- Check all connected tokens
    if PathFinder.graph.edges[currentTokenId] then
      for _, edge in ipairs(PathFinder.graph.edges[currentTokenId]) do
        local nextTokenId = edge.connected_to

        -- Skip inactive pools and already visited tokens
        if edge.status ~= "active" or visited[nextTokenId] then
          goto continue
        end

        -- Add this step to the path
        table.insert(path, {
          from = currentTokenId,
          to = nextTokenId,
          pool_id = edge.pool_id,
          source = edge.source,
          fee_bps = edge.fee_bps
        })

        -- Continue DFS from this token
        dfs(nextTokenId, path, depth + 1)

        -- Remove the last step (backtrack)
        table.remove(path)

        ::continue::
      end
    end

    visited[currentTokenId] = false
  end

  -- Start DFS from source token
  dfs(sourceTokenId, {}, 1)

  -- Sort paths by length (shorter paths first)
  table.sort(paths, function(a, b)
    return #a < #b
  end)

  -- Limit the number of paths to return
  if #paths > Constants.PATH.MAX_PATHS_TO_RETURN then
    local limitedPaths = {}
    for i = 1, Constants.PATH.MAX_PATHS_TO_RETURN do
      table.insert(limitedPaths, paths[i])
    end
    paths = limitedPaths
  end

  return paths
end

-- Calculate total fee for a path (in basis points)
function PathFinder.calculatePathFee(path)
  local totalFee = 0

  for _, step in ipairs(path) do
    -- Compounding fee calculation
    local stepFee = step.fee_bps or 0
    totalFee = totalFee + stepFee - (totalFee * stepFee / Constants.NUMERIC.BASIS_POINTS_MULTIPLIER)
  end

  return math.floor(totalFee)
end

-- Calculate initial ranking for paths based on total fee
function PathFinder.rankPathsByFee(paths)
  local rankedPaths = {}

  for _, path in ipairs(paths) do
    local totalFee = PathFinder.calculatePathFee(path)

    table.insert(rankedPaths, {
      path = path,
      totalFee = totalFee,
      score = 10000 - totalFee -- Higher score is better
    })
  end

  -- Sort by score (descending)
  table.sort(rankedPaths, function(a, b)
    return a.score > b.score
  end)

  return rankedPaths
end

-- Find optimal path between two tokens
function PathFinder.findOptimalPath(sourceTokenId, targetTokenId, inputAmount, callback)
  local paths = PathFinder.findAllPaths(sourceTokenId, targetTokenId)

  if #paths == 0 then
    callback(nil, "No path found between tokens")
    return
  end

  -- Initial ranking based on fees only
  local rankedPaths = PathFinder.rankPathsByFee(paths)

  -- Take top N paths for reserve polling and slippage calculation
  local candidatePaths = {}
  local maxCandidates = math.min(5, #rankedPaths)

  for i = 1, maxCandidates do
    table.insert(candidatePaths, rankedPaths[i])
  end

  -- Poll reserves and calculate expected output for each path
  local pendingPaths = #candidatePaths
  local pathResults = {}

  if pendingPaths == 0 then
    callback(nil, "No candidate paths available")
    return
  end

  for i, candidate in ipairs(candidatePaths) do
    Collector.calculatePathOutput(candidate.path, inputAmount, function(result, err)
      pendingPaths = pendingPaths - 1

      if result then
        -- Calculate effective price and update score
        local effectivePrice = BigDecimal.divide(
          BigDecimal.new(result.outputAmount),
          BigDecimal.new(inputAmount)
        )

        pathResults[i] = {
          path = candidate.path,
          totalFee = candidate.totalFee,
          inputAmount = inputAmount,
          outputAmount = result.outputAmount,
          effectivePrice = effectivePrice,
          steps = result.steps,
          score = tonumber(effectivePrice.value) -- Score based on output amount
        }
      else
        Logger.warn("Path calculation failed", { error = err, path = Utils.jsonEncode(candidate.path) })
      end

      -- Once all paths are processed, return the best one
      if pendingPaths == 0 then
        -- Sort by score (highest output amount first)
        table.sort(pathResults, function(a, b)
          return a.score > b.score
        end)

        if #pathResults > 0 then
          callback(pathResults[1])
        else
          callback(nil, "All path calculations failed")
        end
      end
    end)
  end
end

-- Find arbitrage opportunities in the graph
function PathFinder.findArbitrageOpportunities(startTokenId, inputAmount, callback)
  startTokenId = startTokenId or Constants.PATH.DEFAULT_ARBITRAGE_TOKEN
  inputAmount = inputAmount or Constants.NUMERIC.DEFAULT_SWAP_INPUT

  if not PathFinder.graph or not PathFinder.graph.initialized then
    callback(nil, "Graph not initialized")
    return
  end

  -- Find cycles that start and end at the same token
  local cycles = PathFinder.graph:findCycles(startTokenId)

  if #cycles == 0 then
    callback({ opportunities = {} }, "No cycles found")
    return
  end

  Logger.info("Found cycles", { count = #cycles })

  -- Analyze each cycle for arbitrage opportunity
  local pendingCycles = #cycles
  local opportunities = {}

  for _, cycle in ipairs(cycles) do
    Collector.calculatePathOutput(cycle, inputAmount, function(result, err)
      pendingCycles = pendingCycles - 1

      if result then
        local profitAmount = BigDecimal.subtract(
          BigDecimal.new(result.outputAmount),
          BigDecimal.new(inputAmount)
        )

        -- Calculate profit in basis points
        local profitBps = math.floor(tonumber(BigDecimal.divide(
          BigDecimal.multiply(profitAmount, BigDecimal.new(Constants.NUMERIC.BASIS_POINTS_MULTIPLIER)),
          BigDecimal.new(inputAmount)
        ).value) or 0)

        -- If profitable (considering gas costs), add to opportunities
        if profitBps > Constants.NUMERIC.DEFAULT_MIN_PROFIT_BPS then
          table.insert(opportunities, {
            cycle = cycle,
            inputAmount = inputAmount,
            outputAmount = result.outputAmount,
            profitAmount = profitAmount.value,
            profitBps = profitBps,
            steps = result.steps
          })
        end
      end

      -- If all cycles are processed, return the results
      if pendingCycles == 0 then
        -- Sort opportunities by profit (descending)
        table.sort(opportunities, function(a, b)
          return a.profitBps > b.profitBps
        end)

        Logger.info("Arbitrage analysis completed", {
          total = #cycles,
          opportunities = #opportunities
        })

        callback({ opportunities = opportunities })
      end
    end)
  end
end

-- Find the best route for a token swap
function PathFinder.findBestRoute(sourceTokenId, targetTokenId, inputAmount, callback)
  if not PathFinder.graph or not PathFinder.graph.initialized then
    callback(nil, "Graph not initialized")
    return
  end

  -- Direct pool check first (fastest path)
  local directPools = PathFinder.graph:getDirectPools(sourceTokenId, targetTokenId)

  if #directPools > 0 then
    Logger.info("Found direct connection", { poolCount = #directPools })

    -- If multiple direct pools, find the best one
    if #directPools > 1 then
      local pendingPools = #directPools
      local poolResults = {}

      for i, pool in ipairs(directPools) do
        -- Build a simple path for this direct pool
        local path = {
          {
            from = sourceTokenId,
            to = targetTokenId,
            pool_id = pool.id,
            source = pool.source,
            fee_bps = pool.fee_bps
          }
        }

        Collector.calculatePathOutput(path, inputAmount, function(result, err)
          pendingPools = pendingPools - 1

          if result then
            poolResults[i] = {
              path = path,
              outputAmount = result.outputAmount,
              steps = result.steps
            }
          end

          if pendingPools == 0 then
            -- Sort by output amount (descending)
            table.sort(poolResults, function(a, b)
              return BigDecimal.new(a.outputAmount).value > BigDecimal.new(b.outputAmount).value
            end)

            if #poolResults > 0 then
              callback(poolResults[1])
            else
              -- If all direct paths failed, try multi-hop
              FindMultiHopRoute()
            end
          end
        end)
      end
    else
      -- Just one direct pool, build a simple path
      local path = {
        {
          from = sourceTokenId,
          to = targetTokenId,
          pool_id = directPools[1].id,
          source = directPools[1].source,
          fee_bps = directPools[1].fee_bps
        }
      }

      Collector.calculatePathOutput(path, inputAmount, function(result, err)
        if result then
          callback({
            path = path,
            outputAmount = result.outputAmount,
            steps = result.steps
          })
        else
          -- If direct path failed, try multi-hop
          FindMultiHopRoute()
        end
      end)
    end
  else
    -- No direct connection, try multi-hop paths
    FindMultiHopRoute()
  end

  -- Function to find multi-hop route
  function FindMultiHopRoute()
    Logger.info("Looking for multi-hop routes")

    -- Find the optimal path (this handles multiple hops)
    PathFinder.findOptimalPath(sourceTokenId, targetTokenId, inputAmount, function(result, err)
      if result then
        callback(result)
      else
        callback(nil, err or "No viable path found")
      end
    end)
  end
end

-- Generate quote for a specific path
function PathFinder.generateQuote(path, inputAmount, tokenDecimals, callback)
  if not path or #path == 0 then
    callback(nil, "Invalid path")
    return
  end

  Collector.calculatePathOutput(path, inputAmount, function(result, err)
    if not result then
      callback(nil, err or "Failed to calculate path output")
      return
    end

    -- Calculate price impact and other metrics
    local priceImpact = 0
    local totalFee = PathFinder.calculatePathFee(path)

    -- Generate a user-friendly quote
    local sourceTokenId = path[1].from
    local targetTokenId = path[#path].to

    -- Get token information
    local sourceToken = PathFinder.graph:getToken(sourceTokenId)
    local targetToken = PathFinder.graph:getToken(targetTokenId)

    local sourceDecimals = sourceToken and sourceToken.decimals or tokenDecimals.source or Constants.NUMERIC.DECIMALS
    local targetDecimals = targetToken and targetToken.decimals or tokenDecimals.target or Constants.NUMERIC.DECIMALS

    -- Format amounts for display
    local formattedInputAmount = Utils.formatTokenAmount(inputAmount, sourceDecimals)
    local formattedOutputAmount = Utils.formatTokenAmount(result.outputAmount, targetDecimals)

    -- Calculate execution price
    local executionPrice = BigDecimal.divide(
      BigDecimal.fromTokenAmount(result.outputAmount, targetDecimals),
      BigDecimal.fromTokenAmount(inputAmount, sourceDecimals)
    )

    local quote = {
      path = path,
      steps = result.steps,
      sourceToken = sourceToken,
      targetToken = targetToken,
      inputAmount = inputAmount,
      outputAmount = result.outputAmount,
      formattedInputAmount = formattedInputAmount,
      formattedOutputAmount = formattedOutputAmount,
      executionPrice = executionPrice.toDecimal(8),
      priceImpact = priceImpact,
      fee = {
        bps = totalFee,
        percent = Utils.bpsToDecimal(totalFee)
      },
      route = {
        hops = #path,
        sources = {} -- Will fill below
      }
    }

    -- Extract sources used in the path
    local sources = {}
    for _, step in ipairs(path) do
      if not Utils.tableContains(sources, step.source) then
        table.insert(sources, step.source)
      end
    end
    quote.route.sources = sources

    callback(quote)
  end)
end

-- Find direct swaps between two tokens
-- Find direct swaps between two tokens
function PathFinder.findDirectSwaps(sourceTokenId, targetTokenId, inputAmount, callback)
  Logger.debug("Finding direct swaps", { source = sourceTokenId, target = targetTokenId, amount = inputAmount })

  -- Get direct connections between tokens
  local directPools = PathFinder.graph:getDirectPools(sourceTokenId, targetTokenId)

  if #directPools == 0 then
    Logger.debug("No direct pools found")
    callback(nil)
    return
  end

  Logger.info("Found direct connections", { poolCount = #directPools })

  -- Sort pools by fee (ascending) - prioritize lower fee pools first
  table.sort(directPools, function(a, b)
    return a.fee_bps < b.fee_bps
  end)

  -- Limit to top N pools to reduce API calls
  local maxDirectPools = 3 -- Adjust based on your preferences
  if #directPools > maxDirectPools then
    local limitedPools = {}
    for i = 1, maxDirectPools do
      table.insert(limitedPools, directPools[i])
    end
    directPools = limitedPools
    Logger.debug("Limited direct pool evaluation", { limit = maxDirectPools })
  end

  -- Create simple paths for all direct pools
  local paths = {}
  for _, pool in ipairs(directPools) do
    table.insert(paths, {
      {
        from = sourceTokenId,
        to = targetTokenId,
        pool_id = pool.id,
        source = pool.source,
        fee_bps = pool.fee_bps
      }
    })
  end

  -- Calculate output for each direct path
  local pendingPaths = #paths
  local pathResults = {}

  for i, path in ipairs(paths) do
    Collector.calculatePathOutput(path, inputAmount, function(result, err)
      pendingPaths = pendingPaths - 1

      if result then
        pathResults[i] = {
          path = path,
          totalFee = path[1].fee_bps,
          inputAmount = inputAmount,
          outputAmount = result.outputAmount,
          steps = result.steps
        }
      else
        Logger.warn("Direct path calculation failed", {
          poolId = path[1].pool_id,
          error = err
        })
      end

      if pendingPaths == 0 then
        -- Sort by output amount (descending)
        table.sort(pathResults, function(a, b)
          return BigDecimal.new(a.outputAmount).value > BigDecimal.new(b.outputAmount).value
        end)

        if #pathResults > 0 then
          callback({
            paths = pathResults,
            bestPath = pathResults[1],
            isDirect = true
          })
        else
          Logger.warn("All direct path calculations failed")
          callback(nil)
        end
      end
    end)
  end
end

-- Find paths for swapping a specific amount of token for maximum output
function PathFinder.findOptimalSwap(sourceTokenId, targetTokenId, inputAmount, options, callback)
  options = options or {}
  local maxHops = options.maxHops or Constants.PATH.MAX_PATH_LENGTH
  local maxPaths = options.maxPaths or Constants.PATH.MAX_PATHS_TO_RETURN

  -- First try to find direct swaps
  PathFinder.findDirectSwaps(sourceTokenId, targetTokenId, inputAmount, function(directResults)
    if directResults then
      Logger.info("Using direct swap path", {
        outputAmount = directResults.bestPath.outputAmount,
        poolId = directResults.bestPath.path[1].pool_id
      })
      callback(directResults)
      return
    end

    -- ONLY if no direct paths are found, process multi-hop paths
    Logger.debug("Falling back to multi-hop paths")
    local paths = PathFinder.findAllPaths(sourceTokenId, targetTokenId, maxHops)

    if #paths == 0 then
      callback({
        paths = {},
        bestPath = nil,
        error = "No paths found between tokens"
      })
      return
    end

    -- Initial ranking based on fees
    local rankedPaths = PathFinder.rankPathsByFee(paths)

    -- Take top N paths for detailed analysis
    local candidatePaths = {}
    local maxCandidates = math.min(maxPaths, #rankedPaths)

    for i = 1, maxCandidates do
      table.insert(candidatePaths, rankedPaths[i])
    end

    -- Calculate output for each candidate path
    local pendingPaths = #candidatePaths
    local pathResults = {}

    if pendingPaths == 0 then
      callback({
        paths = {},
        bestPath = nil,
        error = "No candidate paths available"
      })
      return
    end

    for i, candidate in ipairs(candidatePaths) do
      Collector.calculatePathOutput(candidate.path, inputAmount, function(result, err)
        pendingPaths = pendingPaths - 1

        if result then
          pathResults[i] = {
            path = candidate.path,
            totalFee = candidate.totalFee,
            inputAmount = inputAmount,
            outputAmount = result.outputAmount,
            steps = result.steps
          }
        else
          Logger.warn("Path calculation failed", { error = err })
        end

        -- Once all paths are processed, return the results
        if pendingPaths == 0 then
          -- Sort by output amount (descending)
          table.sort(pathResults, function(a, b)
            return BigDecimal.new(a.outputAmount).value > BigDecimal.new(b.outputAmount).value
          end)

          callback({
            paths = pathResults,
            bestPath = #pathResults > 0 and pathResults[1] or nil,
            error = #pathResults == 0 and "All path calculations failed" or nil
          })
        end
      end)
    end
  end)
  -- Remove all code here that was outside the callback
end

-- Create a human-readable description of a path
function PathFinder.describePathRoute(path)
  if not path or #path == 0 then
    return "Invalid path"
  end

  local description = {}

  for i, step in ipairs(path) do
    local sourceToken = PathFinder.graph:getToken(step.from)
    local targetToken = PathFinder.graph:getToken(step.to)
    local pool = PathFinder.graph:getPool(step.pool_id)

    local sourceSymbol = sourceToken and sourceToken.symbol or step.from:sub(1, 8)
    local targetSymbol = targetToken and targetToken.symbol or step.to:sub(1, 8)
    local dexName = step.source:sub(1, 1):upper() .. step.source:sub(2)

    table.insert(description, string.format(
      "%d. %s → %s via %s (fee: %0.2f%%)",
      i,
      sourceSymbol,
      targetSymbol,
      dexName,
      step.fee_bps / 100
    ))
  end

  return table.concat(description, "\n")
end

return PathFinder
