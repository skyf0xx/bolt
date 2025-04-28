local Constants = require('arbitrage.constants')
local Logger = require('arbitrage.logger').createLogger("Collector")
local Utils = require('arbitrage.utils')
local Permaswap = require('arbitrage.collectors.permaswap')
local Botega = require('arbitrage.collectors.botega')
local TokenRepository = require('arbitrage.db.token_repository')
local PoolRepository = require('arbitrage.db.pool_repository')

local Collector = {}

-- Initialize the collector with database connection
function Collector.init(db)
  Collector.db = db
  Logger.info("Collector initialized")
  return Collector
end

-- Collect data from a specific DEX
function Collector.collectFromDex(source, poolAddresses)
  if not poolAddresses or #poolAddresses == 0 then
    return { pools = {}, tokens = {}, reserves = {}, errors = { message = "No pool addresses provided" } }
  end

  local collector
  if source == Constants.SOURCE.PERMASWAP then
    collector = Permaswap
  elseif source == Constants.SOURCE.BOTEGA then
    collector = Botega
  else
    return { pools = {}, tokens = {}, reserves = {}, errors = { message = "Invalid source: " .. tostring(source) } }
  end

  Logger.info("Collecting data from " .. source, { poolCount = #poolAddresses })
  return collector.collectAllPoolsData(poolAddresses)
end

-- Collect data from all configured DEXes
function Collector.collectAll(poolList)
  local results = {
    pools = {},
    tokens = {},
    reserves = {},
    errors = {}
  }

  -- If poolList is provided, use it; otherwise, load from database or use default
  if not poolList then
    poolList = Collector.getConfiguredPools()
  end

  -- Organize pools by source
  local poolsBySource = {}
  for _, pool in ipairs(poolList) do
    if not pool.source or not pool.address then
      table.insert(results.errors, { message = "Invalid pool configuration", pool = pool })
      goto continue
    end

    poolsBySource[pool.source] = poolsBySource[pool.source] or {}
    table.insert(poolsBySource[pool.source], pool.address)

    ::continue::
  end

  -- Collect from each source
  for source, addresses in pairs(poolsBySource) do
    local sourceResults = Collector.collectFromDex(source, addresses)

    -- Merge results
    for _, pool in ipairs(sourceResults.pools) do
      table.insert(results.pools, pool)
    end

    for _, token in ipairs(sourceResults.tokens) do
      local exists = false
      for i, existingToken in ipairs(results.tokens) do
        if existingToken.id == token.id then
          -- Update token if new data is more complete
          if token.symbol and token.symbol ~= "" and existingToken.symbol == "" then
            results.tokens[i].symbol = token.symbol
          end
          if token.name and token.name ~= "" and existingToken.name == "" then
            results.tokens[i].name = token.name
          end
          exists = true
          break
        end
      end

      if not exists then
        table.insert(results.tokens, token)
      end
    end

    -- Merge reserves
    for poolId, reserve in pairs(sourceResults.reserves) do
      results.reserves[poolId] = reserve
    end

    -- Merge errors
    for poolId, err in pairs(sourceResults.errors) do
      results.errors[poolId] = err
    end
  end

  Logger.info("Collection completed", {
    poolCount = #results.pools,
    tokenCount = #results.tokens,
    reserveCount = Utils.tableSize(results.reserves),
    errorCount = Utils.tableSize(results.errors)
  })

  return results
end

-- Save collected data to database
function Collector.saveToDatabase(data)
  if not Collector.db then
    return false, "Database not initialized"
  end

  local db = Collector.db
  Logger.info("Saving collected data to database")

  -- Begin transaction
  db:exec("BEGIN TRANSACTION")

  -- Save tokens
  local tokenSuccess, tokenErr = TokenRepository.batchInsertTokens(db, data.tokens)
  if not tokenSuccess then
    db:exec("ROLLBACK")
    Logger.error("Failed to save tokens", { error = tokenErr })
    return false, "Failed to save tokens: " .. tokenErr
  end

  -- Save pools
  local poolSuccess, poolErr = PoolRepository.batchInsertPools(db, data.pools)
  if not poolSuccess then
    db:exec("ROLLBACK")
    Logger.error("Failed to save pools", { error = poolErr })
    return false, "Failed to save pools: " .. poolErr
  end

  -- Save reserves
  for poolId, reserve in pairs(data.reserves) do
    local reserveA = reserve.reserve_a or "0"
    local reserveB = reserve.reserve_b or "0"

    local reserveSuccess, reserveErr = PoolRepository.updateReserves(db, poolId, reserveA, reserveB)
    if not reserveSuccess then
      Logger.warn("Failed to update reserves for pool", { pool = poolId, error = reserveErr })
      -- Continue with other reserves
    end
  end

  -- Commit transaction
  db:exec("COMMIT")

  Logger.info("Data saved to database", {
    tokens = #data.tokens,
    pools = #data.pools,
    reserves = Utils.tableSize(data.reserves)
  })

  return true
end

-- Get pools that need reserve refresh
function Collector.getPoolsNeedingRefresh(maxAge)
  if not Collector.db then
    return {}, "Database not initialized"
  end

  return PoolRepository.getPoolsNeedingReserveRefresh(Collector.db, maxAge)
end

-- Refresh reserves for specific pools
function Collector.refreshReserves(pools)
  if not pools or #pools == 0 then
    return true, "No pools to refresh"
  end

  local results = {
    success = {},
    errors = {}
  }

  for _, pool in ipairs(pools) do
    local source = pool.source
    local collector

    if source == Constants.SOURCE.PERMASWAP then
      collector = Permaswap
    elseif source == Constants.SOURCE.BOTEGA then
      collector = Botega
    else
      results.errors[pool.id] = "Invalid source: " .. tostring(source)
      goto continue
    end

    -- Fetch reserves
    local reserves, err
    if source == Constants.SOURCE.PERMASWAP then
      reserves, err = collector.fetchReserves(pool.id)
      if reserves then
        reserves = {
          reserve_a = reserves.reserveX,
          reserve_b = reserves.reserveY
        }
      end
    else
      reserves, err = collector.fetchReserves(pool.id)
      if reserves then
        reserves = {
          reserve_a = reserves.reserveA,
          reserve_b = reserves.reserveB
        }
      end
    end

    if not reserves then
      results.errors[pool.id] = err
      goto continue
    end

    -- Update in database
    if Collector.db then
      local success, updateErr = PoolRepository.updateReserves(
        Collector.db,
        pool.id,
        reserves.reserve_a,
        reserves.reserve_b
      )

      if success then
        results.success[pool.id] = reserves
      else
        results.errors[pool.id] = updateErr
      end
    else
      results.success[pool.id] = reserves
    end

    ::continue::
  end

  Logger.info("Reserve refresh completed", {
    successful = Utils.tableSize(results.success),
    failed = Utils.tableSize(results.errors)
  })

  return results
end

-- Get configured pool list (either from database or configuration)
function Collector.getConfiguredPools()
  -- Try to load from database first
  if Collector.db then
    local pools = PoolRepository.getAllPools(Collector.db)
    if pools and #pools > 0 then
      local configuredPools = {}
      for _, pool in ipairs(pools) do
        table.insert(configuredPools, {
          address = pool.id,
          source = pool.source
        })
      end
      return configuredPools
    end
  end

  -- If no pools in database, return an empty list
  -- In a real implementation, this might load from a config file
  Logger.warn("No pools found in database and no default configuration provided")
  return {}
end

-- Calculate output for a specific path (used by path finder)
function Collector.calculatePathOutput(path, inputAmount)
  local result = {
    inputAmount = inputAmount,
    outputAmount = inputAmount,
    steps = {}
  }

  local currentInput = inputAmount

  for i, step in ipairs(path) do
    local poolId = step.pool_id
    local tokenIn = step.from
    local tokenOut = step.to

    -- Get pool data
    local pool = PoolRepository.getPool(Collector.db, poolId)
    if not pool then
      return nil, "Pool not found: " .. poolId
    end

    -- Get reserves
    local reserves = PoolRepository.getReserves(Collector.db, poolId)
    if not reserves then
      return nil, "Reserves not found for pool: " .. poolId
    end

    -- Determine which reserve corresponds to input token
    local reserveIn, reserveOut
    if tokenIn == pool.token_a_id then
      reserveIn = reserves.reserve_a
      reserveOut = reserves.reserve_b
    else
      reserveIn = reserves.reserve_b
      reserveOut = reserves.reserve_a
    end

    -- Calculate output amount based on pool source
    local outputAmount
    if pool.source == Constants.SOURCE.PERMASWAP then
      -- Use Permaswap formula
      outputAmount = Permaswap.calculateOutputAmount(
        currentInput,
        reserveIn,
        reserveOut,
        pool.fee_bps
      )
    elseif pool.source == Constants.SOURCE.BOTEGA then
      -- Convert fee from basis points to percentage for Botega calculation
      local feePercentage = pool.fee_bps / 100
      outputAmount = Botega.calculateOutputAmount(
        currentInput,
        reserveIn,
        reserveOut,
        feePercentage
      )
    else
      return nil, "Unsupported pool source: " .. tostring(pool.source)
    end

    -- Add step to result
    table.insert(result.steps, {
      pool_id = poolId,
      source = pool.source,
      token_in = tokenIn,
      token_out = tokenOut,
      amount_in = currentInput,
      amount_out = outputAmount.value,
      fee_bps = pool.fee_bps
    })

    -- Update for next step
    currentInput = outputAmount.value
  end

  result.outputAmount = currentInput
  return result
end

-- Execute a swap across multiple pools (path)
function Collector.executePathSwap(path, inputAmount, minOutputAmount, userAddress)
  -- This would execute a swap across multiple pools
  -- In a production implementation, this would need to handle:
  -- 1. Cross-DEX token transfers
  -- 2. Atomic execution (or as close as possible)
  -- 3. Slippage protection at each step

  Logger.warn("Path execution not fully implemented, would execute multiple swaps")
  return nil, "Multi-pool swaps require cross-process coordination"
end

return Collector
