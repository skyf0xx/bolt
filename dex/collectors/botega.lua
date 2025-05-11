local Constants = require('dex.utils.constants')
local Logger = require('dex.utils.logger')
Logger = Logger.createLogger("BotegaCollector")
local Utils = require('dex.utils.utils')

local Botega = {}

-- Fetch basic information about a Botega pool
function Botega.fetchPoolInfo(poolAddress, collector, callback)
  Logger.debug("Fetching pool info with tracking", { pool = poolAddress })

  -- Add to pending collections
  collector.pendingCollections[poolAddress] = {
    source = Constants.SOURCE.BOTEGA,
    startTime = os.time(),
    poolId = poolAddress,
    poolCount = 1,
    completedPools = 0,
    callback = callback
  }

  ao.send({
    Target = poolAddress,
    Action = Constants.API.BOTEGA.INFO
  }).onReply(function(response)
    if response.Error then
      Logger.error("Failed to fetch pool info", {
        pool = poolAddress,
        error = response.Error
      })
      -- Remove from pending collections on error
      collector.pendingCollections[poolAddress] = nil
      callback(nil, response.Error)
    else
      callback(response)
    end
  end)
end

-- Fetch token pair information for a pool
function Botega.fetchPair(poolAddress, callback)
  Logger.debug("Fetching token pair", { pool = poolAddress })

  ao.send({
    Target = poolAddress,
    Action = Constants.API.BOTEGA.GET_PAIR
  }).onReply(function(response)
    if response.Error then
      Logger.error("Failed to fetch token pair", {
        pool = poolAddress,
        error = response.Error
      })
      callback(nil, response.Error)
    else
      callback({
        tokenA = response["Token-A"],
        tokenB = response["Token-B"]
      })
    end
  end)
end

-- Fetch reserves for a specific pool
function Botega.fetchReserves(poolAddress, callback)
  Logger.debug("Fetching reserves", { pool = poolAddress })

  ao.send({
    Target = poolAddress,
    Action = Constants.API.BOTEGA.GET_RESERVES
  }).onReply(function(response)
    if response.Error then
      Logger.error("Failed to fetch reserves", {
        pool = poolAddress,
        error = response.Error
      })
      callback(nil, response.Error)
      return
    end

    -- Get token pair to match reserves with token IDs
    Botega.fetchPair(poolAddress, function(pair, pairErr)
      if not pair then
        callback(nil, pairErr)
        return
      end

      -- Extract reserve data, mapping to token A and B
      callback({
        reserveA = response[pair.tokenA] or "0",
        reserveB = response[pair.tokenB] or "0",
        tokenA = pair.tokenA,
        tokenB = pair.tokenB
      })
    end)
  end)
end

-- Fetch fee information for a pool
function Botega.fetchFeePercentage(poolAddress, hasDiscount, callback)
  Logger.debug("Fetching fee percentage", { pool = poolAddress })

  ao.send({
    Target = poolAddress,
    Action = Constants.API.BOTEGA.GET_FEE_PERCENTAGE,
    Tags = {
      ["Has-Fee-Discount"] = hasDiscount and "true" or "false"
    }
  }).onReply(function(response)
    if response.Error then
      Logger.error("Failed to fetch fee percentage", {
        pool = poolAddress,
        error = response.Error
      })
      callback(nil, response.Error)
    else
      callback({
        lpFeePercentage = tonumber(response["LP-Fee-Percentage"]) or 0,
        protocolFeePercentage = tonumber(response["Protocol-Fee-Percentage"]) or 0,
        totalFeePercentage = tonumber(response["Fee-Percentage"]) or 0
      })
    end
  end)
end

-- Get expected output directly from Botega API
function Botega.getSwapOutput(poolAddress, tokenIn, amountIn, userAddress, callback)
  Logger.debug("Getting swap output", {
    pool = poolAddress,
    tokenIn = tokenIn,
    amountIn = amountIn
  })

  ao.send({
    Target = poolAddress,
    Action = Constants.API.BOTEGA.GET_SWAP_OUTPUT,
    Tags = {
      Token = tokenIn,
      Quantity = tostring(amountIn),
      Swapper = userAddress or "0000000000000000000000000000000000000000000" -- Default value required by API
    }
  }).onReply(function(response)
    if response.Error then
      Logger.error("Failed to get swap output", {
        pool = poolAddress,
        error = response.Error
      })
      callback(nil, response.Error)
    else
      callback({
        amount_out = response.Output,
        amountInAfterFees = response["Quantity-After-Fees"],
        lpFee = response["LP-Fee-Quantity"],
        protocolFee = response["Protocol-Fee-Quantity"],
        hasDiscount = response["Has-Fee-Discount"] == "true"
      })
    end
  end)
end

-- Normalize pool data from Botega format to our standard format
function Botega.normalizePoolData(poolAddress, poolInfo)
  if not poolInfo then
    return nil, "Missing required pool data"
  end

  -- Extract token information directly from poolInfo
  local tokenA = {
    id = poolInfo.TokenA,
    symbol = "",
    name = "",
    decimals = tonumber(poolInfo.Denomination),
    logo_url = ""
  }

  local tokenB = {
    id = poolInfo.TokenB,
    symbol = "",
    name = "",
    decimals = tonumber(poolInfo.Denomination),
    logo_url = ""
  }

  -- Extract fee directly from poolInfo
  local feeBps = tonumber(poolInfo.FeeBps) or 25 -- Default to 25 bps if not found

  -- Normalize pool format
  local normalizedPool = {
    id = poolAddress,
    source = Constants.SOURCE.BOTEGA,
    token_a_id = tokenA.id,
    token_b_id = tokenB.id,
    fee_bps = feeBps,
    status = "active", -- Default to active, can be updated based on AMM status

    -- Additional Botega-specific data
    name = poolInfo.Name,
    ticker = poolInfo.Ticker,
    logo = poolInfo.Logo,
    denomination = poolInfo.Denomination,
    totalSupply = poolInfo.TotalSupply,
    type = poolInfo.Type
  }

  return {
    pool = normalizedPool,
    tokens = { tokenA, tokenB }
  }
end

-- Collect data for a single Botega pool
function Botega.collectPoolData(poolAddress, collector, callback)
  Logger.info("Collecting data for pool", { pool = poolAddress })

  -- Get basic pool info only
  Botega.fetchPoolInfo(poolAddress, collector, function(poolInfo, infoErr)
    if not poolInfo then
      callback(nil, infoErr)
      return
    end

    -- Normalize the data using only poolInfo
    local normalizedData = Botega.normalizePoolData(poolAddress, poolInfo.Tags)
    if not normalizedData then
      -- Remove from pending collections
      collector.pendingCollections[poolAddress] = nil
      callback(nil, "Failed to normalize pool data")
      return
    end

    -- Set empty reserves placeholder (will need a separate update if reserves are needed)
    normalizedData.reserves = {
      reserve_a = "0",
      reserve_b = "0"
    }

    -- Remove from pending collections
    collector.pendingCollections[poolAddress] = nil
    callback(normalizedData)
  end)
end

-- Collect data from multiple Botega pools
function Botega.collectAllPoolsData(poolAddresses, collector, finalCallback)
  local results = {
    pools = {},
    tokens = {},
    reserves = {},
    errors = {}
  }

  local pendingPools = #poolAddresses
  if pendingPools == 0 then
    finalCallback(results)
    return
  end

  for _, poolAddress in ipairs(poolAddresses) do
    Botega.collectPoolData(poolAddress, collector, function(poolData, err)
      pendingPools = pendingPools - 1

      if poolData and not err then
        table.insert(results.pools, poolData.pool)

        -- Add tokens
        for _, token in ipairs(poolData.tokens) do
          results.tokens[token.id] = token
        end

        -- Add reserves
        results.reserves[poolAddress] = poolData.reserves
      else
        results.errors[poolAddress] = err
        Logger.warn("Failed to collect data for pool", { pool = poolAddress, error = err })
      end

      -- Check if all pools have been processed
      if pendingPools == 0 then
        -- Convert tokens table to array
        local tokensArray = {}
        for _, token in pairs(results.tokens) do
          table.insert(tokensArray, token)
        end
        results.tokens = tokensArray

        Logger.info("Collected Botega data", {
          poolCount = #results.pools,
          tokenCount = #results.tokens,
          errorCount = Utils.tableSize(results.errors)
        })

        finalCallback(results)
      end
    end)
  end
end

-- Execute a swap via Botega
function Botega.executeSwap(poolAddress, tokenIn, amountIn, minAmountOut, userAddress, callback)
  Logger.info("Executing swap", {
    pool = poolAddress,
    tokenIn = tokenIn,
    amountIn = amountIn,
    minAmountOut = minAmountOut
  })

  -- For Botega, we send a Credit-Notice with special tags
  local request = {
    Target = poolAddress,
    Action = Constants.API.BOTEGA.CREDIT_NOTICE,
    From = tokenIn,
    Tags = {
      Sender = userAddress or "user_address",
      Quantity = tostring(amountIn),
      ["X-Action"] = "Swap"
    }
  }

  -- Add minimum expected output if specified
  if minAmountOut then
    request.Tags["X-Expected-Min-Output"] = tostring(minAmountOut)
  end

  ao.send(request).onReply(function(response)
    if response.Error then
      Logger.error("Failed to execute swap", {
        pool = poolAddress,
        error = response.Error
      })
      callback(nil, response.Error)
    else
      callback({
        action = response.Action,
        recipient = response.Recipient,
        quantity = response.Quantity,
        swapAction = response["X-Action"],
        reservesTokenA = response["X-Reserves-Token-A"],
        reservesTokenB = response["X-Reserves-Token-B"],
        feeBps = response["X-Fee-Bps"],
        tokenA = response["X-Token-A"],
        tokenB = response["X-Token-B"],
        timestamp = response["X-Swap-Timestamp"]
      })
    end
  end)
end

-- Get user balance in pool
function Botega.getBalance(poolAddress, userAddress, callback)
  Logger.debug("Getting balance", { pool = poolAddress, user = userAddress })

  local request = {
    Target = poolAddress,
    Action = Constants.API.BOTEGA.BALANCE,
    Tags = {}
  }

  if userAddress then
    request.Tags.Recipient = userAddress
  end

  ao.send(request).onReply(function(response)
    if response.Error then
      Logger.error("Failed to get balance", {
        pool = poolAddress,
        error = response.Error
      })
      callback(nil, response.Error)
    else
      callback({
        lpBalance = response.Balance,
        ticker = response.Ticker
      })
    end
  end)
end

-- Get total supply of LP tokens
function Botega.getTotalSupply(poolAddress, callback)
  Logger.debug("Getting total supply", { pool = poolAddress })

  ao.send({
    Target = poolAddress,
    Action = Constants.API.BOTEGA.TOTAL_SUPPLY
  }).onReply(function(response)
    if response.Error then
      Logger.error("Failed to get total supply", {
        pool = poolAddress,
        error = response.Error
      })
      callback(nil, response.Error)
    else
      callback({
        totalSupply = response["Total-Supply"],
        ticker = response.Ticker
      })
    end
  end)
end

-- Register as a subscriber to receive notifications
function Botega.registerSubscriber(poolAddress, callback)
  Logger.info("Registering as subscriber", { pool = poolAddress })

  ao.send({
    Target = poolAddress,
    Action = "Register-Subscriber"
  }).onReply(function(response)
    if response.Error then
      Logger.error("Failed to register as subscriber", {
        pool = poolAddress,
        error = response.Error
      })
      callback(nil, response.Error)
    else
      callback({
        whitelisted = response.Whitelisted == "true"
      })
    end
  end)
end

-- Subscribe to specific notification topics
function Botega.subscribeToTopics(poolAddress, topics, callback)
  Logger.info("Subscribing to topics", { pool = poolAddress, topics = topics })

  local topicsJson = Utils.jsonEncode(topics)

  ao.send({
    Target = poolAddress,
    Action = "Subscribe-To-Topics",
    Topics = topicsJson
  }).onReply(function(response)
    if response.Error then
      Logger.error("Failed to subscribe to topics", {
        pool = poolAddress,
        error = response.Error
      })
      callback(nil, response.Error)
    else
      callback({
        updatedTopics = response["Updated-Topics"]
      })
    end
  end)
end

-- Discover Botega pools (this would require some external source or registry)
function Botega.discoverPools(callback)
  -- In a real implementation, this might query a registry process or use a predefined list
  -- For now, we'll return a placeholder message
  Logger.warn("Pool discovery not implemented, use a predefined list of pool addresses")
  callback(nil, "Pool discovery requires external configuration")
end

return Botega
