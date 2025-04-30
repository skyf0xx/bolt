local BigDecimal = require('dex.utils.big_decimal')
local Constants = require('dex.utils.constants')
local Logger = require('dex.utils.logger')
Logger = Logger.createLogger("BotegaCollector")
local Utils = require('dex.utils.utils')

local Botega = {}

-- Fetch basic information about a Botega pool
function Botega.fetchPoolInfo(poolAddress, callback)
  Logger.debug("Fetching pool info", { pool = poolAddress })

  ao.send({
    Target = poolAddress,
    Action = Constants.API.BOTEGA.INFO
  }).onReply(function(response)
    if not response or response.Error then
      Logger.error("Failed to fetch pool info", {
        pool = poolAddress,
        error = response and response.Error or "No response"
      })
      callback(nil, response and response.Error or "No response from pool")
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
    if not response or response.Error then
      Logger.error("Failed to fetch token pair", {
        pool = poolAddress,
        error = response and response.Error or "No response"
      })
      callback(nil, response and response.Error or "No response from pool")
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
    if not response or response.Error then
      Logger.error("Failed to fetch reserves", {
        pool = poolAddress,
        error = response and response.Error or "No response"
      })
      callback(nil, response and response.Error or "No response from pool")
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
    if not response or response.Error then
      Logger.error("Failed to fetch fee percentage", {
        pool = poolAddress,
        error = response and response.Error or "No response"
      })
      callback(nil, response and response.Error or "No response from pool")
    else
      callback({
        lpFeePercentage = tonumber(response["LP-Fee-Percentage"]) or 0,
        protocolFeePercentage = tonumber(response["Protocol-Fee-Percentage"]) or 0,
        totalFeePercentage = tonumber(response["Fee-Percentage"]) or 0
      })
    end
  end)
end

-- Calculate expected output amount for a swap in Botega
function Botega.calculateOutputAmount(amountIn, reserveIn, reserveOut, feePercentage)
  -- Using Botega's two-step formula:
  -- Step 1: Fee deduction
  local feeMultiplier = 1 - (feePercentage / 100)
  local amountInAfterFees = BigDecimal.multiply(
    BigDecimal.new(amountIn),
    BigDecimal.new(tostring(feeMultiplier * Constants.NUMERIC.BASIS_POINTS_MULTIPLIER))
  )
  amountInAfterFees = BigDecimal.divide(
    amountInAfterFees,
    BigDecimal.new(Constants.NUMERIC.BASIS_POINTS_MULTIPLIER)
  )

  -- Step 2: Output calculation
  local k = BigDecimal.multiply(
    BigDecimal.new(reserveIn),
    BigDecimal.new(reserveOut)
  )

  local newReserveIn = BigDecimal.add(
    BigDecimal.new(reserveIn),
    amountInAfterFees
  )

  local newReserveOut = BigDecimal.divide(k, newReserveIn)

  local amountOut = BigDecimal.subtract(
    BigDecimal.new(reserveOut),
    newReserveOut
  )

  return amountOut
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
      Swapper = userAddress or "user_address" -- Default value
    }
  }).onReply(function(response)
    if not response or response.Error then
      Logger.error("Failed to get swap output", {
        pool = poolAddress,
        error = response and response.Error or "No response"
      })
      callback(nil, response and response.Error or "No response from pool")
    else
      callback({
        amountOut = response.Output,
        amountInAfterFees = response["Quantity-After-Fees"],
        lpFee = response["LP-Fee-Quantity"],
        protocolFee = response["Protocol-Fee-Quantity"],
        hasDiscount = response["Has-Fee-Discount"] == "true"
      })
    end
  end)
end

-- Normalize pool data from Botega format to our standard format
function Botega.normalizePoolData(poolAddress, poolInfo, pairInfo, feeInfo)
  if not poolInfo or not pairInfo then
    return nil, "Missing required pool data"
  end

  -- Convert fee from percentage to basis points
  local feeBps = 0
  if poolInfo.FeeBps then
    feeBps = tonumber(poolInfo.FeeBps) or feeBps
  elseif feeInfo and feeInfo.totalFeePercentage then
    feeBps = math.floor(feeInfo.totalFeePercentage * 100) -- Convert percentage to basis points
  end

  -- Normalize pool format
  local normalizedPool = {
    id = poolAddress,
    source = Constants.SOURCE.BOTEGA,
    token_a_id = pairInfo.tokenA,
    token_b_id = pairInfo.tokenB,
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

  -- Extract token information (limited, need to be supplemented later)
  local tokenA = {
    id = pairInfo.tokenA,
    symbol = "", -- Not available in basic info, need separate call
    name = "",
    decimals = tonumber(poolInfo.Denomination) or Constants.NUMERIC.DECIMALS,
    logo_url = ""
  }

  local tokenB = {
    id = pairInfo.tokenB,
    symbol = "", -- Not available in basic info, need separate call
    name = "",
    decimals = tonumber(poolInfo.Denomination) or Constants.NUMERIC.DECIMALS,
    logo_url = ""
  }

  return {
    pool = normalizedPool,
    tokens = { tokenA, tokenB }
  }
end

-- Collect data for a single Botega pool
function Botega.collectPoolData(poolAddress, callback)
  Logger.info("Collecting data for pool", { pool = poolAddress })

  -- Get basic pool info
  Botega.fetchPoolInfo(poolAddress, function(poolInfo, infoErr)
    if not poolInfo then
      callback(nil, infoErr)
      return
    end

    -- Get token pair info
    Botega.fetchPair(poolAddress, function(pairInfo, pairErr)
      if not pairInfo then
        callback(nil, pairErr)
        return
      end

      -- Get fee info
      Botega.fetchFeePercentage(poolAddress, false, function(feeInfo, feeErr)
        if not feeInfo then
          Logger.warn("Failed to fetch fee info, using default", { error = feeErr })
          -- Continue without fee info, will use default or from poolInfo
        end

        -- Normalize the data
        local normalizedData = Botega.normalizePoolData(poolAddress, poolInfo, pairInfo, feeInfo)
        if not normalizedData then
          callback(nil, "Failed to normalize pool data")
          return
        end

        -- Get reserves
        Botega.fetchReserves(poolAddress, function(reserves, reservesErr)
          if reserves then
            normalizedData.reserves = {
              reserve_a = reserves.reserveA,
              reserve_b = reserves.reserveB
            }
          else
            Logger.warn("Failed to fetch reserves", { error = reservesErr })
            normalizedData.reserves = {
              reserve_a = "0",
              reserve_b = "0"
            }
          end

          callback(normalizedData)
        end)
      end)
    end)
  end)
end

-- Collect data from multiple Botega pools
function Botega.collectAllPoolsData(poolAddresses, finalCallback)
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
    Botega.collectPoolData(poolAddress, function(poolData, err)
      pendingPools = pendingPools - 1

      if poolData then
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
    if not response or response.Error then
      Logger.error("Failed to execute swap", {
        pool = poolAddress,
        error = response and response.Error or "No response"
      })
      callback(nil, response and response.Error or "No response from pool")
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
    if not response or response.Error then
      Logger.error("Failed to get balance", {
        pool = poolAddress,
        error = response and response.Error or "No response"
      })
      callback(nil, response and response.Error or "No response from pool")
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
    if not response or response.Error then
      Logger.error("Failed to get total supply", {
        pool = poolAddress,
        error = response and response.Error or "No response"
      })
      callback(nil, response and response.Error or "No response from pool")
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
    if not response or response.Error then
      Logger.error("Failed to register as subscriber", {
        pool = poolAddress,
        error = response and response.Error or "No response"
      })
      callback(nil, response and response.Error or "No response from pool")
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
    if not response or response.Error then
      Logger.error("Failed to subscribe to topics", {
        pool = poolAddress,
        error = response and response.Error or "No response"
      })
      callback(nil, response and response.Error or "No response from pool")
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
