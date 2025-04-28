local BigDecimal = require('arbitrage.utils.big_decimal')
local Constants = require('arbitrage.constants')
local Logger = require('arbitrage.logger').createLogger("BotegaCollector")
local Utils = require('arbitrage.utils')

local Botega = {}

-- Fetch basic information about a Botega pool
function Botega.fetchPoolInfo(poolAddress)
  Logger.debug("Fetching pool info", { pool = poolAddress })

  local response = Send({
    Target = poolAddress,
    Action = Constants.API.BOTEGA.INFO
  })

  if not response or response.Error then
    Logger.error("Failed to fetch pool info", {
      pool = poolAddress,
      error = response and response.Error or "No response"
    })
    return nil, response and response.Error or "No response from pool"
  end

  return response
end

-- Fetch token pair information for a pool
function Botega.fetchPair(poolAddress)
  Logger.debug("Fetching token pair", { pool = poolAddress })

  local response = Send({
    Target = poolAddress,
    Action = Constants.API.BOTEGA.GET_PAIR
  })

  if not response or response.Error then
    Logger.error("Failed to fetch token pair", {
      pool = poolAddress,
      error = response and response.Error or "No response"
    })
    return nil, response and response.Error or "No response from pool"
  end

  return {
    tokenA = response["Token-A"],
    tokenB = response["Token-B"]
  }
end

-- Fetch reserves for a specific pool
function Botega.fetchReserves(poolAddress)
  Logger.debug("Fetching reserves", { pool = poolAddress })

  local response = Send({
    Target = poolAddress,
    Action = Constants.API.BOTEGA.GET_RESERVES
  })

  if not response or response.Error then
    Logger.error("Failed to fetch reserves", {
      pool = poolAddress,
      error = response and response.Error or "No response"
    })
    return nil, response and response.Error or "No response from pool"
  end

  -- Get token pair to match reserves with token IDs
  local pair, pairErr = Botega.fetchPair(poolAddress)
  if not pair then
    return nil, pairErr
  end

  -- Extract reserve data, mapping to token A and B
  return {
    reserveA = response[pair.tokenA] or "0",
    reserveB = response[pair.tokenB] or "0",
    tokenA = pair.tokenA,
    tokenB = pair.tokenB
  }
end

-- Fetch fee information for a pool
function Botega.fetchFeePercentage(poolAddress, hasDiscount)
  Logger.debug("Fetching fee percentage", { pool = poolAddress })

  local response = Send({
    Target = poolAddress,
    Action = Constants.API.BOTEGA.GET_FEE_PERCENTAGE,
    Tags = {
      ["Has-Fee-Discount"] = hasDiscount and "true" or "false"
    }
  })

  if not response or response.Error then
    Logger.error("Failed to fetch fee percentage", {
      pool = poolAddress,
      error = response and response.Error or "No response"
    })
    return nil, response and response.Error or "No response from pool"
  end

  return {
    lpFeePercentage = tonumber(response["LP-Fee-Percentage"]) or 0,
    protocolFeePercentage = tonumber(response["Protocol-Fee-Percentage"]) or 0,
    totalFeePercentage = tonumber(response["Fee-Percentage"]) or 0
  }
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
function Botega.getSwapOutput(poolAddress, tokenIn, amountIn, userAddress)
  Logger.debug("Getting swap output", {
    pool = poolAddress,
    tokenIn = tokenIn,
    amountIn = amountIn
  })

  local response = Send({
    Target = poolAddress,
    Action = Constants.API.BOTEGA.GET_SWAP_OUTPUT,
    Tags = {
      Token = tokenIn,
      Quantity = tostring(amountIn),
      Swapper = userAddress or "user_address" -- Default value
    }
  })

  if not response or response.Error then
    Logger.error("Failed to get swap output", {
      pool = poolAddress,
      error = response and response.Error or "No response"
    })
    return nil, response and response.Error or "No response from pool"
  end

  return {
    amountOut = response.Output,
    amountInAfterFees = response["Quantity-After-Fees"],
    lpFee = response["LP-Fee-Quantity"],
    protocolFee = response["Protocol-Fee-Quantity"],
    hasDiscount = response["Has-Fee-Discount"] == "true"
  }
end

-- Normalize pool data from Botega format to our standard format
function Botega.normalizePoolData(poolAddress, poolInfo, pairInfo, feeInfo)
  if not poolInfo or not pairInfo then
    return nil, "Missing required pool data"
  end

  -- Convert fee from percentage to basis points
  local feeBps = 0
  if poolInfo.FeeBps then
    feeBps = tonumber(poolInfo.FeeBps)
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
function Botega.collectPoolData(poolAddress)
  Logger.info("Collecting data for pool", { pool = poolAddress })

  -- Get basic pool info
  local poolInfo, infoErr = Botega.fetchPoolInfo(poolAddress)
  if not poolInfo then
    return nil, infoErr
  end

  -- Get token pair info
  local pairInfo, pairErr = Botega.fetchPair(poolAddress)
  if not pairInfo then
    return nil, pairErr
  end

  -- Get fee info
  local feeInfo, feeErr = Botega.fetchFeePercentage(poolAddress)
  if not feeInfo then
    Logger.warn("Failed to fetch fee info, using default", { error = feeErr })
    -- Continue without fee info, will use default or from poolInfo
  end

  -- Normalize the data
  local normalizedData = Botega.normalizePoolData(poolAddress, poolInfo, pairInfo, feeInfo)
  if not normalizedData then
    return nil, "Failed to normalize pool data"
  end

  -- Get reserves
  local reserves, reservesErr = Botega.fetchReserves(poolAddress)
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

  return normalizedData
end

-- Collect data from multiple Botega pools
function Botega.collectAllPoolsData(poolAddresses)
  local results = {
    pools = {},
    tokens = {},
    reserves = {},
    errors = {}
  }

  for _, poolAddress in ipairs(poolAddresses) do
    local poolData, err = Botega.collectPoolData(poolAddress)

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
  end

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

  return results
end

-- Execute a swap via Botega
function Botega.executeSwap(poolAddress, tokenIn, amountIn, minAmountOut, userAddress)
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

  local response = Send(request)

  if not response or response.Error then
    Logger.error("Failed to execute swap", {
      pool = poolAddress,
      error = response and response.Error or "No response"
    })
    return nil, response and response.Error or "No response from pool"
  end

  return {
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
  }
end

-- Get user balance in pool
function Botega.getBalance(poolAddress, userAddress)
  Logger.debug("Getting balance", { pool = poolAddress, user = userAddress })

  local request = {
    Target = poolAddress,
    Action = Constants.API.BOTEGA.BALANCE,
    Tags = {}
  }

  if userAddress then
    request.Tags.Recipient = userAddress
  end

  local response = Send(request)

  if not response or response.Error then
    Logger.error("Failed to get balance", {
      pool = poolAddress,
      error = response and response.Error or "No response"
    })
    return nil, response and response.Error or "No response from pool"
  end

  return {
    lpBalance = response.Balance,
    ticker = response.Ticker
  }
end

-- Get total supply of LP tokens
function Botega.getTotalSupply(poolAddress)
  Logger.debug("Getting total supply", { pool = poolAddress })

  local response = Send({
    Target = poolAddress,
    Action = Constants.API.BOTEGA.TOTAL_SUPPLY
  })

  if not response or response.Error then
    Logger.error("Failed to get total supply", {
      pool = poolAddress,
      error = response and response.Error or "No response"
    })
    return nil, response and response.Error or "No response from pool"
  end

  return {
    totalSupply = response["Total-Supply"],
    ticker = response.Ticker
  }
end

-- Register as a subscriber to receive notifications
function Botega.registerSubscriber(poolAddress)
  Logger.info("Registering as subscriber", { pool = poolAddress })

  local response = Send({
    Target = poolAddress,
    Action = "Register-Subscriber"
  })

  if not response or response.Error then
    Logger.error("Failed to register as subscriber", {
      pool = poolAddress,
      error = response and response.Error or "No response"
    })
    return nil, response and response.Error or "No response from pool"
  end

  return {
    whitelisted = response.Whitelisted == "true"
  }
end

-- Subscribe to specific notification topics
function Botega.subscribeToTopics(poolAddress, topics)
  Logger.info("Subscribing to topics", { pool = poolAddress, topics = topics })

  local topicsJson = Utils.jsonEncode(topics)

  local response = Send({
    Target = poolAddress,
    Action = "Subscribe-To-Topics",
    Topics = topicsJson
  })

  if not response or response.Error then
    Logger.error("Failed to subscribe to topics", {
      pool = poolAddress,
      error = response and response.Error or "No response"
    })
    return nil, response and response.Error or "No response from pool"
  end

  return {
    updatedTopics = response["Updated-Topics"]
  }
end

-- Discover Botega pools (this would require some external source or registry)
function Botega.discoverPools()
  -- In a real implementation, this might query a registry process or use a predefined list
  -- For now, we'll return a placeholder message
  Logger.warn("Pool discovery not implemented, use a predefined list of pool addresses")
  return nil, "Pool discovery requires external configuration"
end

return Botega
