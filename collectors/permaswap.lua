local BigDecimal = require('arbitrage.utils.big_decimal')
local Constants = require('arbitrage.constants')
local Logger = require('arbitrage.logger').createLogger("PermaswapCollector")
local Utils = require('arbitrage.utils')

local Permaswap = {}

-- Fetch basic information about a Permaswap pool
function Permaswap.fetchPoolInfo(poolAddress)
  Logger.debug("Fetching pool info", { pool = poolAddress })

  local response = Send({
    Target = poolAddress,
    Action = Constants.API.PERMASWAP.INFO
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

-- Fetch reserves for a specific pool
function Permaswap.fetchReserves(poolAddress)
  Logger.debug("Fetching reserves", { pool = poolAddress })

  -- For Permaswap, reserves are included in the Info response
  local poolInfo, err = Permaswap.fetchPoolInfo(poolAddress)

  if not poolInfo then
    return nil, err
  end

  -- Extract reserve data from pool info
  local reserves = {
    reserveX = poolInfo.PX or "0",
    reserveY = poolInfo.PY or "0"
  }

  return reserves
end

-- Calculate expected output amount for a swap in Permaswap
function Permaswap.calculateOutputAmount(amountIn, reserveIn, reserveOut, feeBps)
  -- Using Permaswap's formula:
  -- amountOut = (amountIn * (10000 - fee) * reserveOut) / ((10000 * reserveIn) + (amountIn * (10000 - fee)))

  return BigDecimal.getOutputAmount(
    BigDecimal.new(amountIn),
    BigDecimal.new(reserveIn),
    BigDecimal.new(reserveOut),
    feeBps
  )
end

-- Get expected output directly from Permaswap API
function Permaswap.getAmountOut(poolAddress, tokenIn, amountIn)
  Logger.debug("Getting amount out", {
    pool = poolAddress,
    tokenIn = tokenIn,
    amountIn = amountIn
  })

  local response = Send({
    Target = poolAddress,
    Action = Constants.API.PERMASWAP.GET_AMOUNT_OUT,
    TokenIn = tokenIn,
    AmountIn = tostring(amountIn)
  })

  if not response or response.Error then
    Logger.error("Failed to get amount out", {
      pool = poolAddress,
      error = response and response.Error or "No response"
    })
    return nil, response and response.Error or "No response from pool"
  end

  return {
    amountOut = response.AmountOut,
    tokenIn = response.TokenIn,
    tokenOut = response.TokenOut,
    fee = {
      issuer = response.IssuerFee or "0",
      holder = response.HolderFee or "0",
      pool = response.PoolFee or "0"
    }
  }
end

-- Normalize pool data from Permaswap format to our standard format
function Permaswap.normalizePoolData(poolAddress, poolData)
  if not poolData then
    return nil, "No pool data provided"
  end

  -- Extract token information
  local tokenX = {
    id = poolData.X,
    symbol = poolData.SymbolX,
    name = poolData.FullNameX,
    decimals = tonumber(poolData.DecimalX) or Constants.NUMERIC.DECIMALS,
    logo_url = poolData.LogoX or ""
  }

  local tokenY = {
    id = poolData.Y,
    symbol = poolData.SymbolY,
    name = poolData.FullNameY,
    decimals = tonumber(poolData.DecimalY) or Constants.NUMERIC.DECIMALS,
    logo_url = poolData.LogoY or ""
  }

  -- Normalize pool format
  local normalizedPool = {
    id = poolAddress,
    source = Constants.SOURCE.PERMASWAP,
    token_a_id = tokenX.id,
    token_b_id = tokenY.id,
    fee_bps = tonumber(poolData.Fee) or 0,
    status = poolData.PoolStatus == "certified" and "active" or "inactive",

    -- Additional Permaswap-specific data
    name = poolData.Name,
    totalSupply = poolData.TotalSupply
  }

  return {
    pool = normalizedPool,
    tokens = { tokenX, tokenY }
  }
end

-- Collect data for a single Permaswap pool
function Permaswap.collectPoolData(poolAddress)
  Logger.info("Collecting data for pool", { pool = poolAddress })

  local poolInfo, err = Permaswap.fetchPoolInfo(poolAddress)

  if not poolInfo then
    return nil, err
  end

  local normalizedData = Permaswap.normalizePoolData(poolAddress, poolInfo)

  if not normalizedData then
    return nil, "Failed to normalize pool data"
  end

  -- Add reserve data
  normalizedData.reserves = {
    reserve_a = poolInfo.PX or "0",
    reserve_b = poolInfo.PY or "0"
  }

  return normalizedData
end

-- Collect data from multiple Permaswap pools
function Permaswap.collectAllPoolsData(poolAddresses)
  local results = {
    pools = {},
    tokens = {},
    reserves = {},
    errors = {}
  }

  for _, poolAddress in ipairs(poolAddresses) do
    local poolData, err = Permaswap.collectPoolData(poolAddress)

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

  Logger.info("Collected Permaswap data", {
    poolCount = #results.pools,
    tokenCount = #results.tokens,
    errorCount = Utils.tableSize(results.errors)
  })

  return results
end

-- Execute a swap via Permaswap
function Permaswap.executeSwap(poolAddress, tokenIn, tokenOut, amountIn, minAmountOut)
  Logger.info("Executing swap", {
    pool = poolAddress,
    tokenIn = tokenIn,
    tokenOut = tokenOut,
    amountIn = amountIn,
    minAmountOut = minAmountOut
  })

  local request = {
    Target = poolAddress,
    Action = Constants.API.PERMASWAP.REQUEST_ORDER,
    TokenIn = tokenIn,
    TokenOut = tokenOut,
    AmountIn = tostring(amountIn)
  }

  -- Add minimum amount out if specified
  if minAmountOut then
    request.AmountOut = tostring(minAmountOut)
  end

  local response = Send(request)

  if not response or response.Error then
    Logger.error("Failed to execute swap", {
      pool = poolAddress,
      error = response and response.Error or "No response"
    })
    return nil, response and response.Error or "No response from pool"
  end

  -- Parse the response data
  local orderData
  if response.Data then
    local status, data = pcall(function() return Utils.jsonDecode(response.Data) end)
    if status then
      orderData = data
    else
      Logger.warn("Failed to parse order data", { error = data })
    end
  end

  return {
    noteId = response.NoteID,
    noteSettle = response.NoteSettle,
    noteSettleVersion = response.NoteSettleVersion,
    orderData = orderData
  }
end

-- Get order status
function Permaswap.getOrderStatus(poolAddress, orderId)
  Logger.debug("Getting order status", { pool = poolAddress, orderId = orderId })

  local response = Send({
    Target = poolAddress,
    Action = Constants.API.PERMASWAP.GET_ORDER,
    OrderId = orderId
  })

  if not response or response.Error then
    Logger.error("Failed to get order status", {
      pool = poolAddress,
      orderId = orderId,
      error = response and response.Error or "No response"
    })
    return nil, response and response.Error or "No response from pool"
  end

  -- Parse the order data
  local orderData
  if response.Data then
    local status, data = pcall(function() return Utils.jsonDecode(response.Data) end)
    if status then
      orderData = data
    else
      Logger.warn("Failed to parse order data", { error = data })
    end
  end

  return orderData
end

-- Get user balance in pool
function Permaswap.getBalance(poolAddress, userAddress)
  Logger.debug("Getting balance", { pool = poolAddress, user = userAddress })

  local request = {
    Target = poolAddress,
    Action = Constants.API.PERMASWAP.BALANCE
  }

  if userAddress then
    request.Account = userAddress
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
    balanceX = response.BalanceX,
    balanceY = response.BalanceY,
    lpBalance = response.Balance,
    totalSupply = response.TotalSupply,
    account = response.Account
  }
end

-- Discover Permaswap pools (this would require some external source or registry)
function Permaswap.discoverPools()
  -- In a real implementation, this might query a registry process or use a predefined list
  -- For now, we'll return a placeholder message
  Logger.warn("Pool discovery not implemented, use a predefined list of pool addresses")
  return nil, "Pool discovery requires external configuration"
end

return Permaswap
