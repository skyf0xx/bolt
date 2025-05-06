local BigDecimal = require('dex.utils.big_decimal')
local Constants = require('dex.utils.constants')
local Logger = require('dex.utils.logger')
Logger = Logger.createLogger("PermaswapCollector")
local Utils = require('dex.utils.utils')

local Permaswap = {}

-- Fetch basic information about a Permaswap pool
function Permaswap.fetchPoolInfo(poolAddress, callback)
  Logger.debug("Fetching pool info", { pool = poolAddress })

  ao.send({
    Target = poolAddress,
    Action = Constants.API.PERMASWAP.INFO
  }).onReply(function(response)
    if response.Error then
      Logger.error("Failed to fetch pool info", {
        pool = poolAddress,
        error = response.Error
      })
      callback(nil, response.Error)
    else
      callback(response)
    end
  end)
end

-- Fetch reserves for a specific pool
function Permaswap.fetchReserves(poolAddress, callback)
  Logger.debug("Fetching reserves", { pool = poolAddress })

  -- For Permaswap, reserves are included in the Info response
  Permaswap.fetchPoolInfo(poolAddress, function(poolInfo, err)
    if not poolInfo then
      callback(nil, err)
      return
    end

    -- Extract reserve data from pool info
    local reserves = {
      reserveX = poolInfo.PX or "0",
      reserveY = poolInfo.PY or "0"
    }

    callback(reserves)
  end)
end

-- Get expected output directly from Permaswap API
function Permaswap.requestOrder(poolAddress, tokenIn, tokenOut, amountIn, callback)
  Logger.debug("Getting amount out", {
    pool = poolAddress,
    tokenIn = tokenIn,
    tokenOut = tokenOut,
    amountIn = amountIn,

  })

  ao.send({
    Target = poolAddress,
    Action = Constants.API.PERMASWAP.REQUEST_ORDER,
    TokenIn = tokenIn,
    TokenOut = tokenOut,
    AmountIn = tostring(amountIn),
    --AmountOut = "1" --force an error if the pool is empty. TODO: deal with error
  }).onReply(function(response)
    if response.Error then
      Logger.error("Failed to get amount out", {
        pool = poolAddress,
        error = response.Error
      })
      callback(nil, response.Error)
    else
      callback({
        amountOut = response.Amount,
        outputAmount = response.Amount,
        tokenIn = response.HolderAssetID,
        tokenOut = response.AssetID,
        fee = {
          issuer = response.IssuerFee or "0",
          holder = response.HolderFee or "0",
          pool = response.PoolFee or "0"
        }
      })
    end
  end)
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
function Permaswap.collectPoolData(poolAddress, callback)
  Logger.info("Collecting data for pool", { pool = poolAddress })

  Permaswap.fetchPoolInfo(poolAddress, function(poolInfo, err)
    if not poolInfo then
      callback(nil, err)
      return
    end

    local normalizedData = Permaswap.normalizePoolData(poolAddress, poolInfo)

    if not normalizedData then
      callback(nil, "Failed to normalize pool data")
      return
    end

    -- Add reserve data
    normalizedData.reserves = {
      reserve_a = poolInfo.PX or "0",
      reserve_b = poolInfo.PY or "0"
    }

    callback(normalizedData)
  end)
end

-- Collect data from multiple Permaswap pools
function Permaswap.collectAllPoolsData(poolAddresses, finalCallback)
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
    Permaswap.collectPoolData(poolAddress, function(poolData, err)
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

        Logger.info("Collected Permaswap data", {
          poolCount = #results.pools,
          tokenCount = #results.tokens,
          errorCount = Utils.tableSize(results.errors)
        })

        finalCallback(results)
      end
    end)
  end
end

-- Execute a swap via Permaswap
function Permaswap.executeSwap(poolAddress, tokenIn, tokenOut, amountIn, minAmountOut, callback)
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

  ao.send(request).onReply(function(response)
    if response.Error then
      Logger.error("Failed to execute swap", {
        pool = poolAddress,
        error = response.Error
      })
      callback(nil, response.Error)
      return
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

    callback({
      noteId = response.NoteID,
      noteSettle = response.NoteSettle,
      noteSettleVersion = response.NoteSettleVersion,
      orderData = orderData
    })
  end)
end

-- Get order status
function Permaswap.getOrderStatus(poolAddress, orderId, callback)
  Logger.debug("Getting order status", { pool = poolAddress, orderId = orderId })

  ao.send({
    Target = poolAddress,
    Action = Constants.API.PERMASWAP.GET_ORDER,
    OrderId = orderId
  }).onReply(function(response)
    if response.Error then
      Logger.error("Failed to get order status", {
        pool = poolAddress,
        orderId = orderId,
        error = response.Error
      })
      callback(nil, response.Error)
      return
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

    callback(orderData)
  end)
end

-- Get user balance in pool
function Permaswap.getBalance(poolAddress, userAddress, callback)
  Logger.debug("Getting balance", { pool = poolAddress, user = userAddress })

  local request = {
    Target = poolAddress,
    Action = Constants.API.PERMASWAP.BALANCE
  }

  if userAddress then
    request.Account = userAddress
  end

  ao.send(request).onReply(function(response)
    if response.Error then
      Logger.error("Failed to get balance", {
        pool = poolAddress,
        error = response.Error
      })
      callback(nil, response.Error)
      return
    end

    callback({
      balanceX = response.BalanceX,
      balanceY = response.BalanceY,
      lpBalance = response.Balance,
      totalSupply = response.TotalSupply,
      account = response.Account
    })
  end)
end

-- Discover Permaswap pools (this would require some external source or registry)
function Permaswap.discoverPools(callback)
  -- In a real implementation, this might query a registry process or use a predefined list
  -- For now, we'll return a placeholder message
  Logger.warn("Pool discovery not implemented, use a predefined list of pool addresses")
  callback(nil, "Pool discovery requires external configuration")
end

return Permaswap
