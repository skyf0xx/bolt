local BigDecimal = require('dex.utils.big_decimal')
local Constants = require('utils.constants')
local Logger = require('dex.logger').createLogger("SwapCalculator")
local Utils = require('dex.utils')
local PermaswapFormula = require('dex.swap.permaswap_formula')
local BotegaFormula = require('dex.swap.botega_formula')
local PoolRepository = require('dex.db.pool_repository')
local Poller = require('dex.reserve.poller')

local Calculator = {}

-- Initialize calculator with required dependencies
function Calculator.init(db, poller)
  Calculator.db = db
  Calculator.poller = poller or Poller.init(db)
  Logger.info("Swap calculator initialized")
  return Calculator
end

-- Calculate output amount for a single swap
function Calculator.calculateSwapOutput(poolId, tokenIn, amountIn, callback)
  Logger.debug("Calculating swap output", {
    pool = poolId,
    tokenIn = tokenIn,
    amountIn = amountIn
  })

  local pool = PoolRepository.getPool(Calculator.db, poolId)
  if not pool then
    callback(nil, Constants.ERROR.POOL_NOT_FOUND)
    return
  end

  Calculator.poller.getReserves(poolId, false, function(reserves, err)
    if not reserves then
      callback(nil, err or Constants.ERROR.INSUFFICIENT_LIQUIDITY)
      return
    end

    local reserveIn, reserveOut, tokenOut
    if tokenIn == pool.token_a_id then
      reserveIn = reserves.reserve_a
      reserveOut = reserves.reserve_b
      tokenOut = pool.token_b_id
    elseif tokenIn == pool.token_b_id then
      reserveIn = reserves.reserve_b
      reserveOut = reserves.reserve_a
      tokenOut = pool.token_a_id
    else
      callback(nil, Constants.ERROR.INVALID_TOKEN)
      return
    end

    local bdAmountIn = BigDecimal.new(amountIn)
    local bdReserveIn = BigDecimal.new(reserveIn)
    local reserveRatioLimit = BigDecimal.new(Constants.NUMERIC.RESERVE_RATIO_LIMIT *
      Constants.NUMERIC.BASIS_POINTS_MULTIPLIER)
    local maxInput = BigDecimal.divide(BigDecimal.multiply(bdReserveIn, reserveRatioLimit),
      BigDecimal.new(Constants.NUMERIC.BASIS_POINTS_MULTIPLIER))

    if BigDecimal.gt(bdAmountIn, maxInput) then
      callback(nil, "Input amount exceeds maximum allowed relative to pool reserves")
      return
    end

    local outputAmount
    if pool.source == Constants.SOURCE.PERMASWAP then
      outputAmount = PermaswapFormula.getOutputAmount(amountIn, reserveIn, reserveOut, pool.fee_bps)
    elseif pool.source == Constants.SOURCE.BOTEGA then
      local feePercentage = pool.fee_bps / 100
      outputAmount = BotegaFormula.getOutputAmount(amountIn, reserveIn, reserveOut, feePercentage)
    else
      callback(nil, "Unsupported pool source: " .. tostring(pool.source))
      return
    end

    local priceImpactBps
    if pool.source == Constants.SOURCE.PERMASWAP then
      priceImpactBps = PermaswapFormula.calculatePriceImpactBps(amountIn, reserveIn, reserveOut)
    else
      local feePercentage = pool.fee_bps / 100
      priceImpactBps = BotegaFormula.calculatePriceImpactBps(amountIn, reserveIn, reserveOut, feePercentage)
    end

    callback({
      pool_id = poolId,
      source = pool.source,
      token_in = tokenIn,
      token_out = tokenOut,
      amount_in = amountIn,
      amount_out = outputAmount.value,
      fee_bps = pool.fee_bps,
      price_impact_bps = priceImpactBps.value,
      reserves = {
        ['in'] = reserveIn,
        ['out'] = reserveOut
      }
    })
  end)
end

-- Calculate output amount for a multi-hop path
function Calculator.calculatePathOutput(path, inputAmount, callback)
  if not path or #path == 0 then
    callback(nil, "Empty path provided")
    return
  end

  Logger.debug("Calculating path output", {
    pathLength = #path,
    inputAmount = inputAmount
  })

  local poolIds = {}
  for _, step in ipairs(path) do
    table.insert(poolIds, step.pool_id)
  end

  poolIds = Utils.uniqueArray(poolIds)

  Calculator.poller.pollMultiplePools(poolIds, false, function(pollResults)
    if Utils.tableSize(pollResults.errors) > 0 then
      Logger.warn("Some reserves could not be fetched", { errors = pollResults.errors })
    end

    local result = {
      input_amount = inputAmount,
      output_amount = inputAmount,
      steps = {},
      total_fee_bps = 0
    }

    local function processStep(index, currentAmount)
      if index > #path then
        result.output_amount = currentAmount
        callback(result)
        return
      end

      local step = path[index]
      local poolId = step.pool_id
      local tokenIn = step.from
      local tokenOut = step.to

      Calculator.calculateSwapOutput(poolId, tokenIn, currentAmount, function(stepResult, err)
        if not stepResult then
          callback(nil, "Error calculating step " .. index .. ": " .. (err or "Unknown error"))
          return
        end

        table.insert(result.steps, stepResult)

        local stepFeeBps = stepResult.fee_bps
        result.total_fee_bps = result.total_fee_bps + stepFeeBps -
            (result.total_fee_bps * stepFeeBps / Constants.NUMERIC.BASIS_POINTS_MULTIPLIER)

        processStep(index + 1, stepResult.amount_out)
      end)
    end

    processStep(1, inputAmount)
  end)
end

-- Calculate input amount needed to get a specific output amount
function Calculator.calculateRequiredInput(poolId, tokenOut, desiredOutput, callback)
  Logger.debug("Calculating required input", {
    pool = poolId,
    tokenOut = tokenOut,
    desiredOutput = desiredOutput
  })

  local pool = PoolRepository.getPool(Calculator.db, poolId)
  if not pool then
    callback(nil, Constants.ERROR.POOL_NOT_FOUND)
    return
  end

  Calculator.poller.getReserves(poolId, false, function(reserves, err)
    if not reserves then
      callback(nil, err or Constants.ERROR.INSUFFICIENT_LIQUIDITY)
      return
    end

    local reserveIn, reserveOut, tokenIn
    if tokenOut == pool.token_a_id then
      reserveIn = reserves.reserve_b
      reserveOut = reserves.reserve_a
      tokenIn = pool.token_b_id
    elseif tokenOut == pool.token_b_id then
      reserveIn = reserves.reserve_a
      reserveOut = reserves.reserve_b
      tokenIn = pool.token_a_id
    else
      callback(nil, Constants.ERROR.INVALID_TOKEN)
      return
    end

    local bdDesiredOutput = BigDecimal.new(desiredOutput)
    local bdReserveOut = BigDecimal.new(reserveOut)

    if BigDecimal.gte(bdDesiredOutput, bdReserveOut) then
      callback(nil, "Desired output exceeds available reserves")
      return
    end

    local inputAmount
    if pool.source == Constants.SOURCE.PERMASWAP then
      inputAmount = PermaswapFormula.getInputAmount(desiredOutput, reserveIn, reserveOut, pool.fee_bps) or 0
    elseif pool.source == Constants.SOURCE.BOTEGA then
      local feePercentage = pool.fee_bps / 100
      inputAmount = BotegaFormula.getInputAmount(desiredOutput, reserveIn, reserveOut, feePercentage) or 0
    else
      callback(nil, "Unsupported pool source: " .. tostring(pool.source))
      return
    end

    local priceImpactBps
    if pool.source == Constants.SOURCE.PERMASWAP then
      priceImpactBps = PermaswapFormula.calculatePriceImpactBps(inputAmount.value, reserveIn, reserveOut)
    else
      local feePercentage = pool.fee_bps / 100
      priceImpactBps = BotegaFormula.calculatePriceImpactBps(inputAmount.value, reserveIn, reserveOut, feePercentage)
    end

    callback({
      pool_id = poolId,
      source = pool.source,
      token_in = tokenIn,
      token_out = tokenOut,
      amount_in = inputAmount.value,
      amount_out = desiredOutput,
      fee_bps = pool.fee_bps,
      price_impact_bps = priceImpactBps.value,
      reserves = {
        ['in'] = reserveIn,
        ['out'] = reserveOut
      }
    })
  end)
end

-- Calculate price impact for a swap
function Calculator.calculatePriceImpact(poolId, tokenIn, amountIn, callback)
  local pool = PoolRepository.getPool(Calculator.db, poolId)
  if not pool then
    callback(nil, Constants.ERROR.POOL_NOT_FOUND)
    return
  end

  Calculator.poller.getReserves(poolId, false, function(reserves, err)
    if not reserves then
      callback(nil, err or Constants.ERROR.INSUFFICIENT_LIQUIDITY)
      return
    end

    local reserveIn, reserveOut
    if tokenIn == pool.token_a_id then
      reserveIn = reserves.reserve_a
      reserveOut = reserves.reserve_b
    elseif tokenIn == pool.token_b_id then
      reserveIn = reserves.reserve_b
      reserveOut = reserves.reserve_a
    else
      callback(nil, Constants.ERROR.INVALID_TOKEN)
      return
    end

    local priceImpactBps
    if pool.source == Constants.SOURCE.PERMASWAP then
      priceImpactBps = PermaswapFormula.calculatePriceImpactBps(amountIn, reserveIn, reserveOut)
    elseif pool.source == Constants.SOURCE.BOTEGA then
      local feePercentage = pool.fee_bps / 100
      priceImpactBps = BotegaFormula.calculatePriceImpactBps(amountIn, reserveIn, reserveOut, feePercentage)
    else
      callback(nil, "Unsupported pool source: " .. tostring(pool.source))
      return
    end

    callback({
      pool_id = poolId,
      source = pool.source,
      token_in = tokenIn,
      amount_in = amountIn,
      price_impact_bps = priceImpactBps.value,
      price_impact_percent = Utils.bpsToDecimal(priceImpactBps.value)
    })
  end)
end

-- Calculate the spot price between two tokens in a pool
function Calculator.calculateSpotPrice(poolId, baseToken, quoteToken, callback)
  local pool = PoolRepository.getPool(Calculator.db, poolId)
  if not pool then
    callback(nil, Constants.ERROR.POOL_NOT_FOUND)
    return
  end

  Calculator.poller.getReserves(poolId, false, function(reserves, err)
    if not reserves then
      callback(nil, err or Constants.ERROR.INSUFFICIENT_LIQUIDITY)
      return
    end

    local baseReserve, quoteReserve
    if baseToken == pool.token_a_id and quoteToken == pool.token_b_id then
      baseReserve = reserves.reserve_a
      quoteReserve = reserves.reserve_b
    elseif baseToken == pool.token_b_id and quoteToken == pool.token_a_id then
      baseReserve = reserves.reserve_b
      quoteReserve = reserves.reserve_a
    else
      callback(nil, Constants.ERROR.INVALID_TOKEN)
      return
    end

    local bdBaseReserve = BigDecimal.new(baseReserve)
    local bdQuoteReserve = BigDecimal.new(quoteReserve)
    local spotPrice = BigDecimal.divide(bdQuoteReserve, bdBaseReserve)

    callback({
      pool_id = poolId,
      source = pool.source,
      base_token = baseToken,
      quote_token = quoteToken,
      spot_price = spotPrice.value,
      reserves = {
        base = baseReserve,
        quote = quoteReserve
      }
    })
  end)
end

return Calculator
