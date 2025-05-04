local BigDecimal = require('dex.utils.big_decimal')
local Constants = require('dex.utils.constants')
local Logger = require('dex.utils.logger')
Logger = Logger.createLogger("SwapCalculator")
local Utils = require('dex.utils.utils')
local PermaswapFormula = require('dex.swap.permaswap_formula')
local BotegaFormula = require('dex.swap.botega_formula')
local PoolRepository = require('dex.db.pool_repository')
local Poller = require('dex.reserve.poller')
-- Add new dependencies
local Permaswap = require('dex.collectors.permaswap')
local Botega = require('dex.collectors.botega')

local Calculator = {}

-- Initialize calculator with required dependencies
function Calculator.init(db, poller)
  Calculator.db = db
  Calculator.poller = poller or Poller.init(db)
  Logger.info("Swap calculator initialized")
  return Calculator
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
