local BigDecimal = require('dex.utils.big_decimal')
local Constants = require('dex.utils.constants')
local Logger = require('dex.utils.logger')
Logger = Logger.createLogger("PermaswapFormula")

local PermaswapFormula = {}

-- Calculate expected output amount for a swap in Permaswap
-- Using formula: amount_out = (amountIn * (10000 - fee) * reserveOut) / ((10000 * reserveIn) + (amountIn * (10000 - fee)))
function PermaswapFormula.getOutputAmount(amountIn, reserveIn, reserveOut, feeBps)
  Logger.debug("Calculating output amount", {
    amountIn = amountIn,
    reserveIn = reserveIn,
    reserveOut = reserveOut,
    feeBps = feeBps
  })

  -- Convert inputs to BigDecimal
  local bdAmountIn = BigDecimal.new(amountIn)
  local bdReserveIn = BigDecimal.new(reserveIn)
  local bdReserveOut = BigDecimal.new(reserveOut)
  local bdFee = BigDecimal.new(feeBps)
  local bdBpsMultiplier = BigDecimal.new(Constants.NUMERIC.BASIS_POINTS_MULTIPLIER)

  -- Calculate fee factor: (10000 - fee)
  local feeFactor = BigDecimal.subtract(bdBpsMultiplier, bdFee)

  -- Calculate numerator: amountIn * (10000 - fee) * reserveOut
  local amountInWithFee = BigDecimal.multiply(bdAmountIn, feeFactor)
  local numerator = BigDecimal.multiply(amountInWithFee, bdReserveOut)

  -- Calculate denominator: (10000 * reserveIn) + (amountIn * (10000 - fee))
  local scaledReserveIn = BigDecimal.multiply(bdBpsMultiplier, bdReserveIn)
  local denominator = BigDecimal.add(scaledReserveIn, amountInWithFee)

  -- Final calculation: numerator / denominator
  local result = BigDecimal.divide(numerator, denominator)

  Logger.debug("Output amount calculated", { result = result.value })
  return result
end

-- Calculate input amount needed to get a specific output
-- Derived from output formula: amountIn = (reserveIn * amount_out * 10000) / ((reserveOut - amount_out) * (10000 - fee))
function PermaswapFormula.getInputAmount(amount_out, reserveIn, reserveOut, feeBps)
  Logger.debug("Calculating input amount", {
    amount_out = amount_out,
    reserveIn = reserveIn,
    reserveOut = reserveOut,
    feeBps = feeBps
  })

  -- Convert inputs to BigDecimal
  local bdAmountOut = BigDecimal.new(amount_out)
  local bdReserveIn = BigDecimal.new(reserveIn)
  local bdReserveOut = BigDecimal.new(reserveOut)
  local bdFee = BigDecimal.new(feeBps)
  local bdBpsMultiplier = BigDecimal.new(Constants.NUMERIC.BASIS_POINTS_MULTIPLIER)

  -- Check if output amount exceeds available reserves
  if BigDecimal.gte(bdAmountOut, bdReserveOut) then
    Logger.error("Insufficient output reserve", {
      amount_out = amount_out,
      reserveOut = reserveOut
    })
    return nil, "Insufficient output reserve"
  end

  -- Calculate fee factor: (10000 - fee)
  local feeFactor = BigDecimal.subtract(bdBpsMultiplier, bdFee)

  -- Calculate numerator: reserveIn * amount_out * 10000
  local scaledReserveIn = BigDecimal.multiply(bdReserveIn, bdBpsMultiplier)
  local numerator = BigDecimal.multiply(scaledReserveIn, bdAmountOut)

  -- Calculate denominator: (reserveOut - amount_out) * (10000 - fee)
  local remainingReserveOut = BigDecimal.subtract(bdReserveOut, bdAmountOut)
  local denominator = BigDecimal.multiply(remainingReserveOut, feeFactor)

  -- Final calculation: numerator / denominator
  -- Add 1 to round up for minimum input amount
  local result = BigDecimal.add(
    BigDecimal.divide(numerator, denominator),
    BigDecimal.new('1')
  )

  Logger.debug("Input amount calculated", { result = result.value })
  return result
end

-- Calculate price impact percentage (in basis points)
function PermaswapFormula.calculatePriceImpactBps(amountIn, reserveIn, reserveOut)
  Logger.debug("Calculating price impact", {
    amountIn = amountIn,
    reserveIn = reserveIn,
    reserveOut = reserveOut
  })

  -- Convert inputs to BigDecimal
  local bdAmountIn = BigDecimal.new(amountIn)
  local bdReserveIn = BigDecimal.new(reserveIn)
  local bdReserveOut = BigDecimal.new(reserveOut)
  local bdBpsMultiplier = BigDecimal.new(Constants.NUMERIC.BASIS_POINTS_MULTIPLIER)

  -- Calculate spot price: reserveOut / reserveIn
  local spotPrice = BigDecimal.divide(bdReserveOut, bdReserveIn)

  -- Calculate new reserve in: reserveIn + amountIn
  local newReserveIn = BigDecimal.add(bdReserveIn, bdAmountIn)

  -- Calculate new reserve out: (reserveIn * reserveOut) / newReserveIn
  local product = BigDecimal.multiply(bdReserveIn, bdReserveOut)
  local newReserveOut = BigDecimal.divide(product, newReserveIn)

  -- Calculate execution price: (reserveOut - newReserveOut) / amountIn
  local deltaReserveOut = BigDecimal.subtract(bdReserveOut, newReserveOut)
  local executionPrice = BigDecimal.divide(deltaReserveOut, bdAmountIn)

  -- Calculate price impact: (spotPrice - executionPrice) / spotPrice
  local priceDifference = BigDecimal.subtract(spotPrice, executionPrice)
  local impact = BigDecimal.divide(priceDifference, spotPrice)

  -- Convert to basis points (multiply by 10000)
  local impactBps = BigDecimal.multiply(impact, bdBpsMultiplier)

  Logger.debug("Price impact calculated", { impactBps = impactBps.value })
  return impactBps
end

-- Calculate the constant product value (k = x * y)
function PermaswapFormula.calculateConstantProduct(reserveX, reserveY)
  local bdReserveX = BigDecimal.new(reserveX)
  local bdReserveY = BigDecimal.new(reserveY)

  return BigDecimal.multiply(bdReserveX, bdReserveY)
end

-- Verify if a trade would satisfy the constant product formula
function PermaswapFormula.verifyTradeValidity(reserveIn, reserveOut, amountIn, amount_out, feeBps)
  -- Convert all values to BigDecimal
  local bdReserveIn = BigDecimal.new(reserveIn)
  local bdReserveOut = BigDecimal.new(reserveOut)
  local bdAmountIn = BigDecimal.new(amountIn)
  local bdAmountOut = BigDecimal.new(amount_out)
  local bdFee = BigDecimal.new(feeBps)
  local bdBpsMultiplier = BigDecimal.new(Constants.NUMERIC.BASIS_POINTS_MULTIPLIER)

  -- Original constant product: reserveIn * reserveOut
  local originalK = BigDecimal.multiply(bdReserveIn, bdReserveOut)

  -- Calculate fee factor: (10000 - fee) / 10000
  local feeFactor = BigDecimal.subtract(bdBpsMultiplier, bdFee)
  local feeMultiplier = BigDecimal.divide(feeFactor, bdBpsMultiplier)

  -- Amount in after fee
  local amountInAfterFee = BigDecimal.multiply(bdAmountIn, feeMultiplier)

  -- New reserves after trade
  local newReserveIn = BigDecimal.add(bdReserveIn, amountInAfterFee)
  local newReserveOut = BigDecimal.subtract(bdReserveOut, bdAmountOut)

  -- New constant product
  local newK = BigDecimal.multiply(newReserveIn, newReserveOut)

  -- The new K should be greater than or equal to the original K
  return {
    valid = BigDecimal.gte(newK, originalK),
    originalK = originalK.value,
    newK = newK.value,
    difference = BigDecimal.subtract(newK, originalK).value
  }
end

-- Calculate new reserves after a swap
function PermaswapFormula.calculateNewReserves(reserveIn, reserveOut, amountIn, feeBps)
  -- Convert all values to BigDecimal
  local bdReserveIn = BigDecimal.new(reserveIn)
  local bdReserveOut = BigDecimal.new(reserveOut)
  local bdAmountIn = BigDecimal.new(amountIn)
  local bdFee = BigDecimal.new(feeBps)
  local bdBpsMultiplier = BigDecimal.new(Constants.NUMERIC.BASIS_POINTS_MULTIPLIER)

  -- Calculate fee factor: (10000 - fee) / 10000
  local feeFactor = BigDecimal.subtract(bdBpsMultiplier, bdFee)
  local feeMultiplier = BigDecimal.divide(feeFactor, bdBpsMultiplier)

  -- Amount in after fee
  local amountInAfterFee = BigDecimal.multiply(bdAmountIn, feeMultiplier)

  -- Calculate amount out
  local amount_out = PermaswapFormula.getOutputAmount(amountIn, reserveIn, reserveOut, feeBps)

  -- New reserves after trade
  local newReserveIn = BigDecimal.add(bdReserveIn, amountInAfterFee)
  local newReserveOut = BigDecimal.subtract(bdReserveOut, amount_out)

  return {
    newReserveIn = newReserveIn.value,
    newReserveOut = newReserveOut.value,
    amount_out = amount_out.value
  }
end

return PermaswapFormula
