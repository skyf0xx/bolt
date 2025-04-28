local BigDecimal = require('arbitrage.utils.big_decimal')
local Constants = require('arbitrage.constants')
local Logger = require('arbitrage.logger').createLogger("BotegaFormula")
local Utils = require('arbitrage.utils')

local BotegaFormula = {}

-- Calculate expected output amount for a swap in Botega
-- Using two-step formula:
-- Step 1: incomingQtyFeeAdjusted = inputQty * (100 - lpFeePercent - protocolFeePercent) / 100
-- Step 2: outputQty = reserveOut - (reserveIn * reserveOut) / (reserveIn + incomingQtyFeeAdjusted)
function BotegaFormula.getOutputAmount(amountIn, reserveIn, reserveOut, feePercent)
  Logger.debug("Calculating output amount", {
    amountIn = amountIn,
    reserveIn = reserveIn,
    reserveOut = reserveOut,
    feePercent = feePercent
  })

  -- Convert inputs to BigDecimal
  local bdAmountIn = BigDecimal.new(amountIn)
  local bdReserveIn = BigDecimal.new(reserveIn)
  local bdReserveOut = BigDecimal.new(reserveOut)
  local bdFeePercent = BigDecimal.new(feePercent)
  local bdHundred = BigDecimal.new('100')

  -- Step 1: Calculate amount after fee deduction
  -- incomingQtyFeeAdjusted = inputQty * (100 - feePercent) / 100
  local feeAdjustment = BigDecimal.subtract(bdHundred, bdFeePercent)
  local scaledAmount = BigDecimal.multiply(bdAmountIn, feeAdjustment)
  local amountInAfterFees = BigDecimal.divide(scaledAmount, bdHundred)

  -- Step 2: Calculate output using constant product formula
  -- outputQty = reserveOut - (reserveIn * reserveOut) / (reserveIn + incomingQtyFeeAdjusted)

  -- Calculate new denominator: reserveIn + incomingQtyFeeAdjusted
  local newDenominator = BigDecimal.add(bdReserveIn, amountInAfterFees)

  -- Calculate product of reserves: reserveIn * reserveOut
  local reserveProduct = BigDecimal.multiply(bdReserveIn, bdReserveOut)

  -- Calculate new reserve out: (reserveIn * reserveOut) / newDenominator
  local newReserveOut = BigDecimal.divide(reserveProduct, newDenominator)

  -- Calculate output amount: reserveOut - newReserveOut
  local outputAmount = BigDecimal.subtract(bdReserveOut, newReserveOut)

  Logger.debug("Output amount calculated", {
    amountInAfterFees = amountInAfterFees.value,
    result = outputAmount.value
  })

  return outputAmount
end

-- Calculate input amount needed to get a specific output
function BotegaFormula.getInputAmount(amountOut, reserveIn, reserveOut, feePercent)
  Logger.debug("Calculating input amount", {
    amountOut = amountOut,
    reserveIn = reserveIn,
    reserveOut = reserveOut,
    feePercent = feePercent
  })

  -- Convert inputs to BigDecimal
  local bdAmountOut = BigDecimal.new(amountOut)
  local bdReserveIn = BigDecimal.new(reserveIn)
  local bdReserveOut = BigDecimal.new(reserveOut)
  local bdFeePercent = BigDecimal.new(feePercent)
  local bdHundred = BigDecimal.new('100')

  -- Check if output amount exceeds available reserves
  if BigDecimal.gte(bdAmountOut, bdReserveOut) then
    Logger.error("Insufficient output reserve", {
      amountOut = amountOut,
      reserveOut = reserveOut
    })
    return nil, "Insufficient output reserve"
  end

  -- Step 1: Calculate what the new reserve out would be after swap
  local newReserveOut = BigDecimal.subtract(bdReserveOut, bdAmountOut)

  -- Step 2: Calculate what reserveIn + amountInAfterFees would be based on constant product
  -- (reserveIn + amountInAfterFees) = (reserveIn * reserveOut) / newReserveOut
  local reserveProduct = BigDecimal.multiply(bdReserveIn, bdReserveOut)
  local totalNewReserveIn = BigDecimal.divide(reserveProduct, newReserveOut)

  -- Step 3: Calculate amountInAfterFees
  -- amountInAfterFees = totalNewReserveIn - reserveIn
  local amountInAfterFees = BigDecimal.subtract(totalNewReserveIn, bdReserveIn)

  -- Step 4: Calculate actual amountIn by accounting for fees
  -- amountIn = amountInAfterFees * 100 / (100 - feePercent)
  local feeAdjustment = BigDecimal.subtract(bdHundred, bdFeePercent)
  local scaledAmount = BigDecimal.multiply(amountInAfterFees, bdHundred)
  local amountIn = BigDecimal.divide(scaledAmount, feeAdjustment)

  -- Add 1 to round up for minimum input amount
  local result = BigDecimal.add(amountIn, BigDecimal.new('1'))

  Logger.debug("Input amount calculated", {
    amountInAfterFees = amountInAfterFees.value,
    result = result.value
  })

  return result
end

-- Calculate price impact percentage (in basis points)
function BotegaFormula.calculatePriceImpactBps(amountIn, reserveIn, reserveOut, feePercent)
  Logger.debug("Calculating price impact", {
    amountIn = amountIn,
    reserveIn = reserveIn,
    reserveOut = reserveOut,
    feePercent = feePercent
  })

  -- Convert inputs to BigDecimal
  local bdAmountIn = BigDecimal.new(amountIn)
  local bdReserveIn = BigDecimal.new(reserveIn)
  local bdReserveOut = BigDecimal.new(reserveOut)
  local bdFeePercent = BigDecimal.new(feePercent)
  local bdHundred = BigDecimal.new('100')
  local bdBpsMultiplier = BigDecimal.new(Constants.NUMERIC.BASIS_POINTS_MULTIPLIER)

  -- Calculate spot price: reserveOut / reserveIn
  local spotPrice = BigDecimal.divide(bdReserveOut, bdReserveIn)

  -- Calculate amount after fee deduction
  local feeAdjustment = BigDecimal.subtract(bdHundred, bdFeePercent)
  local scaledAmount = BigDecimal.multiply(bdAmountIn, feeAdjustment)
  local amountInAfterFees = BigDecimal.divide(scaledAmount, bdHundred)

  -- Calculate new reserves
  local newReserveIn = BigDecimal.add(bdReserveIn, amountInAfterFees)
  local reserveProduct = BigDecimal.multiply(bdReserveIn, bdReserveOut)
  local newReserveOut = BigDecimal.divide(reserveProduct, newReserveIn)

  -- Calculate output amount
  local amountOut = BigDecimal.subtract(bdReserveOut, newReserveOut)

  -- Calculate execution price: amountOut / amountIn
  local executionPrice = BigDecimal.divide(amountOut, bdAmountIn)

  -- Calculate price impact: (spotPrice - executionPrice) / spotPrice
  local priceDifference = BigDecimal.subtract(spotPrice, executionPrice)
  local impact = BigDecimal.divide(priceDifference, spotPrice)

  -- Convert to basis points (multiply by 10000)
  local impactBps = BigDecimal.multiply(impact, bdBpsMultiplier)

  Logger.debug("Price impact calculated", { impactBps = impactBps.value })
  return impactBps
end

-- Calculate the constant product value (k = x * y)
function BotegaFormula.calculateConstantProduct(reserveX, reserveY)
  local bdReserveX = BigDecimal.new(reserveX)
  local bdReserveY = BigDecimal.new(reserveY)

  return BigDecimal.multiply(bdReserveX, bdReserveY)
end

-- Verify if a trade would satisfy the constant product formula
function BotegaFormula.verifyTradeValidity(reserveIn, reserveOut, amountIn, amountOut, feePercent)
  -- Convert all values to BigDecimal
  local bdReserveIn = BigDecimal.new(reserveIn)
  local bdReserveOut = BigDecimal.new(reserveOut)
  local bdAmountIn = BigDecimal.new(amountIn)
  local bdAmountOut = BigDecimal.new(amountOut)
  local bdFeePercent = BigDecimal.new(feePercent)
  local bdHundred = BigDecimal.new('100')

  -- Original constant product: reserveIn * reserveOut
  local originalK = BigDecimal.multiply(bdReserveIn, bdReserveOut)

  -- Calculate fee adjustment factor
  local feeAdjustment = BigDecimal.subtract(bdHundred, bdFeePercent)
  local feeMultiplier = BigDecimal.divide(feeAdjustment, bdHundred)

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
function BotegaFormula.calculateNewReserves(reserveIn, reserveOut, amountIn, feePercent)
  -- Convert all values to BigDecimal
  local bdReserveIn = BigDecimal.new(reserveIn)
  local bdReserveOut = BigDecimal.new(reserveOut)
  local bdAmountIn = BigDecimal.new(amountIn)
  local bdFeePercent = BigDecimal.new(feePercent)
  local bdHundred = BigDecimal.new('100')

  -- Calculate fee adjustment factor
  local feeAdjustment = BigDecimal.subtract(bdHundred, bdFeePercent)
  local scaledAmount = BigDecimal.multiply(bdAmountIn, feeAdjustment)
  local amountInAfterFees = BigDecimal.divide(scaledAmount, bdHundred)

  -- Calculate amount out using formula
  local amountOut = BotegaFormula.getOutputAmount(amountIn, reserveIn, reserveOut, feePercent)

  -- Calculate new reserves
  local newReserveIn = BigDecimal.add(bdReserveIn, amountInAfterFees)
  local newReserveOut = BigDecimal.subtract(bdReserveOut, amountOut)

  return {
    newReserveIn = newReserveIn.value,
    newReserveOut = newReserveOut.value,
    amountOut = amountOut.value,
    amountInAfterFees = amountInAfterFees.value
  }
end

-- Convert fee from basis points to percentage for Botega calculations
function BotegaFormula.bpsToPercentage(feeBps)
  return feeBps / 100
end

-- Calculate LP and protocol fee amounts
function BotegaFormula.calculateFeeAmounts(amountIn, lpFeePercent, protocolFeePercent)
  -- Convert to BigDecimal
  local bdAmountIn = BigDecimal.new(amountIn)
  local bdLpFeePercent = BigDecimal.new(lpFeePercent)
  local bdProtocolFeePercent = BigDecimal.new(protocolFeePercent)
  local bdHundred = BigDecimal.new('100')

  -- Calculate LP fee amount: amountIn * lpFeePercent / 100
  local lpFeeAmount = BigDecimal.divide(
    BigDecimal.multiply(bdAmountIn, bdLpFeePercent),
    bdHundred
  )

  -- Calculate protocol fee amount: amountIn * protocolFeePercent / 100
  local protocolFeeAmount = BigDecimal.divide(
    BigDecimal.multiply(bdAmountIn, bdProtocolFeePercent),
    bdHundred
  )

  -- Calculate total fee amount
  local totalFeeAmount = BigDecimal.add(lpFeeAmount, protocolFeeAmount)

  -- Calculate amount after fees
  local amountAfterFees = BigDecimal.subtract(bdAmountIn, totalFeeAmount)

  return {
    lpFeeAmount = lpFeeAmount.value,
    protocolFeeAmount = protocolFeeAmount.value,
    totalFeeAmount = totalFeeAmount.value,
    amountAfterFees = amountAfterFees.value
  }
end

return BotegaFormula
