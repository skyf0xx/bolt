local bint = require('bint')(256) -- Using the bint library for arbitrary precision
local Constants = require('dex.constants')

local BigDecimal = {}

-- Create a new BigDecimal from various input types
function BigDecimal.new(value)
  local bd = {}

  -- Convert the input value to a string representation of BigInt
  if type(value) == 'table' and value.value then
    -- If it's already a BigDecimal, just copy it
    bd.value = value.value
  elseif type(value) == 'string' then
    -- If it's a string, directly use as bint input
    bd.value = tostring(bint(value))
  elseif type(value) == 'number' then
    -- If it's a number, convert to string first to avoid precision loss
    bd.value = tostring(bint(tostring(value)))
  else
    -- Default to zero
    bd.value = '0'
  end

  return setmetatable(bd, { __index = BigDecimal })
end

-- Addition: a + b
function BigDecimal.add(a, b)
  local result = BigDecimal.new({})
  result.value = tostring(bint(a.value) + bint(b.value))
  return result
end

-- Subtraction: a - b
function BigDecimal.subtract(a, b)
  local result = BigDecimal.new({})
  result.value = tostring(bint(a.value) - bint(b.value))
  return result
end

-- Multiplication: a * b
function BigDecimal.multiply(a, b)
  local result = BigDecimal.new({})
  result.value = tostring(bint(a.value) * bint(b.value))
  return result
end

-- Division: a / b with precision control
function BigDecimal.divide(a, b, precision)
  precision = precision or Constants.NUMERIC.DECIMALS

  if bint(b.value) == bint.zero() then
    error("Division by zero")
  end

  -- Scale up for precision before division
  local scale = bint(10) ^ precision
  local scaledA = bint(a.value) * scale

  local result = BigDecimal.new({})
  result.value = tostring(bint.udiv(scaledA, bint(b.value)))
  return result
end

-- Integer division: floor(a / b)
function BigDecimal.divideInt(a, b)
  if bint(b.value) == bint.zero() then
    error("Division by zero")
  end

  local result = BigDecimal.new({})
  result.value = tostring(bint.udiv(bint(a.value), bint(b.value)))
  return result
end

-- Modulo: a % b
function BigDecimal.mod(a, b)
  if bint(b.value) == bint.zero() then
    error("Modulo by zero")
  end

  local result = BigDecimal.new({})
  result.value = tostring(bint(a.value) % bint(b.value))
  return result
end

-- Power: a^exponent
function BigDecimal.pow(a, exponent)
  if type(exponent) ~= 'number' then
    error("Exponent must be a number")
  end

  local result = BigDecimal.new('1')
  local base = BigDecimal.new(a.value)

  -- Simple integer power implementation
  for _ = 1, exponent do
    result = BigDecimal.multiply(result, base)
  end

  return result
end

-- Comparison operators
function BigDecimal.eq(a, b)
  return bint(a.value) == bint(b.value)
end

function BigDecimal.lt(a, b)
  return bint(a.value) < bint(b.value)
end

function BigDecimal.lte(a, b)
  return bint(a.value) <= bint(b.value)
end

function BigDecimal.gt(a, b)
  return bint(a.value) > bint(b.value)
end

function BigDecimal.gte(a, b)
  return bint(a.value) >= bint(b.value)
end

-- Convert to decimal with proper scaling (mainly for display purposes)
function BigDecimal.toDecimal(a, decimals)
  decimals = decimals or Constants.NUMERIC.DECIMALS

  local value = a.value
  local intPart, fracPart

  -- If the value is small enough to need leading zeros
  if #value <= decimals then
    intPart = '0'
    fracPart = string.rep('0', decimals - #value) .. value
  else
    intPart = string.sub(value, 1, #value - decimals)
    fracPart = string.sub(value, #value - decimals + 1)

    -- If intPart is empty, it was all leading zeros
    if intPart == '' then
      intPart = '0'
    end
  end

  -- Remove trailing zeros from the fractional part
  fracPart = fracPart:gsub('0+$', '')

  -- If there's a fractional part, include the decimal point
  if fracPart ~= '' then
    return intPart .. '.' .. fracPart
  else
    return intPart
  end
end

-- Convert from a decimal string to BigDecimal
function BigDecimal.fromDecimal(decimalStr, decimals)
  decimals = decimals or Constants.NUMERIC.DECIMALS

  -- Parse the decimal string
  local intPart, fracPart = string.match(decimalStr, "([^.]+)%.?([^.]*)")
  if not intPart then
    return BigDecimal.new('0')
  end

  -- Remove leading zeros from integer part
  intPart = intPart:gsub('^0+', '')
  if intPart == '' then
    intPart = '0'
  end

  -- Pad or truncate fractional part to specified decimals
  if #fracPart < decimals then
    fracPart = fracPart .. string.rep('0', decimals - #fracPart)
  elseif #fracPart > decimals then
    fracPart = string.sub(fracPart, 1, decimals)
  end

  local value
  if intPart == '0' and fracPart:gsub('0', '') == '' then
    value = '0'
  else
    value = intPart .. fracPart
  end

  return BigDecimal.new(value)
end

-- Convert to string (full precision value)
function BigDecimal.toString(a)
  return a.value
end

-- Create a BigDecimal from token amount and decimals
function BigDecimal.fromTokenAmount(amount, decimals)
  decimals = decimals or Constants.NUMERIC.DECIMALS

  local numAmount = tonumber(amount)
  if not numAmount then
    return BigDecimal.new('0')
  end

  -- Calculate the scaled integer value
  local scaledValue = math.floor(numAmount * (10 ^ decimals))
  return BigDecimal.new(tostring(scaledValue))
end

-- Convert a BigDecimal to token amount based on decimals
function BigDecimal.toTokenAmount(bd, decimals)
  decimals = decimals or Constants.NUMERIC.DECIMALS

  local divisor = BigDecimal.new(tostring(10 ^ decimals))
  return BigDecimal.divide(bd, divisor, decimals)
end

-- AMM-specific functions for constant product formula (x * y = k)
function BigDecimal.getOutputAmount(amountIn, reserveIn, reserveOut, feeBps)
  local amountInWithFee = BigDecimal.multiply(
    amountIn,
    BigDecimal.subtract(
      BigDecimal.new(Constants.NUMERIC.BASIS_POINTS_MULTIPLIER),
      BigDecimal.new(feeBps)
    )
  )

  local numerator = BigDecimal.multiply(amountInWithFee, reserveOut)
  local denominator = BigDecimal.add(
    BigDecimal.multiply(
      BigDecimal.new(Constants.NUMERIC.BASIS_POINTS_MULTIPLIER),
      reserveIn
    ),
    amountInWithFee
  )

  return BigDecimal.divide(numerator, denominator)
end

-- Calculate amount in given desired output and reserves
function BigDecimal.getInputAmount(amountOut, reserveIn, reserveOut, feeBps)
  if BigDecimal.gte(amountOut, reserveOut) then
    error("Insufficient output reserve")
  end

  local numerator = BigDecimal.multiply(
    BigDecimal.multiply(reserveIn, amountOut),
    BigDecimal.new(Constants.NUMERIC.BASIS_POINTS_MULTIPLIER)
  )

  local denominator = BigDecimal.multiply(
    BigDecimal.subtract(reserveOut, amountOut),
    BigDecimal.subtract(
      BigDecimal.new(Constants.NUMERIC.BASIS_POINTS_MULTIPLIER),
      BigDecimal.new(feeBps)
    )
  )

  -- Add 1 to round up
  return BigDecimal.add(
    BigDecimal.divide(numerator, denominator),
    BigDecimal.new('1')
  )
end

-- Calculate price impact percentage (in basis points)
function BigDecimal.calculatePriceImpactBps(amountIn, reserveIn, reserveOut)
  local spotPrice = BigDecimal.divide(reserveOut, reserveIn)

  local newReserveIn = BigDecimal.add(reserveIn, amountIn)
  local newReserveOut = BigDecimal.divide(
    BigDecimal.multiply(reserveIn, reserveOut),
    newReserveIn
  )

  local executionPrice = BigDecimal.divide(
    BigDecimal.subtract(reserveOut, newReserveOut),
    amountIn
  )

  local impact = BigDecimal.divide(
    BigDecimal.subtract(spotPrice, executionPrice),
    spotPrice
  )

  -- Convert to basis points (multiply by 10000)
  return BigDecimal.multiply(
    impact,
    BigDecimal.new(Constants.NUMERIC.BASIS_POINTS_MULTIPLIER)
  )
end

return BigDecimal
