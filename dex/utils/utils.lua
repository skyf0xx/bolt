local Utils = {}
local Constants = require('utils.constants')
local json = require('json')

-- String manipulation utilities

-- Safely get a substring with bounds checking
function Utils.stringSubstr(str, startPos, endPos)
  if not str then return "" end

  startPos = math.max(1, startPos)
  endPos = endPos or #str
  endPos = math.min(endPos, #str)

  if startPos > endPos then return "" end
  return string.sub(str, startPos, endPos)
end

-- Create a short identifier for a string (useful for logs)
function Utils.shortId(str, length)
  length = length or 6
  if not str then return "nil" end
  return Utils.stringSubstr(str, 1, length)
end

-- Table utilities

-- Check if a table contains a value
function Utils.tableContains(tbl, value)
  for _, v in pairs(tbl) do
    if v == value then return true end
  end
  return false
end

-- Get the size of a table (works for non-sequential tables)
function Utils.tableSize(tbl)
  if not tbl then return 0 end

  local count = 0
  for _ in pairs(tbl) do count = count + 1 end
  return count
end

-- Merge two tables (shallow copy)
function Utils.tableMerge(t1, t2)
  local result = {}
  for k, v in pairs(t1) do result[k] = v end
  for k, v in pairs(t2) do result[k] = v end
  return result
end

-- Deep copy a table
function Utils.deepCopy(obj)
  if type(obj) ~= 'table' then return obj end

  -- Use JSON for deep copy (more efficient for most cases)
  -- Fall back to manual copy for tables with non-serializable values
  local status, result = pcall(function()
    return json.decode(json.encode(obj))
  end)

  if status then
    return result
  else
    -- Manual deep copy fallback
    local res = {}
    for k, v in pairs(obj) do
      res[k] = Utils.deepCopy(v)
    end
    return res
  end
end

-- Remove duplicates from array
function Utils.uniqueArray(arr)
  local seen = {}
  local result = {}

  for _, v in ipairs(arr) do
    if not seen[v] then
      seen[v] = true
      table.insert(result, v)
    end
  end

  return result
end

-- Math utilities

-- Convert string to big integer safely
function Utils.toBigInt(str)
  if not str then return 0 end
  if type(str) == 'number' then return math.floor(str) end

  local num = tonumber(str)
  if not num then return 0 end
  return math.floor(num)
end

-- Convert basis points to decimal (10000 basis points = 1.0)
function Utils.bpsToDecimal(bps)
  return tonumber(bps) / Constants.NUMERIC.BASIS_POINTS_MULTIPLIER
end

-- Convert decimal to basis points (1.0 = 10000 basis points)
function Utils.decimalToBps(decimal)
  return math.floor(decimal * Constants.NUMERIC.BASIS_POINTS_MULTIPLIER)
end

-- Calculate percentage change between two values
function Utils.percentageChange(oldValue, newValue)
  oldValue = tonumber(oldValue)
  newValue = tonumber(newValue)

  if not oldValue or not newValue or oldValue == 0 then return 0 end
  return ((newValue - oldValue) / oldValue) * 100
end

-- Format numbers for display (with commas)
function Utils.formatNumber(num, decimals)
  decimals = decimals or 0
  if type(num) == 'string' then
    num = tonumber(num)
  end

  if not num then return "0" end

  -- Format with commas
  local int, dec = math.modf(num)
  local formatted = tostring(int):reverse():gsub("(%d%d%d)", "%1,"):gsub(",$", ""):reverse()

  if decimals > 0 then
    formatted = formatted .. string.format("%." .. decimals .. "f", dec):sub(2)
  end

  return formatted
end

-- JSON utilities

-- Encode a table to JSON string
function Utils.jsonEncode(data)
  local status, result = pcall(json.encode, data)
  if status then
    return result
  else
    return "{\"error\":\"Failed to encode JSON\"}"
  end
end

-- Decode a JSON string to table
function Utils.jsonDecode(jsonStr)
  if not jsonStr or jsonStr == "" then return {} end

  local status, result = pcall(json.decode, jsonStr)
  if status then
    return result
  else
    return {}
  end
end

-- Pretty print JSON with indentation
function Utils.jsonPretty(data)
  if type(data) == "string" then
    -- If it's already a string, try to parse it first
    data = Utils.jsonDecode(data)
  end

  local status, result = pcall(json.encode, data, { pretty = true })
  if status then
    return result
  else
    return "{\"error\":\"Failed to encode JSON\"}"
  end
end

-- Alias for backwards compatibility
Utils.stringifyJson = Utils.jsonEncode

-- Error handling

-- Create standardized error object
function Utils.createError(code, message, details)
  return {
    code = code or Constants.ERROR.UNKNOWN_ERROR,
    message = message or "Unknown error occurred",
    details = details,
    timestamp = os.time()
  }
end

-- Create a simple event emitter
function Utils.createEventEmitter()
  local listeners = {}

  return {
    on = function(event, callback)
      listeners[event] = listeners[event] or {}
      table.insert(listeners[event], callback)
    end,

    emit = function(event, ...)
      if not listeners[event] then return end

      for _, callback in ipairs(listeners[event]) do
        callback(...)
      end
    end,

    removeAllListeners = function(event)
      if event then
        listeners[event] = nil
      else
        listeners = {}
      end
    end
  }
end

-- Provide a way to track multiple async operations and call a callback when all are complete
function Utils.createTaskGroup(totalTasks, onComplete)
  local completed = 0
  local results = {}
  local errors = {}

  return {
    taskDone = function(index, result, isError)
      completed = completed + 1

      if isError then
        errors[index] = result
      else
        results[index] = result
      end

      if completed >= totalTasks then
        onComplete({
          results = results,
          errors = errors,
          success = Utils.tableSize(errors) == 0
        })
      end
    end,

    getTotalTasks = function()
      return totalTasks
    end,

    getCompletedTasks = function()
      return completed
    end
  }
end

-- Token and pool utilities

-- Format token amount considering decimals
function Utils.formatTokenAmount(amount, decimals)
  decimals = decimals or Constants.NUMERIC.DECIMALS

  if not amount then return "0" end
  local numAmount = tonumber(amount)
  if not numAmount then return "0" end

  return numAmount / (10 ^ decimals)
end

-- Parse token amount to smallest unit
function Utils.parseTokenAmount(amount, decimals)
  decimals = decimals or Constants.NUMERIC.DECIMALS

  if not amount then return "0" end
  local numAmount = tonumber(amount)
  if not numAmount then return "0" end

  return math.floor(numAmount * (10 ^ decimals))
end

-- Create unique ID for a token pair (consistent regardless of order)
function Utils.createPairId(tokenA, tokenB)
  if tokenA < tokenB then
    return tokenA .. "-" .. tokenB
  else
    return tokenB .. "-" .. tokenA
  end
end

-- Create unique ID for a directed token pair (order matters)
function Utils.createDirectedPairId(tokenIn, tokenOut)
  return tokenIn .. "->" .. tokenOut
end

-- Logging (basic implementation, can be expanded)
function Utils.log(level, message, data)
  local levels = { DEBUG = 0, INFO = 1, WARN = 2, ERROR = 3 }
  local currentLevel = levels[Constants.OPTIMIZATION.LOG_LEVEL] or 1

  if levels[level] < currentLevel then
    return -- Skip logs below current level
  end

  local timestamp = os.date("%Y-%m-%d %H:%M:%S")
  local logStr = string.format("[%s] [%s] %s", timestamp, level, message)

  if data then
    if type(data) == "table" then
      logStr = logStr .. " " .. Utils.jsonEncode(data)
    else
      logStr = logStr .. " " .. tostring(data)
    end
  end

  print(logStr)
end

return Utils
