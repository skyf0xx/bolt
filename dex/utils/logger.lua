local Constants = require('dex.constants')
local Utils = require('dex.utils')

local Logger = {}

-- Log levels and their numeric values
Logger.LEVELS = {
  DEBUG = 1,
  INFO = 2,
  WARN = 3,
  ERROR = 4
}

-- Keep track of current log level
local currentLevel = Logger.LEVELS[Constants.OPTIMIZATION.LOG_LEVEL] or Logger.LEVELS.INFO

-- Convert any data to string representation
local function stringify(data)
  if data == nil then
    return "nil"
  elseif type(data) == "table" then
    local result = "{"
    local first = true
    for k, v in pairs(data) do
      if not first then
        result = result .. ", "
      end
      first = false

      -- Handle key
      if type(k) == "string" then
        result = result .. k .. "="
      else
        result = result .. "[" .. tostring(k) .. "]="
      end

      -- Handle value recursively (with depth limit)
      if type(v) == "table" then
        -- Prevent deep recursion by showing table reference only
        result = result .. "table:" .. tostring(v)
      else
        result = result .. tostring(v)
      end
    end
    return result .. "}"
  else
    return tostring(data)
  end
end

-- Format a log message with timestamp and metadata
local function formatLogMessage(level, module, message, data)
  local timestamp = os.date("%Y-%m-%d %H:%M:%S")
  local result = string.format("[%s] [%s]", timestamp, level)

  if module then
    result = result .. string.format(" [%s]", module)
  end

  result = result .. " " .. message

  if data ~= nil then
    result = result .. " " .. stringify(data)
  end

  return result
end

-- Set the log level
function Logger.setLevel(level)
  if Logger.LEVELS[level] then
    currentLevel = Logger.LEVELS[level]
    Logger.info("Logger", "Log level set to " .. level)
  else
    Logger.warn("Logger", "Invalid log level: " .. tostring(level))
  end
end

-- Log based on level
local function log(level, levelName, module, message, data)
  if level >= currentLevel then
    local logMessage = formatLogMessage(levelName, module, message, data)
    print(logMessage)

    -- Here we could add file logging or other output methods
    -- For example:
    -- appendToFile("logs.txt", logMessage)
  end
end

-- Debug level logging
function Logger.debug(module, message, data)
  log(Logger.LEVELS.DEBUG, "DEBUG", module, message, data)
end

-- Info level logging
function Logger.info(module, message, data)
  log(Logger.LEVELS.INFO, "INFO", module, message, data)
end

-- Warning level logging
function Logger.warn(module, message, data)
  log(Logger.LEVELS.WARN, "WARN", module, message, data)
end

-- Error level logging
function Logger.error(module, message, data)
  log(Logger.LEVELS.ERROR, "ERROR", module, message, data)
end

-- Create a module-specific logger
function Logger.createLogger(moduleName)
  return {
    debug = function(message, data)
      Logger.debug(moduleName, message, data)
    end,

    info = function(message, data)
      Logger.info(moduleName, message, data)
    end,

    warn = function(message, data)
      Logger.warn(moduleName, message, data)
    end,

    error = function(message, data)
      Logger.error(moduleName, message, data)
    end
  }
end

-- Handle uncaught errors
function Logger.handleError(err)
  Logger.error("Global", "Uncaught error", err)

  -- We could add error reporting to a monitoring service here

  return Utils.createError(Constants.ERROR.UNKNOWN_ERROR, "An unexpected error occurred", err)
end

-- Create a logger that prefixes log messages with transaction ID
function Logger.createTxLogger(txId)
  return Logger.createLogger("TX-" .. Utils.shortId(txId))
end

-- Initialize with log level from constants
Logger.setLevel(Constants.OPTIMIZATION.LOG_LEVEL)

return Logger
