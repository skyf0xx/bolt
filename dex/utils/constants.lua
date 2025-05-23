local Constants = {}

-- Version information
Constants.VERSION = "0.1.0"
Constants.APP_NAME = "DEX Aggregator for Permaswap and Botega"

-- Source identifiers for pools
Constants.SOURCE = {
  PERMASWAP = "permaswap",
  BOTEGA = "botega"
}

-- Time constants (in seconds)
Constants.TIME = {
  RESERVE_CACHE_EXPIRY = 120, -- 2 minutes
  POOL_DATA_REFRESH = 3600,   -- 1 hour
  GRAPH_REBUILD = 3600,       -- 1 hour
  MAX_REQUEST_TIMEOUT = 30    -- 30 seconds
}

-- Numeric constants
Constants.NUMERIC = {
  DECIMALS = 12, -- Default token decimal precision
  BASIS_POINTS_MULTIPLIER = 10000,
  PERCENTAGE_MULTIPLIER = 100,
  MIN_LIQUIDITY_THRESHOLD = "1000000",  -- Minimum reserves to consider a pool active
  DEFAULT_SLIPPAGE_TOLERANCE = 50,      -- 0.5% default slippage tolerance (in basis points)
  DEFAULT_MIN_PROFIT_BPS = 10,          -- 0.1% minimum profit threshold (in basis points)
  DEFAULT_MAX_HOPS = 3,                 -- Maximum number of hops in a path
  MAX_CYCLES_TO_ANALYZE = 100,          -- Maximum number of cycles to analyze for arbitrage
  DEFAULT_SWAP_INPUT = "1000000000000", -- 1 token with 12 decimals
  RESERVE_RATIO_LIMIT = 0.25            -- Max input amount as percentage of pool reserves (25%)
}

-- Error codes
Constants.ERROR = {
  POOL_NOT_FOUND = "ERR_POOL_NOT_FOUND",
  INSUFFICIENT_LIQUIDITY = "ERR_INSUFFICIENT_LIQUIDITY",
  INVALID_TOKEN = "ERR_INVALID_TOKEN",
  PATH_NOT_FOUND = "ERR_PATH_NOT_FOUND",
  REQUEST_TIMEOUT = "ERR_REQUEST_TIMEOUT",
  CALCULATION_FAILED = "ERR_CALCULATION_FAILED",
  DATABASE_ERROR = "ERR_DATABASE_ERROR",
  UNKNOWN_ERROR = "ERR_UNKNOWN",
  INVALID_PATH = "ERR_INVALID_PATH",
  EXCESSIVE_SLIPPAGE = "ERR_EXCESSIVE_SLIPPAGE"
}

-- API endpoints
Constants.API = {
  -- Permaswap endpoints
  PERMASWAP = {
    INFO = "Info",
    GET_AMOUNT_OUT = "GetAmountOut",
    REQUEST_ORDER = "RequestOrder",
    GET_ORDER = "GetOrder",
    BALANCE = "Balance"
  },

  -- Botega endpoints
  BOTEGA = {
    INFO = "Info",
    GET_PAIR = "Get-Pair",
    GET_RESERVES = "Get-Reserves",
    GET_FEE_PERCENTAGE = "Get-Fee-Percentage",
    GET_SWAP_OUTPUT = "Get-Swap-Output",
    CREDIT_NOTICE = "Credit-Notice",
    BALANCE = "Balance",
    TOTAL_SUPPLY = "Total-Supply"
  }
}

-- SQLite database constants
Constants.DB = {
  TABLES = {
    TOKENS = "tokens",
    POOLS = "pools",
    SWAPS = "swaps",
    RESERVES = "reserves"
  }
}

-- Path finding constants
Constants.PATH = {
  MAX_PATHS_TO_RETURN = 5,                                                -- Maximum number of paths to return in results
  MAX_PATH_LENGTH = 4,                                                    -- Maximum length of a path (including source)
  DEFAULT_ARBITRAGE_TOKEN = "xU9zFkq3X2ZQ6olwNVvr1vUWIjc3kXTWr7xKQD6dh10" -- Default token for arbitrage cycles (qAR)
}

-- Optimization constants
Constants.OPTIMIZATION = {
  BATCH_SIZE = 10,             -- Number of pools to update in a batch
  MAX_CONCURRENT_REQUESTS = 5, -- Maximum number of concurrent API requests
  CACHE_SIZE = 100,            -- Maximum size of reserve cache (number of pools)
  LOG_LEVEL = "INFO"           -- Default log level (DEBUG, INFO, WARN, ERROR)
}

-- Subscription constants
Constants.SUBSCRIPTION = {
  OPPORTUNITY_NOTIFICATION_COOLDOWN = 300, -- 5 minutes between notifications of similar opportunities
  MAX_SUBSCRIBERS = 100                    -- Maximum number of subscribers
}

return Constants
