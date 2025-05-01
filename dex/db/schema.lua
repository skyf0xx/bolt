local sqlite3 = require('lsqlite3')
local Constants = require('dex.utils.constants')
local Logger = require('dex.utils.logger')
Logger = Logger.createLogger("Schema")

local Schema = {}

-- Initialize database
function Schema.init()
  -- Check if we already have a valid db connection
  if Schema.db and pcall(function() return Schema.db:exec("SELECT 1") == sqlite3.OK end) then
    Logger.info("Reusing existing database connection")
    return Schema.db
  end

  local db

  Logger.info("Opening in-memory database")
  db = sqlite3.open_memory()

  if not db then
    Logger.error("Failed to open database")
    return nil, "Failed to open database"
  end

  -- Store the db connection in the Schema module
  Schema.db = db
  return db
end

-- Setup database schema
function Schema.setupSchema(db)
  Logger.info("Setting up database schema")

  -- Create tokens table
  local tokenResult = db:exec([[
        CREATE TABLE IF NOT EXISTS tokens (
            id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            name TEXT,
            decimals INTEGER NOT NULL,
            logo_url TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_tokens_symbol ON tokens(symbol);
    ]])

  if tokenResult ~= sqlite3.OK then
    Logger.error("Failed to create tokens table", db:errmsg())
    return false, "Failed to create tokens table: " .. db:errmsg()
  end

  -- Create pools table
  local poolResult = db:exec([[
        CREATE TABLE IF NOT EXISTS pools (
            id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            token_a_id TEXT NOT NULL,
            token_b_id TEXT NOT NULL,
            fee_bps INTEGER NOT NULL,
            status TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            FOREIGN KEY (token_a_id) REFERENCES tokens(id),
            FOREIGN KEY (token_b_id) REFERENCES tokens(id),
            UNIQUE(source, token_a_id, token_b_id)
        );
        CREATE INDEX IF NOT EXISTS idx_pools_source ON pools(source);
        CREATE INDEX IF NOT EXISTS idx_pools_token_a ON pools(token_a_id);
        CREATE INDEX IF NOT EXISTS idx_pools_token_b ON pools(token_b_id);
        CREATE INDEX IF NOT EXISTS idx_pools_pair ON pools(token_a_id, token_b_id);
    ]])

  if poolResult ~= sqlite3.OK then
    Logger.error("Failed to create pools table", db:errmsg())
    return false, "Failed to create pools table: " .. db:errmsg()
  end

  -- Create reserves table (cache of current reserves)
  local reservesResult = db:exec([[
        CREATE TABLE IF NOT EXISTS reserves (
            pool_id TEXT PRIMARY KEY,
            reserve_a TEXT NOT NULL,
            reserve_b TEXT NOT NULL,
            last_updated INTEGER NOT NULL,
            FOREIGN KEY (pool_id) REFERENCES pools(id)
        );
        CREATE INDEX IF NOT EXISTS idx_reserves_last_updated ON reserves(last_updated);
    ]])

  if reservesResult ~= sqlite3.OK then
    Logger.error("Failed to create reserves table", db:errmsg())
    return false, "Failed to create reserves table: " .. db:errmsg()
  end

  -- Create swaps table (history of completed swaps for analysis)
  local swapsResult = db:exec([[
        CREATE TABLE IF NOT EXISTS swaps (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            path TEXT NOT NULL,
            token_in_id TEXT NOT NULL,
            token_out_id TEXT NOT NULL,
            amount_in TEXT NOT NULL,
            amount_out TEXT NOT NULL,
            total_fee_bps INTEGER NOT NULL,
            execution_timestamp INTEGER NOT NULL,
            FOREIGN KEY (token_in_id) REFERENCES tokens(id),
            FOREIGN KEY (token_out_id) REFERENCES tokens(id)
        );
        CREATE INDEX IF NOT EXISTS idx_swaps_user ON swaps(user_id);
        CREATE INDEX IF NOT EXISTS idx_swaps_token_in ON swaps(token_in_id);
        CREATE INDEX IF NOT EXISTS idx_swaps_token_out ON swaps(token_out_id);
        CREATE INDEX IF NOT EXISTS idx_swaps_timestamp ON swaps(execution_timestamp);
    ]])

  if swapsResult ~= sqlite3.OK then
    Logger.error("Failed to create swaps table", db:errmsg())
    return false, "Failed to create swaps table: " .. db:errmsg()
  end

  -- Create arbitrage_opportunities table (detected opportunities for analysis)
  local opportunitiesResult = db:exec([[
        CREATE TABLE IF NOT EXISTS arbitrage_opportunities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cycle_path TEXT NOT NULL,
            profit_bps INTEGER NOT NULL,
            token_id TEXT NOT NULL,
            input_amount TEXT NOT NULL,
            detection_timestamp INTEGER NOT NULL,
            is_exploitable BOOLEAN NOT NULL DEFAULT 0,
            FOREIGN KEY (token_id) REFERENCES tokens(id)
        );
        CREATE INDEX IF NOT EXISTS idx_opportunities_token ON arbitrage_opportunities(token_id);
        CREATE INDEX IF NOT EXISTS idx_opportunities_timestamp ON arbitrage_opportunities(detection_timestamp);
        CREATE INDEX IF NOT EXISTS idx_opportunities_profit ON arbitrage_opportunities(profit_bps);
    ]])

  if opportunitiesResult ~= sqlite3.OK then
    Logger.error("Failed to create arbitrage_opportunities table", db:errmsg())
    return false, "Failed to create arbitrage_opportunities table: " .. db:errmsg()
  end

  Logger.info("Database schema setup complete")
  return true
end

-- Helper function to execute prepared statements with error handling
function Schema.execute(db, sql, params)
  local stmt = db:prepare(sql)

  if not stmt then
    Logger.error("Failed to prepare statement", db:errmsg())
    return false, "Failed to prepare statement: " .. db:errmsg()
  end

  if params then
    for i, v in ipairs(params) do
      stmt:bind(i, v)
    end
  end

  local result = stmt:step()
  stmt:finalize()

  if result ~= sqlite3.DONE then
    Logger.error("Failed to execute statement", db:errmsg())
    return false, "Failed to execute statement: " .. db:errmsg()
  end

  return true
end

-- Helper function to query data with error handling
function Schema.query(db, sql, params)
  local stmt = db:prepare(sql)

  if not stmt then
    Logger.error("Failed to prepare query", db:errmsg())
    return nil, "Failed to prepare query: " .. db:errmsg()
  end

  if params then
    for i, v in ipairs(params) do
      stmt:bind(i, v)
    end
  end

  local results = {}
  while stmt:step() == sqlite3.ROW do
    local row = {}
    for i = 0, stmt:columns() - 1 do
      local name = stmt:column_name(i)
      local value = stmt:column_value(i)
      row[name] = value
    end
    table.insert(results, row)
  end

  stmt:finalize()
  return results
end

-- Reset database (for testing)
function Schema.resetDatabase(db)
  Logger.warn("Resetting database - all data will be lost")

  local tables = { "arbitrage_opportunities", "swaps", "reserves", "pools", "tokens" }

  for _, table in ipairs(tables) do
    local result = db:exec("DELETE FROM " .. table)
    if result ~= sqlite3.OK then
      Logger.error("Failed to clear table " .. table, db:errmsg())
      return false, "Failed to clear table " .. table .. ": " .. db:errmsg()
    end
  end

  Logger.info("Database reset complete")
  return true
end

-- Close database connection
function Schema.close(db)
  if db then
    db:close()
    Schema.db = nil -- Clear the stored connection
    Logger.info("Database connection closed")
    return true
  end
  return false
end

return Schema
