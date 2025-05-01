local sqlite3 = require('lsqlite3')
local Constants = require('dex.utils.constants')
local Logger = require('dex.utils.logger')
local SqlAccessor = require('dex.utils.sql_accessor') -- Add this new helper
Logger = Logger.createLogger("TokenRepository")

local TokenRepository = {}

-- Add a new token or update if it already exists
function TokenRepository.addOrUpdateToken(db, token)
  if not token or not token.id then
    return false, "Token ID is required"
  end

  local currentTime = os.time()

  -- Check if token already exists
  local existing = TokenRepository.getToken(db, token.id)
  if existing then
    -- Update existing token
    local sql = [[
            UPDATE tokens
            SET symbol = ?, name = ?, decimals = ?, logo_url = ?, updated_at = ?
            WHERE id = ?
        ]]

    local params = {
      token.symbol or existing.symbol,
      token.name or existing.name,
      token.decimals or existing.decimals,
      token.logo_url or existing.logo_url,
      currentTime,
      token.id
    }

    local success, err = pcall(function()
      return db:execute(sql, params)
    end)

    if not success then
      Logger.error("Failed to update token", { id = token.id, error = err })
      return false, "Failed to update token: " .. tostring(err)
    end

    Logger.info("Updated token", { id = token.id, symbol = token.symbol })
    return true, "Token updated"
  else
    -- Insert new token
    local sql = [[
            INSERT INTO tokens (id, symbol, name, decimals, logo_url, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ]]

    local params = {
      token.id,
      token.symbol or "",
      token.name or "",
      token.decimals or Constants.NUMERIC.DECIMALS,
      token.logo_url or "",
      currentTime,
      currentTime
    }

    local success, err = pcall(function()
      return db:execute(sql, params)
    end)

    if not success then
      Logger.error("Failed to insert token", { id = token.id, error = err, params = params })
      return false, "Failed to insert token: " .. tostring(err)
    end

    Logger.info("Added new token", { id = token.id, symbol = token.symbol })
    return true, "Token added"
  end
end

-- Get a token by ID
function TokenRepository.getToken(db, tokenId)
  if not tokenId then
    return nil, "Token ID is required"
  end

  local sql = "SELECT * FROM tokens WHERE id = ?"
  local stmt = db:prepare(sql)

  if not stmt then
    Logger.error("Failed to prepare statement", db:errmsg())
    return nil, "Failed to prepare statement: " .. db:errmsg()
  end

  stmt:bind(1, tokenId)

  local result = nil
  if stmt:step() == sqlite3.ROW then
    result = {
      id = SqlAccessor.column_text(stmt, 0),
      symbol = SqlAccessor.column_text(stmt, 1),
      name = SqlAccessor.column_text(stmt, 2),
      decimals = SqlAccessor.column_int(stmt, 3),
      logo_url = SqlAccessor.column_text(stmt, 4),
      created_at = SqlAccessor.column_int(stmt, 5),
      updated_at = SqlAccessor.column_int(stmt, 6)
    }
  end

  stmt:finalize()
  return result
end

-- Get a token by symbol
function TokenRepository.getTokenBySymbol(db, symbol)
  if not symbol then
    return nil, "Symbol is required"
  end

  local sql = "SELECT * FROM tokens WHERE symbol = ? COLLATE NOCASE LIMIT 1"
  local stmt = db:prepare(sql)

  if not stmt then
    Logger.error("Failed to prepare statement", db:errmsg())
    return nil, "Failed to prepare statement: " .. db:errmsg()
  end

  stmt:bind(1, symbol)

  local result = nil
  if stmt:step() == sqlite3.ROW then
    result = {
      id = SqlAccessor.column_text(stmt, 0),
      symbol = SqlAccessor.column_text(stmt, 1),
      name = SqlAccessor.column_text(stmt, 2),
      decimals = SqlAccessor.column_int(stmt, 3),
      logo_url = SqlAccessor.column_text(stmt, 4),
      created_at = SqlAccessor.column_int(stmt, 5),
      updated_at = SqlAccessor.column_int(stmt, 6)
    }
  end

  stmt:finalize()
  return result
end

-- Get all tokens
function TokenRepository.getAllTokens(db)
  local sql = "SELECT * FROM tokens ORDER BY symbol"
  local stmt = db:prepare(sql)

  if not stmt then
    Logger.error("Failed to prepare statement", db:errmsg())
    return {}, "Failed to prepare statement: " .. db:errmsg()
  end

  local tokens = {}
  while stmt:step() == sqlite3.ROW do
    table.insert(tokens, {
      id = SqlAccessor.column_text(stmt, 0),
      symbol = SqlAccessor.column_text(stmt, 1),
      name = SqlAccessor.column_text(stmt, 2),
      decimals = SqlAccessor.column_int(stmt, 3),
      logo_url = SqlAccessor.column_text(stmt, 4),
      created_at = SqlAccessor.column_int(stmt, 5),
      updated_at = SqlAccessor.column_int(stmt, 6)
    })
  end

  stmt:finalize()
  return tokens
end

-- Search tokens by symbol or name
function TokenRepository.searchTokens(db, query)
  if not query or query == "" then
    return TokenRepository.getAllTokens(db)
  end

  local searchQuery = "%" .. query .. "%"
  local sql = "SELECT * FROM tokens WHERE symbol LIKE ? COLLATE NOCASE OR name LIKE ? COLLATE NOCASE ORDER BY symbol"
  local stmt = db:prepare(sql)

  if not stmt then
    Logger.error("Failed to prepare statement", db:errmsg())
    return {}, "Failed to prepare statement: " .. db:errmsg()
  end

  stmt:bind(1, searchQuery)
  stmt:bind(2, searchQuery)

  local tokens = {}
  while stmt:step() == sqlite3.ROW do
    table.insert(tokens, {
      id = SqlAccessor.column_text(stmt, 0),
      symbol = SqlAccessor.column_text(stmt, 1),
      name = SqlAccessor.column_text(stmt, 2),
      decimals = SqlAccessor.column_int(stmt, 3),
      logo_url = SqlAccessor.column_text(stmt, 4),
      created_at = SqlAccessor.column_int(stmt, 5),
      updated_at = SqlAccessor.column_int(stmt, 6)
    })
  end

  stmt:finalize()
  return tokens
end

-- Delete a token (be careful with this due to foreign key constraints)
function TokenRepository.deleteToken(db, tokenId)
  if not tokenId then
    return false, "Token ID is required"
  end

  -- Begin transaction to ensure atomicity
  db:exec("BEGIN TRANSACTION")

  -- First check if token is used in any pools
  local checkSql = "SELECT COUNT(*) FROM pools WHERE token_a_id = ? OR token_b_id = ?"
  local checkStmt = db:prepare(checkSql)

  if not checkStmt then
    db:exec("ROLLBACK")
    Logger.error("Failed to prepare statement", db:errmsg())
    return false, "Failed to prepare statement: " .. db:errmsg()
  end

  checkStmt:bind(1, tokenId)
  checkStmt:bind(2, tokenId)

  local count = 0
  if checkStmt:step() == sqlite3.ROW then
    count = SqlAccessor.column_int(checkStmt, 0) or 0
  end

  checkStmt:finalize()

  if count > 0 then
    db:exec("ROLLBACK")
    Logger.warn("Cannot delete token, it is used in pools", { id = tokenId, poolCount = count })
    return false, "Cannot delete token, it is used in " .. count .. " pools"
  end

  -- Delete the token
  local sql = "DELETE FROM tokens WHERE id = ?"
  local stmt = db:prepare(sql)

  if not stmt then
    db:exec("ROLLBACK")
    Logger.error("Failed to prepare statement", db:errmsg())
    return false, "Failed to prepare statement: " .. db:errmsg()
  end

  stmt:bind(1, tokenId)

  local result = stmt:step()
  stmt:finalize()

  if result ~= sqlite3.DONE then
    db:exec("ROLLBACK")
    Logger.error("Failed to delete token", { id = tokenId, error = db:errmsg() })
    return false, "Failed to delete token: " .. db:errmsg()
  end

  db:exec("COMMIT")
  Logger.info("Deleted token", { id = tokenId })
  return true, "Token deleted"
end

-- Batch insert tokens for initial setup or updates
function TokenRepository.batchInsertTokens(db, tokens)
  if not tokens or #tokens == 0 then
    return true, "No tokens to insert"
  end

  local currentTime = os.time()
  local successCount = 0
  local errorCount = 0

  -- Begin transaction for better performance
  db:exec("BEGIN TRANSACTION")

  local sql = [[
        INSERT OR REPLACE INTO tokens
        (id, symbol, name, decimals, logo_url, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ]]

  local stmt = db:prepare(sql)

  if not stmt then
    db:exec("ROLLBACK")
    Logger.error("Failed to prepare statement", db:errmsg())
    return false, "Failed to prepare statement: " .. db:errmsg()
  end

  for _, token in ipairs(tokens) do
    stmt:reset()

    local created_at = currentTime

    -- Check if token already exists
    local existing = TokenRepository.getToken(db, token.id)
    if existing then
      created_at = existing.created_at
    end

    -- Bind parameters
    stmt:bind(1, token.id)
    stmt:bind(2, token.symbol or "")
    stmt:bind(3, token.name or "")
    stmt:bind(4, token.decimals or Constants.NUMERIC.DECIMALS)
    stmt:bind(5, token.logo_url or "")
    stmt:bind(6, created_at)
    stmt:bind(7, currentTime)

    local result = stmt:step()

    if result ~= sqlite3.DONE then
      Logger.error("Failed to insert token",
        { id = token.id, error = db:errmsg(), params = { token.id, token.symbol, token.name, token.decimals, token.logo_url, created_at, currentTime } })
      errorCount = errorCount + 1
    else
      successCount = successCount + 1
    end
  end

  stmt:finalize()

  -- Commit transaction
  db:exec("COMMIT")

  Logger.info("Batch insert completed", { total = #tokens, success = successCount, errors = errorCount })

  if errorCount > 0 then
    return false, "Completed with " .. errorCount .. " errors out of " .. #tokens .. " tokens"
  else
    return true, "Successfully inserted " .. successCount .. " tokens"
  end
end

-- Get tokens by multiple IDs
function TokenRepository.getTokensByIds(db, tokenIds)
  if not tokenIds or #tokenIds == 0 then
    return {}
  end

  -- Build SQL query with placeholders for all IDs
  local placeholders = {}
  for i = 1, #tokenIds do
    table.insert(placeholders, "?")
  end

  local sql = "SELECT * FROM tokens WHERE id IN (" .. table.concat(placeholders, ",") .. ")"
  local stmt = db:prepare(sql)

  if not stmt then
    Logger.error("Failed to prepare statement", db:errmsg())
    return {}
  end

  -- Bind all token IDs
  for i, id in ipairs(tokenIds) do
    stmt:bind(i, id)
  end

  local tokens = {}
  while stmt:step() == sqlite3.ROW do
    table.insert(tokens, {
      id = SqlAccessor.column_text(stmt, 0),
      symbol = SqlAccessor.column_text(stmt, 1),
      name = SqlAccessor.column_text(stmt, 2),
      decimals = SqlAccessor.column_int(stmt, 3),
      logo_url = SqlAccessor.column_text(stmt, 4),
      created_at = SqlAccessor.column_int(stmt, 5),
      updated_at = SqlAccessor.column_int(stmt, 6)
    })
  end

  stmt:finalize()
  return tokens
end

-- Get token statistics
function TokenRepository.getTokenStatistics(db)
  local sql = [[
        SELECT
            COUNT(*) as total_tokens,
            MIN(created_at) as oldest_token_timestamp,
            MAX(created_at) as newest_token_timestamp,
            MAX(updated_at) as last_updated_timestamp
        FROM tokens
    ]]

  local stmt = db:prepare(sql)

  if not stmt then
    Logger.error("Failed to prepare statement", db:errmsg())
    return nil, "Failed to prepare statement: " .. db:errmsg()
  end

  local stats = nil
  if stmt:step() == sqlite3.ROW then
    stats = {
      total_tokens = SqlAccessor.column_int(stmt, 0),
      oldest_token_timestamp = SqlAccessor.column_int(stmt, 1),
      newest_token_timestamp = SqlAccessor.column_int(stmt, 2),
      last_updated_timestamp = SqlAccessor.column_int(stmt, 3)
    }
  end

  stmt:finalize()
  return stats
end

return TokenRepository
