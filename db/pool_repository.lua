local sqlite3 = require('lsqlite3')
local Constants = require('arbitrage.constants')
local Logger = require('arbitrage.logger').createLogger("PoolRepository")
local Utils = require('arbitrage.utils')
local TokenRepository = require('arbitrage.db.token_repository')

local PoolRepository = {}

-- Add a new pool or update if it already exists
function PoolRepository.addOrUpdatePool(db, pool)
  if not pool or not pool.id then
    return false, "Pool ID is required"
  end

  if not pool.token_a_id or not pool.token_b_id then
    return false, "Both token_a_id and token_b_id are required"
  end

  local currentTime = os.time()

  -- Check if pool already exists
  local existing = PoolRepository.getPool(db, pool.id)
  if existing then
    -- Update existing pool
    local sql = [[
            UPDATE pools
            SET source = ?, token_a_id = ?, token_b_id = ?, fee_bps = ?, status = ?, updated_at = ?
            WHERE id = ?
        ]]

    local params = {
      pool.source or existing.source,
      pool.token_a_id or existing.token_a_id,
      pool.token_b_id or existing.token_b_id,
      pool.fee_bps or existing.fee_bps,
      pool.status or existing.status,
      currentTime,
      pool.id
    }

    local success, err = pcall(function()
      return db:execute(sql, params)
    end)

    if not success then
      Logger.error("Failed to update pool", { id = pool.id, error = err })
      return false, "Failed to update pool: " .. tostring(err)
    end

    Logger.info("Updated pool", { id = pool.id, source = pool.source })
    return true, "Pool updated"
  else
    -- Insert new pool
    local sql = [[
            INSERT INTO pools (id, source, token_a_id, token_b_id, fee_bps, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ]]

    local params = {
      pool.id,
      pool.source or "",
      pool.token_a_id,
      pool.token_b_id,
      pool.fee_bps or 0,
      pool.status or "active",
      currentTime,
      currentTime
    }

    local success, err = pcall(function()
      return db:execute(sql, params)
    end)

    if not success then
      Logger.error("Failed to insert pool", { id = pool.id, error = err })
      return false, "Failed to insert pool: " .. tostring(err)
    end

    Logger.info("Added new pool", { id = pool.id, source = pool.source })
    return true, "Pool added"
  end
end

-- Get a pool by ID
function PoolRepository.getPool(db, poolId)
  if not poolId then
    return nil, "Pool ID is required"
  end

  local sql = "SELECT * FROM pools WHERE id = ?"
  local stmt = db:prepare(sql)

  if not stmt then
    Logger.error("Failed to prepare statement", db:errmsg())
    return nil, "Failed to prepare statement: " .. db:errmsg()
  end

  stmt:bind(1, poolId)

  local result = nil
  if stmt:step() == sqlite3.ROW then
    result = {
      id = stmt:column_text(0),
      source = stmt:column_text(1),
      token_a_id = stmt:column_text(2),
      token_b_id = stmt:column_text(3),
      fee_bps = stmt:column_int(4),
      status = stmt:column_text(5),
      created_at = stmt:column_int(6),
      updated_at = stmt:column_int(7)
    }
  end

  stmt:finalize()
  return result
end

-- Get all pools
function PoolRepository.getAllPools(db)
  local sql = "SELECT * FROM pools ORDER BY source, id"
  local stmt = db:prepare(sql)

  if not stmt then
    Logger.error("Failed to prepare statement", db:errmsg())
    return {}, "Failed to prepare statement: " .. db:errmsg()
  end

  local pools = {}
  while stmt:step() == sqlite3.ROW do
    table.insert(pools, {
      id = stmt:column_text(0),
      source = stmt:column_text(1),
      token_a_id = stmt:column_text(2),
      token_b_id = stmt:column_text(3),
      fee_bps = stmt:column_int(4),
      status = stmt:column_text(5),
      created_at = stmt:column_int(6),
      updated_at = stmt:column_int(7)
    })
  end

  stmt:finalize()
  return pools
end

-- Get pools by source
function PoolRepository.getPoolsBySource(db, source)
  if not source then
    return {}, "Source is required"
  end

  local sql = "SELECT * FROM pools WHERE source = ? ORDER BY id"
  local stmt = db:prepare(sql)

  if not stmt then
    Logger.error("Failed to prepare statement", db:errmsg())
    return {}, "Failed to prepare statement: " .. db:errmsg()
  end

  stmt:bind(1, source)

  local pools = {}
  while stmt:step() == sqlite3.ROW do
    table.insert(pools, {
      id = stmt:column_text(0),
      source = stmt:column_text(1),
      token_a_id = stmt:column_text(2),
      token_b_id = stmt:column_text(3),
      fee_bps = stmt:column_int(4),
      status = stmt:column_text(5),
      created_at = stmt:column_int(6),
      updated_at = stmt:column_int(7)
    })
  end

  stmt:finalize()
  return pools
end

-- Find pools by token
function PoolRepository.getPoolsByToken(db, tokenId)
  if not tokenId then
    return {}, "Token ID is required"
  end

  local sql = "SELECT * FROM pools WHERE token_a_id = ? OR token_b_id = ? ORDER BY source, id"
  local stmt = db:prepare(sql)

  if not stmt then
    Logger.error("Failed to prepare statement", db:errmsg())
    return {}, "Failed to prepare statement: " .. db:errmsg()
  end

  stmt:bind(1, tokenId)
  stmt:bind(2, tokenId)

  local pools = {}
  while stmt:step() == sqlite3.ROW do
    table.insert(pools, {
      id = stmt:column_text(0),
      source = stmt:column_text(1),
      token_a_id = stmt:column_text(2),
      token_b_id = stmt:column_text(3),
      fee_bps = stmt:column_int(4),
      status = stmt:column_text(5),
      created_at = stmt:column_int(6),
      updated_at = stmt:column_int(7)
    })
  end

  stmt:finalize()
  return pools
end

-- Find pool by token pair
function PoolRepository.getPoolByTokenPair(db, tokenAId, tokenBId, source)
  if not tokenAId or not tokenBId then
    return nil, "Both token IDs are required"
  end

  local sql
  local params

  if source then
    sql = [[
            SELECT * FROM pools
            WHERE ((token_a_id = ? AND token_b_id = ?) OR (token_a_id = ? AND token_b_id = ?))
            AND source = ?
            LIMIT 1
        ]]
    params = { tokenAId, tokenBId, tokenBId, tokenAId, source }
  else
    sql = [[
            SELECT * FROM pools
            WHERE (token_a_id = ? AND token_b_id = ?) OR (token_a_id = ? AND token_b_id = ?)
            LIMIT 1
        ]]
    params = { tokenAId, tokenBId, tokenBId, tokenAId }
  end

  local stmt = db:prepare(sql)

  if not stmt then
    Logger.error("Failed to prepare statement", db:errmsg())
    return nil, "Failed to prepare statement: " .. db:errmsg()
  end

  for i, param in ipairs(params) do
    stmt:bind(i, param)
  end

  local result = nil
  if stmt:step() == sqlite3.ROW then
    result = {
      id = stmt:column_text(0),
      source = stmt:column_text(1),
      token_a_id = stmt:column_text(2),
      token_b_id = stmt:column_text(3),
      fee_bps = stmt:column_int(4),
      status = stmt:column_text(5),
      created_at = stmt:column_int(6),
      updated_at = stmt:column_int(7)
    }
  end

  stmt:finalize()
  return result
end

-- Delete a pool
function PoolRepository.deletePool(db, poolId)
  if not poolId then
    return false, "Pool ID is required"
  end

  -- Begin transaction to ensure atomicity
  db:exec("BEGIN TRANSACTION")

  -- First delete any reserve entries for this pool
  local deleteReservesSql = "DELETE FROM reserves WHERE pool_id = ?"
  local reservesStmt = db:prepare(deleteReservesSql)

  if not reservesStmt then
    db:exec("ROLLBACK")
    Logger.error("Failed to prepare statement", db:errmsg())
    return false, "Failed to prepare statement: " .. db:errmsg()
  end

  reservesStmt:bind(1, poolId)
  local reservesResult = reservesStmt:step()
  reservesStmt:finalize()

  -- Now delete the pool
  local sql = "DELETE FROM pools WHERE id = ?"
  local stmt = db:prepare(sql)

  if not stmt then
    db:exec("ROLLBACK")
    Logger.error("Failed to prepare statement", db:errmsg())
    return false, "Failed to prepare statement: " .. db:errmsg()
  end

  stmt:bind(1, poolId)

  local result = stmt:step()
  stmt:finalize()

  if result ~= sqlite3.DONE then
    db:exec("ROLLBACK")
    Logger.error("Failed to delete pool", { id = poolId, error = db:errmsg() })
    return false, "Failed to delete pool: " .. db:errmsg()
  end

  db:exec("COMMIT")
  Logger.info("Deleted pool", { id = poolId })
  return true, "Pool deleted"
end

-- Batch insert pools for initial setup or updates
function PoolRepository.batchInsertPools(db, pools)
  if not pools or #pools == 0 then
    return true, "No pools to insert"
  end

  local currentTime = os.time()
  local successCount = 0
  local errorCount = 0

  -- Begin transaction for better performance
  db:exec("BEGIN TRANSACTION")

  local sql = [[
        INSERT OR REPLACE INTO pools
        (id, source, token_a_id, token_b_id, fee_bps, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ]]

  local stmt = db:prepare(sql)

  if not stmt then
    db:exec("ROLLBACK")
    Logger.error("Failed to prepare statement", db:errmsg())
    return false, "Failed to prepare statement: " .. db:errmsg()
  end

  for _, pool in ipairs(pools) do
    stmt:reset()

    local created_at = currentTime

    -- Check if pool already exists
    local existing = PoolRepository.getPool(db, pool.id)
    if existing then
      created_at = existing.created_at
    end

    -- Bind parameters
    stmt:bind(1, pool.id)
    stmt:bind(2, pool.source or "")
    stmt:bind(3, pool.token_a_id)
    stmt:bind(4, pool.token_b_id)
    stmt:bind(5, pool.fee_bps or 0)
    stmt:bind(6, pool.status or "active")
    stmt:bind(7, created_at)
    stmt:bind(8, currentTime)

    local result = stmt:step()

    if result ~= sqlite3.DONE then
      Logger.warn("Failed to insert pool", { id = pool.id, error = db:errmsg() })
      errorCount = errorCount + 1
    else
      successCount = successCount + 1
    end
  end

  stmt:finalize()

  -- Commit transaction
  db:exec("COMMIT")

  Logger.info("Batch insert completed", { total = #pools, success = successCount, errors = errorCount })

  if errorCount > 0 then
    return false, "Completed with " .. errorCount .. " errors out of " .. #pools .. " pools"
  else
    return true, "Successfully inserted " .. successCount .. " pools"
  end
end

-- Get pools with full token information
function PoolRepository.getPoolsWithTokenInfo(db)
  local sql = [[
        SELECT
            p.id, p.source, p.fee_bps, p.status,
            ta.id as token_a_id, ta.symbol as token_a_symbol, ta.name as token_a_name, ta.decimals as token_a_decimals, ta.logo_url as token_a_logo,
            tb.id as token_b_id, tb.symbol as token_b_symbol, tb.name as token_b_name, tb.decimals as token_b_decimals, tb.logo_url as token_b_logo
        FROM pools p
        JOIN tokens ta ON p.token_a_id = ta.id
        JOIN tokens tb ON p.token_b_id = tb.id
        ORDER BY p.source, ta.symbol, tb.symbol
    ]]

  local stmt = db:prepare(sql)

  if not stmt then
    Logger.error("Failed to prepare statement", db:errmsg())
    return {}, "Failed to prepare statement: " .. db:errmsg()
  end

  local pools = {}
  while stmt:step() == sqlite3.ROW do
    table.insert(pools, {
      id = stmt:column_text(0),
      source = stmt:column_text(1),
      fee_bps = stmt:column_int(2),
      status = stmt:column_text(3),
      token_a = {
        id = stmt:column_text(4),
        symbol = stmt:column_text(5),
        name = stmt:column_text(6),
        decimals = stmt:column_int(7),
        logo_url = stmt:column_text(8)
      },
      token_b = {
        id = stmt:column_text(9),
        symbol = stmt:column_text(10),
        name = stmt:column_text(11),
        decimals = stmt:column_int(12),
        logo_url = stmt:column_text(13)
      }
    })
  end

  stmt:finalize()
  return pools
end

-- Update or insert pool reserves
function PoolRepository.updateReserves(db, poolId, reserveA, reserveB)
  if not poolId then
    return false, "Pool ID is required"
  end

  if not reserveA or not reserveB then
    return false, "Both reserve values are required"
  end

  local currentTime = os.time()

  -- Check if reserves entry already exists
  local checkSql = "SELECT COUNT(*) FROM reserves WHERE pool_id = ?"
  local checkStmt = db:prepare(checkSql)

  if not checkStmt then
    Logger.error("Failed to prepare statement", db:errmsg())
    return false, "Failed to prepare statement: " .. db:errmsg()
  end

  checkStmt:bind(1, poolId)

  local exists = false
  if checkStmt:step() == sqlite3.ROW then
    exists = checkStmt:column_int(0) > 0
  end

  checkStmt:finalize()

  local sql
  if exists then
    sql = [[
            UPDATE reserves
            SET reserve_a = ?, reserve_b = ?, last_updated = ?
            WHERE pool_id = ?
        ]]
  else
    sql = [[
            INSERT INTO reserves
            (pool_id, reserve_a, reserve_b, last_updated)
            VALUES (?, ?, ?, ?)
        ]]
  end

  local stmt = db:prepare(sql)

  if not stmt then
    Logger.error("Failed to prepare statement", db:errmsg())
    return false, "Failed to prepare statement: " .. db:errmsg()
  end

  if exists then
    stmt:bind(1, tostring(reserveA))
    stmt:bind(2, tostring(reserveB))
    stmt:bind(3, currentTime)
    stmt:bind(4, poolId)
  else
    stmt:bind(1, poolId)
    stmt:bind(2, tostring(reserveA))
    stmt:bind(3, tostring(reserveB))
    stmt:bind(4, currentTime)
  end

  local result = stmt:step()
  stmt:finalize()

  if result ~= sqlite3.DONE then
    Logger.error("Failed to update reserves", { id = poolId, error = db:errmsg() })
    return false, "Failed to update reserves: " .. db:errmsg()
  end

  Logger.debug("Updated reserves", { id = poolId, reserveA = reserveA, reserveB = reserveB })
  return true, "Reserves updated"
end

-- Get pool reserves
function PoolRepository.getReserves(db, poolId)
  if not poolId then
    return nil, "Pool ID is required"
  end

  local sql = "SELECT reserve_a, reserve_b, last_updated FROM reserves WHERE pool_id = ?"
  local stmt = db:prepare(sql)

  if not stmt then
    Logger.error("Failed to prepare statement", db:errmsg())
    return nil, "Failed to prepare statement: " .. db:errmsg()
  end

  stmt:bind(1, poolId)

  local result = nil
  if stmt:step() == sqlite3.ROW then
    result = {
      reserve_a = stmt:column_text(0),
      reserve_b = stmt:column_text(1),
      last_updated = stmt:column_int(2)
    }
  end

  stmt:finalize()
  return result
end

-- Get all reserves
function PoolRepository.getAllReserves(db)
  local sql = [[
        SELECT r.pool_id, r.reserve_a, r.reserve_b, r.last_updated,
               p.token_a_id, p.token_b_id
        FROM reserves r
        JOIN pools p ON r.pool_id = p.id
    ]]

  local stmt = db:prepare(sql)

  if not stmt then
    Logger.error("Failed to prepare statement", db:errmsg())
    return {}, "Failed to prepare statement: " .. db:errmsg()
  end

  local reserves = {}
  while stmt:step() == sqlite3.ROW do
    reserves[stmt:column_text(0)] = {
      pool_id = stmt:column_text(0),
      reserve_a = stmt:column_text(1),
      reserve_b = stmt:column_text(2),
      last_updated = stmt:column_int(3),
      token_a_id = stmt:column_text(4),
      token_b_id = stmt:column_text(5)
    }
  end

  stmt:finalize()
  return reserves
end

-- Check if reserves need refreshing
function PoolRepository.needsReserveRefresh(db, poolId, maxAge)
  maxAge = maxAge or Constants.TIME.RESERVE_CACHE_EXPIRY

  if not poolId then
    return true, "Pool ID is required"
  end

  local sql = "SELECT last_updated FROM reserves WHERE pool_id = ?"
  local stmt = db:prepare(sql)

  if not stmt then
    Logger.error("Failed to prepare statement", db:errmsg())
    return true, "Failed to prepare statement: " .. db:errmsg()
  end

  stmt:bind(1, poolId)

  local needsRefresh = true
  if stmt:step() == sqlite3.ROW then
    local lastUpdated = stmt:column_int(0)
    local currentTime = os.time()
    needsRefresh = (currentTime - lastUpdated) > maxAge
  end

  stmt:finalize()
  return needsRefresh
end

-- Get pools that need reserve refresh
function PoolRepository.getPoolsNeedingReserveRefresh(db, maxAge)
  maxAge = maxAge or Constants.TIME.RESERVE_CACHE_EXPIRY
  local currentTime = os.time()

  local sql = [[
        SELECT p.id, p.source, p.token_a_id, p.token_b_id, p.fee_bps, p.status
        FROM pools p
        LEFT JOIN reserves r ON p.id = r.pool_id
        WHERE r.pool_id IS NULL OR (? - r.last_updated) > ?
        ORDER BY r.last_updated ASC NULLS FIRST
        LIMIT ?
    ]]

  local stmt = db:prepare(sql)

  if not stmt then
    Logger.error("Failed to prepare statement", db:errmsg())
    return {}, "Failed to prepare statement: " .. db:errmsg()
  end

  stmt:bind(1, currentTime)
  stmt:bind(2, maxAge)
  stmt:bind(3, Constants.OPTIMIZATION.BATCH_SIZE)

  local pools = {}
  while stmt:step() == sqlite3.ROW do
    table.insert(pools, {
      id = stmt:column_text(0),
      source = stmt:column_text(1),
      token_a_id = stmt:column_text(2),
      token_b_id = stmt:column_text(3),
      fee_bps = stmt:column_int(4),
      status = stmt:column_text(5)
    })
  end

  stmt:finalize()
  return pools
end

-- Get pool statistics
function PoolRepository.getPoolStatistics(db)
  local sql = [[
        SELECT
            COUNT(*) as total_pools,
            COUNT(DISTINCT source) as source_count,
            MIN(created_at) as oldest_pool_timestamp,
            MAX(created_at) as newest_pool_timestamp,
            MAX(updated_at) as last_updated_timestamp,
            (SELECT COUNT(*) FROM reserves) as reserves_count
        FROM pools
    ]]

  local stmt = db:prepare(sql)

  if not stmt then
    Logger.error("Failed to prepare statement", db:errmsg())
    return nil, "Failed to prepare statement: " .. db:errmsg()
  end

  local stats = nil
  if stmt:step() == sqlite3.ROW then
    stats = {
      total_pools = stmt:column_int(0),
      source_count = stmt:column_int(1),
      oldest_pool_timestamp = stmt:column_int(2),
      newest_pool_timestamp = stmt:column_int(3),
      last_updated_timestamp = stmt:column_int(4),
      reserves_count = stmt:column_int(5)
    }
  end

  stmt:finalize()

  -- Get source statistics
  local sourceSql = [[
        SELECT source, COUNT(*) as count
        FROM pools
        GROUP BY source
        ORDER BY count DESC
    ]]

  local sourceStmt = db:prepare(sourceSql)

  if not sourceStmt then
    Logger.error("Failed to prepare statement", db:errmsg())
    return stats
  end

  stats.sources = {}
  while sourceStmt:step() == sqlite3.ROW do
    stats.sources[sourceStmt:column_text(0)] = sourceStmt:column_int(1)
  end

  sourceStmt:finalize()
  return stats
end

-- Find pools that connect two tokens (direct or indirect)
function PoolRepository.findConnectingPools(db, tokenIdA, tokenIdB, maxHops)
  maxHops = maxHops or Constants.PATH.MAX_PATH_LENGTH

  if not tokenIdA or not tokenIdB then
    return {}, "Both token IDs are required"
  end

  if tokenIdA == tokenIdB then
    return {}, "Source and destination tokens must be different"
  end

  -- Start with direct connections (1 hop)
  local directPools = PoolRepository.getPoolByTokenPair(db, tokenIdA, tokenIdB)
  if directPools then
    return { directPools }, "Direct connection found"
  end

  -- If maxHops is 1 or less, we're done
  if maxHops <= 1 then
    return {}, "No direct connection found and max hops is 1"
  end

  -- Get all pools involving tokenA
  local aPoolsSql = [[
        SELECT p.*, t.id as connecting_token_id
        FROM pools p
        JOIN tokens t ON
            (p.token_a_id = ? AND p.token_b_id = t.id) OR
            (p.token_b_id = ? AND p.token_a_id = t.id)
        WHERE p.status = 'active'
    ]]

  local aStmt = db:prepare(aPoolsSql)

  if not aStmt then
    Logger.error("Failed to prepare statement", db:errmsg())
    return {}, "Failed to prepare statement: " .. db:errmsg()
  end

  aStmt:bind(1, tokenIdA)
  aStmt:bind(2, tokenIdA)

  local aPools = {}
  while aStmt:step() == sqlite3.ROW do
    table.insert(aPools, {
      id = aStmt:column_text(0),
      source = aStmt:column_text(1),
      token_a_id = aStmt:column_text(2),
      token_b_id = aStmt:column_text(3),
      fee_bps = aStmt:column_int(4),
      status = aStmt:column_text(5),
      connecting_token_id = aStmt:column_text(8)
    })
  end

  aStmt:finalize()

  -- For each connecting token, see if it connects to tokenB
  local results = {}

  for _, aPool in ipairs(aPools) do
    local connectingTokenId = aPool.connecting_token_id

    -- Check if there's a direct connection from this token to tokenB
    local bPool = PoolRepository.getPoolByTokenPair(db, connectingTokenId, tokenIdB)

    if bPool then
      table.insert(results, { aPool, bPool })
    elseif maxHops > 2 then
      -- For longer paths, we'd need a more sophisticated algorithm (like BFS)
      -- This is simplified for the example but would need expansion in production
      local connectingPools = PoolRepository.findConnectingPools(
        db, connectingTokenId, tokenIdB, maxHops - 1)

      if #connectingPools > 0 then
        for _, path in ipairs(connectingPools) do
          local newPath = { aPool }
          for _, p in ipairs(path) do
            table.insert(newPath, p)
          end
          table.insert(results, newPath)
        end
      end
    end
  end

  return results
end

return PoolRepository
