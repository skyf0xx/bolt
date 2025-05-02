-- Advanced SQL Column Access Helper
-- This module provides comprehensive compatibility functions for SQLite operations
-- when the standard column_text and column_int methods are not available

local sqlite3 = require('lsqlite3')
local Logger = require('dex.utils.logger')
Logger = Logger.createLogger("SqlAccessor")

local SqlAccessor = {}

-- Type constants to match column types
SqlAccessor.TYPE = {
  INTEGER = 1,
  FLOAT = 2,
  TEXT = 3,
  BLOB = 4,
  NULL = 5
}

-- Helper function to get column value as text
function SqlAccessor.column_text(stmt, index)
  if not stmt or not index then return nil end

  local value = stmt:get_value(index)
  if value == nil then return nil end

  return tostring(value)
end

-- Helper function to get column value as integer
function SqlAccessor.column_int(stmt, index)
  if not stmt or not index then return 0 end

  local value = stmt:get_value(index)
  if value == nil then return 0 end

  return tonumber(value) or 0
end

-- Helper function to get column value based on its type
function SqlAccessor.column_value(stmt, index)
  if not stmt then return nil end

  local value = stmt:get_value(index)
  local valType = stmt:get_type(index)

  if valType == SqlAccessor.TYPE.NULL then
    return nil
  elseif valType == SqlAccessor.TYPE.INTEGER or valType == SqlAccessor.TYPE.FLOAT then
    return tonumber(value) or 0
  else
    return tostring(value)
  end
end

-- Create a row object from statement columns - more flexible approach
function SqlAccessor.create_row(stmt)
  if not stmt then return {} end

  local row = {}
  for i = 0, stmt:columns() - 1 do
    local name = stmt:get_name(i)
    local value = SqlAccessor.column_value(stmt, i)
    row[name] = value
  end

  return row
end

-- Execute a query and get multiple rows with automatic column mapping
function SqlAccessor.query(db, sql, params)
  if not db then
    Logger.error("Database connection is required")
    return nil, "Database connection is required"
  end

  local stmt = db:prepare(sql)
  if not stmt then
    Logger.error("Failed to prepare statement", db:errmsg())
    return nil, "Failed to prepare statement: " .. db:errmsg()
  end

  -- Bind parameters if provided
  if params then
    if type(params) == "table" then
      -- Array style params (indexed numerically)
      if #params > 0 then
        for i, param in ipairs(params) do
          stmt:bind(i, param)
        end
        -- Dict style params (named parameters)
      elseif next(params) then
        for name, value in pairs(params) do
          stmt:bind_names(params)
          break -- bind_names handles all named parameters at once
        end
      end
    end
  end

  local results = {}
  while stmt:step() == sqlite3.ROW do
    table.insert(results, SqlAccessor.create_row(stmt))
  end

  stmt:finalize()
  return results
end

-- Execute a query and get a single row
function SqlAccessor.query_row(db, sql, params)
  local results, err = SqlAccessor.query(db, sql, params)
  if not results then
    return nil, err
  end

  return results[1]
end

-- Execute a query and get a single value
function SqlAccessor.query_value(db, sql, params)
  local row, err = SqlAccessor.query_row(db, sql, params)
  if not row then
    return nil, err
  end

  -- Return the first column's value
  for _, value in pairs(row) do
    return value
  end

  return nil
end

-- Execute a statement (INSERT, UPDATE, DELETE)
function SqlAccessor.execute(db, sql, params)
  if not db then
    Logger.error("Database connection is required")
    return false, "Database connection is required"
  end

  local stmt = db:prepare(sql)
  if not stmt then
    Logger.error("Failed to prepare statement", db:errmsg())
    return false, "Failed to prepare statement: " .. db:errmsg()
  end

  -- Bind parameters if provided
  if params then
    if type(params) == "table" then
      -- Array style params
      if #params > 0 then
        for i, param in ipairs(params) do
          stmt:bind(i, param)
        end
        -- Dict style params
      elseif next(params) then
        stmt:bind_names(params)
      end
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

-- Transaction helper
function SqlAccessor.transaction(db, func)
  if not db then
    Logger.error("Database connection is required")
    return false, "Database connection is required"
  end

  db:exec("BEGIN TRANSACTION")

  local success, result = pcall(func)

  if success then
    db:exec("COMMIT")
    return true, result
  else
    db:exec("ROLLBACK")
    Logger.error("Transaction failed", result)
    return false, "Transaction failed: " .. tostring(result)
  end
end

return SqlAccessor
