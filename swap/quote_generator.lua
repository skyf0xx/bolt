local BigDecimal = require('arbitrage.utils.big_decimal')
local Constants = require('arbitrage.constants')
local Logger = require('arbitrage.logger').createLogger("QuoteGenerator")
local Utils = require('arbitrage.utils')
local Calculator = require('arbitrage.swap.calculator')
local TokenRepository = require('arbitrage.db.token_repository')
local PoolRepository = require('arbitrage.db.pool_repository')

local QuoteGenerator = {}

-- Initialize the quote generator with dependencies
function QuoteGenerator.init(db, calculator)
  QuoteGenerator.db = db
  QuoteGenerator.calculator = calculator or Calculator.init(db)
  Logger.info("Quote generator initialized")
  return QuoteGenerator
end

-- Generate a quote for a specific path and input amount
function QuoteGenerator.generateQuote(path, inputAmount, callback)
  if not path or #path == 0 then
    callback(nil, "Invalid path")
    return
  end

  Logger.debug("Generating quote", { pathLength = #path, inputAmount = inputAmount })

  -- Get token information for display purposes
  local sourceTokenId = path[1].from
  local targetTokenId = path[#path].to

  -- Get tokens from database
  QuoteGenerator.getTokenPairInfo(sourceTokenId, targetTokenId, function(tokenInfo, tokenErr)
    if not tokenInfo then
      Logger.warn("Token information not available", { error = tokenErr })
      -- Continue with limited token info
    end

    local sourceDecimals = tokenInfo and tokenInfo.source and tokenInfo.source.decimals or Constants.NUMERIC.DECIMALS
    local targetDecimals = tokenInfo and tokenInfo.target and tokenInfo.target.decimals or Constants.NUMERIC.DECIMALS

    -- Calculate the output for the path
    QuoteGenerator.calculator.calculatePathOutput(path, inputAmount, function(result, err)
      if not result then
        callback(nil, err or "Failed to calculate path output")
        return
      end

      -- Format amounts for display
      local formattedInputAmount = Utils.formatTokenAmount(inputAmount, sourceDecimals)
      local formattedOutputAmount = Utils.formatTokenAmount(result.output_amount, targetDecimals)

      -- Calculate execution price
      local executionPrice = BigDecimal.divide(
        BigDecimal.fromTokenAmount(result.output_amount, targetDecimals),
        BigDecimal.fromTokenAmount(inputAmount, sourceDecimals)
      )

      -- Extract sources used in the path
      local sources = {}
      for _, step in ipairs(path) do
        if not Utils.tableContains(sources, step.source) then
          table.insert(sources, step.source)
        end
      end

      -- Calculate average price impact
      local totalPriceImpactBps = 0
      for _, step in ipairs(result.steps) do
        totalPriceImpactBps = totalPriceImpactBps + tonumber(step.price_impact_bps or 0)
      end
      local avgPriceImpactBps = #result.steps > 0 and (totalPriceImpactBps / #result.steps) or 0

      -- Generate the quote
      local quote = {
        path = path,
        steps = result.steps,
        source_token = tokenInfo and tokenInfo.source or { id = sourceTokenId },
        target_token = tokenInfo and tokenInfo.target or { id = targetTokenId },
        input_amount = inputAmount,
        output_amount = result.output_amount,
        formatted_input = formattedInputAmount,
        formatted_output = formattedOutputAmount,
        execution_price = executionPrice.toDecimal(8),
        price_impact_bps = avgPriceImpactBps,
        price_impact_percent = Utils.bpsToDecimal(avgPriceImpactBps),
        fee = {
          bps = result.total_fee_bps,
          percent = Utils.bpsToDecimal(result.total_fee_bps)
        },
        route = {
          hops = #path,
          sources = sources
        },
        timestamp = os.time(),
        expiry = os.time() + Constants.TIME.RESERVE_CACHE_EXPIRY
      }

      -- Add slippage-adjusted output amounts
      quote.minimum_received = QuoteGenerator.applySlippage(
        result.output_amount,
        Constants.NUMERIC.DEFAULT_SLIPPAGE_TOLERANCE
      )

      -- Generate human-readable route description
      quote.route_description = QuoteGenerator.generateRouteDescription(
        path,
        tokenInfo,
        result.steps
      )

      callback(quote)
    end)
  end)
end

-- Apply slippage tolerance to an output amount
function QuoteGenerator.applySlippage(amount, slippageBps)
  local bdAmount = BigDecimal.new(amount)
  local bdSlippageFactor = BigDecimal.divide(
    BigDecimal.new(Constants.NUMERIC.BASIS_POINTS_MULTIPLIER - slippageBps),
    BigDecimal.new(Constants.NUMERIC.BASIS_POINTS_MULTIPLIER)
  )
  return BigDecimal.multiply(bdAmount, bdSlippageFactor).value
end

-- Get token pair information by IDs
function QuoteGenerator.getTokenPairInfo(sourceTokenId, targetTokenId, callback)
  if not QuoteGenerator.db then
    callback(nil, "Database not initialized")
    return
  end

  -- Get both tokens in parallel
  local sourceToken, targetToken
  local pendingTokens = 2

  local function checkCompletion()
    if pendingTokens == 0 then
      callback({
        source = sourceToken,
        target = targetToken
      })
    end
  end

  -- Get source token
  TokenRepository.getToken(QuoteGenerator.db, sourceTokenId, function(token, err)
    pendingTokens = pendingTokens - 1
    if token then
      sourceToken = token
    else
      Logger.warn("Source token not found", { id = sourceTokenId, error = err })
      sourceToken = { id = sourceTokenId }
    end
    checkCompletion()
  end)

  -- Get target token
  TokenRepository.getToken(QuoteGenerator.db, targetTokenId, function(token, err)
    pendingTokens = pendingTokens - 1
    if token then
      targetToken = token
    else
      Logger.warn("Target token not found", { id = targetTokenId, error = err })
      targetToken = { id = targetTokenId }
    end
    checkCompletion()
  end)
end

-- Generate a human-readable route description
function QuoteGenerator.generateRouteDescription(path, tokenInfo, steps)
  local description = {}
  local tokenSymbols = {}

  -- Store token symbols for quick lookup
  if tokenInfo then
    if tokenInfo.source then
      tokenSymbols[tokenInfo.source.id] = tokenInfo.source.symbol
    end
    if tokenInfo.target then
      tokenSymbols[tokenInfo.target.id] = tokenInfo.target.symbol
    end
  end

  for i, step in ipairs(path) do
    local stepOutput = steps and steps[i] and steps[i].amount_out or "?"
    local sourceSymbol = tokenSymbols[step.from] or step.from:sub(1, 8)
    local targetSymbol = tokenSymbols[step.to] or step.to:sub(1, 8)
    local dexName = step.source:sub(1, 1):upper() .. step.source:sub(2) -- Capitalize first letter

    table.insert(description, string.format(
      "%d. %s → %s via %s (fee: %0.2f%%, output: %s)",
      i,
      sourceSymbol,
      targetSymbol,
      dexName,
      step.fee_bps / 100,
      Utils.shortId(stepOutput, 10)
    ))
  end

  return table.concat(description, "\n")
end

-- Generate comparative quotes for multiple paths
function QuoteGenerator.generateComparativeQuotes(paths, inputAmount, callback)
  if not paths or #paths == 0 then
    callback({ quotes = {} }, "No paths provided")
    return
  end

  local quotes = {}
  local pendingPaths = #paths

  for i, path in ipairs(paths) do
    QuoteGenerator.generateQuote(path, inputAmount, function(quote, err)
      pendingPaths = pendingPaths - 1

      if quote then
        table.insert(quotes, quote)
      else
        Logger.warn("Failed to generate quote for path", { pathIndex = i, error = err })
      end

      if pendingPaths == 0 then
        -- Sort quotes by output amount (descending)
        table.sort(quotes, function(a, b)
          return BigDecimal.new(a.output_amount).value > BigDecimal.new(b.output_amount).value
        end)

        callback({
          quotes = quotes,
          best_quote = #quotes > 0 and quotes[1] or nil,
          input_amount = inputAmount,
          quote_count = #quotes
        })
      end
    end)
  end
end

-- Find and generate the best quote for a token pair
function QuoteGenerator.findBestQuote(sourceTokenId, targetTokenId, inputAmount, pathFinder, options, callback)
  options = options or {}

  if not pathFinder then
    callback(nil, "PathFinder is required")
    return
  end

  Logger.info("Finding best quote", {
    source = sourceTokenId,
    target = targetTokenId,
    amount = inputAmount
  })

  -- Find optimal paths
  pathFinder.findOptimalSwap(sourceTokenId, targetTokenId, inputAmount, options, function(result)
    if not result or not result.paths or #result.paths == 0 then
      callback(nil, result and result.error or "No paths found")
      return
    end

    -- Extract just the paths from the results
    local paths = {}
    for _, pathData in ipairs(result.paths) do
      table.insert(paths, pathData.path)
    end

    -- Generate comparative quotes
    QuoteGenerator.generateComparativeQuotes(paths, inputAmount, function(quoteResults)
      callback(quoteResults)
    end)
  end)
end

-- Generate a quote for a direct swap (single pool)
function QuoteGenerator.generateDirectQuote(poolId, tokenIn, inputAmount, callback)
  -- Get pool information
  local pool = PoolRepository.getPool(QuoteGenerator.db, poolId)
  if not pool then
    callback(nil, Constants.ERROR.POOL_NOT_FOUND)
    return
  end

  -- Determine token out
  local tokenOut
  if tokenIn == pool.token_a_id then
    tokenOut = pool.token_b_id
  elseif tokenIn == pool.token_b_id then
    tokenOut = pool.token_a_id
  else
    callback(nil, Constants.ERROR.INVALID_TOKEN)
    return
  end

  -- Create a simple path with one hop
  local path = {
    {
      from = tokenIn,
      to = tokenOut,
      pool_id = poolId,
      source = pool.source,
      fee_bps = pool.fee_bps
    }
  }

  -- Generate the quote
  QuoteGenerator.generateQuote(path, inputAmount, callback)
end

-- Create a quoted swap order with all required information
function QuoteGenerator.createSwapOrder(quote, userAddress, callback)
  if not quote then
    callback(nil, "Quote is required")
    return
  end

  -- Add additional information for the swap order
  local order = Utils.deepCopy(quote)
  order.user_address = userAddress
  order.order_id = "order_" .. os.time() .. "_" .. math.random(1000, 9999)
  order.status = "created"
  order.created_at = os.time()

  callback(order)
end

return QuoteGenerator
