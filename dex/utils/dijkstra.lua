local Utils = require('dex.utils.utils')
local Constants = require('dex.utils.constants')
local Logger = require('dex.utils.logger')

-- PriorityQueue implementation for Dijkstra's algorithm
local PriorityQueue = {}
PriorityQueue.__index = PriorityQueue -- Set metatable for proper method inheritance

function PriorityQueue.new()
  local queue = {
    items = {},
    count = 0
  }
  return setmetatable(queue, PriorityQueue) -- Set metatable to properly inherit methods
end

function PriorityQueue:push(item, priority)
  table.insert(self.items, { item = item, priority = priority })
  self.count = self.count + 1
  self:heapifyUp(self.count)
end

function PriorityQueue:pop()
  if self.count == 0 then
    return nil
  end

  local top = self.items[1].item
  self.items[1] = self.items[self.count]
  self.items[self.count] = nil
  self.count = self.count - 1

  if self.count > 0 then
    self:heapifyDown(1)
  end

  return top
end

function PriorityQueue:isEmpty()
  return self.count == 0
end

function PriorityQueue:heapifyUp(index)
  local parent = math.floor(index / 2)

  if parent > 0 and self.items[index].priority < self.items[parent].priority then
    self.items[index], self.items[parent] = self.items[parent], self.items[index]
    self:heapifyUp(parent)
  end
end

function PriorityQueue:heapifyDown(index)
  local smallest = index
  local left = index * 2
  local right = index * 2 + 1

  if left <= self.count and self.items[left].priority < self.items[smallest].priority then
    smallest = left
  end

  if right <= self.count and self.items[right].priority < self.items[smallest].priority then
    smallest = right
  end

  if smallest ~= index then
    self.items[index], self.items[smallest] = self.items[smallest], self.items[index]
    self:heapifyDown(smallest)
  end
end

-- Dijkstra's algorithm implementation that can be used by both Graph and PathFinder
local Dijkstra = {}

-- Run Dijkstra's algorithm to find shortest paths
-- @param graph: the graph object with nodes and edges
-- @param start: starting token ID
-- @param target: target token ID (optional, if nil will find paths to all tokens)
-- @param options: table with options (maxHops, weightFunction, etc.)
-- @return distances, previous (for reconstructing paths)
function Dijkstra.findShortestPaths(graph, start, target, options)
  options = options or {}
  local maxHops = options.maxHops or Constants.PATH.MAX_PATH_LENGTH

  -- Weight function - how to calculate the "cost" of an edge
  -- Default is to use fee_bps as the weight
  local weightFunc = options.weightFunction or function(edge)
    return edge.fee_bps or 0
  end

  local queue = PriorityQueue.new()
  local distances = {}   -- Token -> distance (cost)
  local previous = {}    -- Token -> {previous token, edge}
  local visited = {}     -- Token -> true/nil
  local pathLengths = {} -- Token -> number of hops

  -- Initialize with starting point
  distances[start] = 0
  pathLengths[start] = 0
  queue:push(start, 0)

  while not queue:isEmpty() do
    local current = queue:pop()

    -- If we've reached our target, we can stop
    if target and current == target then
      break
    end

    -- Skip if already visited
    if visited[current] then
      goto continue
    end

    visited[current] = true

    -- Skip if we've reached max hops
    if pathLengths[current] >= maxHops then
      goto continue
    end

    -- Process each edge from current token
    if graph.edges[current] then
      for _, edge in ipairs(graph.edges[current]) do
        -- Skip inactive pools
        if edge.status ~= "active" then
          goto nextEdge
        end

        local nextToken = edge.connected_to
        local weight = weightFunc(edge)
        local newDistance = distances[current] + weight

        -- Update distance if better path found
        if not distances[nextToken] or newDistance < distances[nextToken] then
          distances[nextToken] = newDistance
          previous[nextToken] = { token = current, edge = edge }
          pathLengths[nextToken] = pathLengths[current] + 1
          queue:push(nextToken, newDistance)
        end

        ::nextEdge::
      end
    end

    ::continue::
  end

  return distances, previous, pathLengths
end

-- Reconstruct a path from Dijkstra results
-- @param start: starting token ID
-- @param target: target token ID
-- @param previous: the previous map from findShortestPaths
-- @return reconstructed path
function Dijkstra.reconstructPath(start, target, previous)
  if not previous[target] then
    return nil -- No path exists
  end

  local path = {}
  local current = target

  while current ~= start do
    local prev = previous[current]
    if not prev then
      return nil -- Broken path
    end

    -- Add step to the beginning of the path
    table.insert(path, 1, {
      from = prev.token,
      to = current,
      pool_id = prev.edge.pool_id,
      source = prev.edge.source,
      fee_bps = prev.edge.fee_bps
    })

    current = prev.token
  end

  return path
end

-- Find multiple paths between two tokens using Dijkstra
-- @param graph: the graph object
-- @param sourceTokenId: starting token ID
-- @param targetTokenId: target token ID
-- @param maxHops: maximum number of hops
-- @param maxPaths: maximum number of paths to return
-- @return array of paths
function Dijkstra.findAllPaths(graph, sourceTokenId, targetTokenId, maxHops, maxPaths)
  maxHops = maxHops or Constants.PATH.MAX_PATH_LENGTH
  maxPaths = maxPaths or Constants.PATH.MAX_PATHS_TO_RETURN

  -- First, find the shortest path
  local distances, previous, pathLengths = Dijkstra.findShortestPaths(
    graph, sourceTokenId, targetTokenId, { maxHops = maxHops }
  )

  local paths = {}

  -- If we found a path, add it
  local shortestPath = Dijkstra.reconstructPath(sourceTokenId, targetTokenId, previous)
  if shortestPath then
    table.insert(paths, shortestPath)
  else
    -- No path found
    return {}
  end

  -- If we need more paths, we'll use a modified approach to find alternative paths
  if maxPaths > 1 and shortestPath then
    -- Find alternative paths by temporarily removing edges from the shortest path
    local originalPath = Utils.deepCopy(shortestPath)

    for i = 1, #originalPath do
      -- Create a modified graph by "removing" this edge
      local modifiedGraph = Utils.deepCopy(graph)
      local step = originalPath[i]

      -- Temporarily mark the pool as inactive to find alternative paths
      for j, edge in ipairs(modifiedGraph.edges[step.from]) do
        if edge.pool_id == step.pool_id and edge.connected_to == step.to then
          modifiedGraph.edges[step.from][j].status = "temp_inactive"
          break
        end
      end

      for j, edge in ipairs(modifiedGraph.edges[step.to]) do
        if edge.pool_id == step.pool_id and edge.connected_to == step.from then
          modifiedGraph.edges[step.to][j].status = "temp_inactive"
          break
        end
      end

      -- Find alternative path in the modified graph
      local altDistances, altPrevious = Dijkstra.findShortestPaths(
        modifiedGraph, sourceTokenId, targetTokenId, { maxHops = maxHops }
      )

      local altPath = Dijkstra.reconstructPath(sourceTokenId, targetTokenId, altPrevious)
      if altPath then
        -- Check if this is a unique path
        local isUnique = true
        for _, existingPath in ipairs(paths) do
          if Dijkstra.areSimilarPaths(existingPath, altPath) then
            isUnique = false
            break
          end
        end

        if isUnique then
          table.insert(paths, altPath)
          if #paths >= maxPaths then
            break
          end
        end
      end
    end
  end

  -- Sort paths by length (number of hops)
  table.sort(paths, function(a, b)
    return #a < #b
  end)

  return paths
end

-- Check if two paths are similar (to avoid duplicates)
function Dijkstra.areSimilarPaths(path1, path2)
  if #path1 ~= #path2 then
    return false
  end

  -- Simple check - do they use the same pools in the same order?
  local pools1 = {}
  local pools2 = {}

  for i, step in ipairs(path1) do
    pools1[i] = step.pool_id
  end

  for i, step in ipairs(path2) do
    pools2[i] = step.pool_id
  end

  for i, pool in ipairs(pools1) do
    if pool ~= pools2[i] then
      return false
    end
  end

  return true
end

-- Find cycle paths using Dijkstra's algorithm
-- @param graph: the graph object
-- @param startTokenId: the token ID to start and end at
-- @param maxHops: maximum number of hops
-- @return array of cycle paths
function Dijkstra.findCycles(graph, startTokenId, maxHops)
  maxHops = maxHops or Constants.PATH.MAX_PATH_LENGTH
  local cycles = {}

  -- For each token connected to start token, try to find a path back
  if graph.edges[startTokenId] then
    for _, edge in ipairs(graph.edges[startTokenId]) do
      if edge.status ~= "active" then
        goto continue
      end

      local nextToken = edge.connected_to

      -- Skip self-loops
      if nextToken == startTokenId then
        goto continue
      end

      -- Create a modified graph that excludes the direct edge back to start
      -- to force finding longer cycles
      local modifiedGraph = Utils.deepCopy(graph)

      -- Find paths from this neighbor back to start
      local distances, previous = Dijkstra.findShortestPaths(
        modifiedGraph, nextToken, startTokenId, { maxHops = maxHops - 1 }
      )

      local cyclePath = Dijkstra.reconstructPath(nextToken, startTokenId, previous)
      if cyclePath and #cyclePath > 0 then
        -- Add the initial step to complete the cycle
        table.insert(cyclePath, 1, {
          from = startTokenId,
          to = nextToken,
          pool_id = edge.pool_id,
          source = edge.source,
          fee_bps = edge.fee_bps
        })

        -- Only include cycles with more than 2 steps (to avoid A->B->A trivial cycles)
        if #cyclePath > 2 then
          table.insert(cycles, cyclePath)
        end
      end

      ::continue::
    end
  end

  -- Limit number of cycles
  if #cycles > Constants.NUMERIC.MAX_CYCLES_TO_ANALYZE then
    Logger.warn("Too many cycles found, limiting results", {
      found = #cycles,
      limit = Constants.NUMERIC.MAX_CYCLES_TO_ANALYZE
    })

    local limitedCycles = {}
    for i = 1, Constants.NUMERIC.MAX_CYCLES_TO_ANALYZE do
      table.insert(limitedCycles, cycles[i])
    end

    cycles = limitedCycles
  end

  return cycles
end

return Dijkstra
