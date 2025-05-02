local ao = require('ao')
local json = require('json')


local BOLT_PROTOCOL = 'cMoONh83OXPJxJXtVv4WgNw91XRAbSg0yYRNw1RsO18' --'AzsqUB479zLabNlI9LbrSukKE6nGUtIYkpu3zGc2nz8'
local permaswapPools = {
  'VdbIsznFtQtStQXr7ICSsX2yWhHJwougW3dGhIGRam0',
  '8JC9QewRw4wDnE59LomfhakoBrB_LHkN3oDvU3xXNbE'
}

local botegaPools = {
  'Ov64swLY1JfXK5nMFO-mc_Kb_s8vA6w2KDcjpho9BRU',
  'bMyl_dysjbyKJBbaT9u9RwgTd527H3WcSvVyGugljvM'
}

Handlers.add('populate-permaswap', Handlers.utils.hasMatchingTag('Action', 'Permaswap'), function(msg)
  assert(msg.From == ao.id, "Only authorized processes can call this handler")
  ao.send({
    Target = BOLT_PROTOCOL,
    Action = "CollectData",
    Source = "permaswap",
    PoolAddresses = json.encode(permaswapPools),
    SaveToDb = 'true',
    RebuildGraph = 'true'
  })
end)


Handlers.add('populate-botega', Handlers.utils.hasMatchingTag('Action', 'Botega'), function(msg)
  assert(msg.From == ao.id, "Only authorized processes can call this handler")
  ao.send({
    Target = BOLT_PROTOCOL,
    Action = "CollectData",
    Source = "botega",
    PoolAddresses = json.encode(botegaPools),
    SaveToDb = 'true',
    RebuildGraph = 'true'
  })
end)

Handlers.add('update-token-data', Handlers.utils.hasMatchingTag('Action', 'TokenData'), function(msg)
  assert(msg.From == ao.id, "Only authorized processes can call this handler")
  ao.send({
    Target = BOLT_PROTOCOL,
    Action = "UpdateTokenInfo",
  })
end)
