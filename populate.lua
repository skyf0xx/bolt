local ao = require('ao')
local json = require('json')


local BOLT_PROTOCOL = 'cMoONh83OXPJxJXtVv4WgNw91XRAbSg0yYRNw1RsO18' --'AzsqUB479zLabNlI9LbrSukKE6nGUtIYkpu3zGc2nz8'
Handlers.add('populate-botega', Handlers.utils.hasMatchingTag('Action', 'PopulateBotega'), function(msg)
  assert(msg.From == ao.id, "Only authorized processes can call this handler")
  ao.send({
    Target = BOLT_PROTOCOL,
    Action = "CollectData",
    Source = "botega",
    SaveToDb = 'true',
    RebuildGraph = 'false'
  }).onReply(function(response)
    print('Aggregator has been Populated with fresh reserves')
  end)
end)

Handlers.add('populate', Handlers.utils.hasMatchingTag('Action', 'Populate'), function(msg)
  assert(msg.From == ao.id, "Only authorized processes can call this handler")
  ao.send({
    Target = BOLT_PROTOCOL,
    Action = "CollectData",
    Source = "permaswap",
    SaveToDb = 'true',
    RebuildGraph = 'false'
  }).onReply(function(response)
    ao.send({
      Target = BOLT_PROTOCOL,
      Action = "CollectData",
      Source = "botega",
      SaveToDb = 'true',
      RebuildGraph = 'false'
    }).onReply(function(response)
      ao.send({
        Target = BOLT_PROTOCOL,
        Action = "UpdateTokenInfo",
      }).onReply(function(response)
        ao.send({
          Target = BOLT_PROTOCOL,
          Action = "BuildGraph",
        }).onReply(function(response)
          ao.send({
            Target = BOLT_PROTOCOL,
            Action = "RefreshReserves",
            ForceFresh = 'true'
          }).onReply(function(response)
            print('Aggregator has been Populated with fresh reserves')
          end)
        end)
      end)
    end)
  end)
end)


Handlers.add('flush-collectors', Handlers.utils.hasMatchingTag('Action', 'FlushCollectors'), function(msg)
  assert(msg.From == ao.id, "Only authorized processes can call this handler")
  ao.send({
    Target = BOLT_PROTOCOL,
    Action = "FlushCollectors",
    Forced = "true",
  }).onReply(function(response)
    print('Flush successful')
    print(response)
  end)
end)
