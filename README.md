statis
======

Flexible time-series stat tracking for python and redis.

Heavily inspired by Redistat:
https://github.com/jimeh/redistat


## Usage
```
import statis
import json

stats = statis.Statis('statis-redis', port=6379)

stats.store('personas/c1733467-c272-45b0-84dd-9e345d16a3c8', ['age/33','eyes/blue'] )
stats.store('personas/f528fb69-85ea-4882-86f9-cd6f07c8b400', ['age/22'] )
stats.store('personas/fea80818-ddfa-42ae-adeb-ce2b1dafcc24', ['age/22','eyes/brown'] )
stats.store('personas/bb7ac6ef-a24a-44dd-a013-06cb11fbc606', ['age/43','eyes/blue'])

data = stats.fetch(
    path='personas',
    depth=statis.DAY
)
print(json.dumps(data, indent=4))

```

Returns:
```
[
    {
        "age": 4,
        "age/33": 1,
        "eyes": 3,
        "eyes/blue": 2,
        "age/22": 2,
        "eyes/brown": 1,
        "age/43": 1,
        "datekey": "20170806"
    }
]
```
