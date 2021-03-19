# Distributed Rate Limiter (WIP)

Experimental distributed rate limiting using a sliding window. Sacrifices consistency for availability,
partition tolerance (CAP theorem) and speed. Still, the enforced approximate rate limit shouldn't be much higher than
the actually intended one. Therefore this rate limiter should only be used for overflow control and not for billing 
purposes.

Also this is still WIP so some essential features are still not implemented and the program is not tested.

## Installation

Currently not availabel on [Hex](https://hex.pm/). However if you want to install this rate limiter anyway use git

```elixir
def deps do
  [
    {:distributed_rate_limiter, git: "https://github.com/InfiniteSoul/distributed_rate_limiter.git", branch: "master"}
  ]
end
```
