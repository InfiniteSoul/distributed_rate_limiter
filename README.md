# Distributed Rate Limiter (WIP)

**Disclaimer: This is a PoC and should not be used in any type of real application. This is mainly because every request 
is handled by the same GenServer that is sure to become a bottleneck in any kind of real situation.**

Proof-of-concept distributed rate limiting using a sliding window. Sacrifices consistency for availability,
partition tolerance (CAP theorem) and speed. Still, the enforced approximate rate limit shouldn't be much higher than
the actually intended one. Therefore this rate limiter should only be used for overflow control and not for billing 
purposes.

Also this is still WIP so some essential features are still not implemented and the program is not tested.

## Installation

Currently not available on [Hex](https://hex.pm/). However if you want to install this rate limiter anyway use git

```elixir
def deps do
  [
    {:distributed_rate_limiter, git: "https://github.com/InfiniteSoul/distributed_rate_limiter.git", branch: "master"}
  ]
end
```
