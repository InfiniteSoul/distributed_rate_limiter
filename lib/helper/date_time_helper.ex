defmodule DistributedRateLimiter.DateTimeHelper do
  @moduledoc false

  @doc """
  Checks if the given value is between start_time (inclusive) and end_time (exclusive)
  """
  @spec is_between(Calender.datetime(), Calender.datetime(), Calender.datetime()) :: boolean
  def is_between(value, start_time, end_time) do
    DateTime.compare(value, start_time) != :lt and DateTime.compare(value, end_time) == :lt
  end
end
