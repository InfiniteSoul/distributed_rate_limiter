defmodule DistributedRateLimiter.Window do
  @moduledoc """
  A struct that is used to keep track of requests made for rate limiting purposes.
  Stores for each rate limited resource (e.g. user):
  * the total number of requests made in the distributed system (as seen by this window, can be less than the actual number in the system, but never more)
  * list of timestamps of all requests handled by this window (TODO: Maybe Elias-Fano encoded to save space)
  * for each process except this one: Number of requests allowed
  """

  alias DistributedRateLimiter.DateTimeHelper

  @type timestamp() :: integer()
  @type resource_key() :: any()
  @type resource() ::
          {total :: integer(), timestamps :: [timestamp()],
           other_proc :: %{String.t() => integer()}}
  @type resources() :: %{resource_key() => resource()}
  @type t() :: %__MODULE__{
          start_time: Calender.datetime(),
          end_time: Calender.datetime(),
          resources: resources()
        }

  # start_time: A DateTime struct that represents the timestamp when the window was started (inclusive)
  # end_time: A DateTime struct that represents the timestamp when the window was closed (exclusive)
  # Each resource: {total :: integer, timestamps :: list of integers, other_proc = %{uid: number (integer)}}
  defstruct [:start_time, :end_time, resources: %{}]

  @spec new(Calender.datetime(), Calender.datetime()) :: t()
  def new(start_time, end_time) do
    if DateTime.compare(start_time, end_time) == :gt do
      {:error, :invalid_time_interval}
    else
      %__MODULE__{start_time: start_time, end_time: end_time}
    end
  end

  @doc """
  Adds a request timestamp to a given resource and updates its total counter
  """
  @spec add_request(t(), resource_key(), Calender.datetime()) :: t()
  def add_request(window, request_resource, timestamp) do
    if DateTimeHelper.is_between(timestamp, window.start_time, window.end_time) do
      {total, timestamps, other_proc} = Map.get(window.resources, request_resource, {0, [], %{}})

      %__MODULE__{
        window
        | resources:
            Map.put(
              window.resources,
              request_resource,
              {total + 1, [timestamp | timestamps], other_proc}
            )
      }
    else
      window
    end
  end

  @doc """
  Adds one or more requests from another process to the given window. Always assumes that the requests are not already contained in this window.

  Params:
  * proc: UUID v5 of the foreign process as a hexadecimal string
  * resources: A map of resource names -> request time stamps of a foreign process
  """
  @spec add_foreign_requests(t(), String.t(), %{resource_key() => [timestamp()]}) :: t()
  def add_foreign_requests(window, proc, resources) do
    do_update_foreign_requests(window, proc, resources, fn _k, v1, v2 -> v1 + v2 end)
  end

  @doc """
  Adds one or more requests from another process to the given window. Overwrites any existing requests from the other process.

  Params:
  * proc: UUID v5 of the foreign process as a hexadecimal string
  * resources: A map of resource names -> request time stamps of a foreign process
  """
  @spec update_foreign_requests(t(), String.t(), %{resource_key() => [timestamp()]}) :: t()
  def update_foreign_requests(window, proc, resources) do
    do_update_foreign_requests(window, proc, resources, fn _k, _v1, v2 -> v2 end)
  end

  @spec do_update_foreign_requests(
          t(),
          String.t(),
          %{resource_key() => [timestamp()]},
          (String.t(), integer(), integer() -> integer())
        ) :: t()
  defp do_update_foreign_requests(window, proc, resources, count_merge_fun) do
    # Filter and format foreign resources (these need to be merged into the current window in the next step)
    foreign_requests =
      resources
      |> Enum.map(fn {resource, req_times} ->
        {resource,
         Enum.count(req_times, fn req_time ->
           DateTimeHelper.is_between(req_time, window.start_time, window.end_time)
         end)}
      end)
      |> Enum.map(fn {resource, req_times} ->
        {resource, {req_times, [], %{proc => req_times}}}
      end)
      |> Enum.into(%{})

    # Merge foreign requests with requests already found in this window. Uses the count_merge_fun function to
    # resolve any conflicts. This function is used in the public API to either add to or replace the value
    # found in conflicting entries
    %__MODULE__{
      window
      | resources:
          Map.merge(window.resources, foreign_requests, fn _k,
                                                           {total, timestamps, other_proc},
                                                           {_, _, proc_req_count} ->
            new_other_proc =
              Map.merge(other_proc, proc_req_count, fn k, v1, v2 ->
                count_merge_fun.(k, v1, v2)
              end)

            {total - Map.get(other_proc, proc, 0) + Map.get(new_other_proc, proc, 0), timestamps,
             new_other_proc}
          end)
    }
  end

  @doc """
  Gets the total requests as seen by this window for a given resource.
  """
  @spec get_total(t(), any) :: integer
  def get_total(%__MODULE__{resources: resources}, resource) do
    {total, _, _} = Map.get(resources, resource, {0, [], %{}})
    total
  end

  @spec get_own_resource_requests(t()) :: %{resource_key() => [timestamp()]}
  def get_own_resource_requests(%__MODULE__{resources: resources}) do
    resources
    |> Enum.map(fn {resource, {_, timestamps, _}} -> {resource, timestamps} end)
    |> Enum.into(%{})
  end
end
