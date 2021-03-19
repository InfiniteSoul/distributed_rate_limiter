defmodule DistributedRateLimiter.Limiter do
  @moduledoc """
  Distributed rate limiting using a sliding window. Sacrifices consistency for availability,
  partition tolerance and speed. Still, the enforced approximate rate limit shouldn't be much higher than
  the actually intended one. As this rate limit is only used for overflow control and not for billing purposes,
  this should be sufficient.

  In case two nodes want to recover after a network partition, TODO

  A complete log of requests isn't used due to memory usage concerns and a (self-imposed) requirement of a dynamic
  rate limit per request. This could be reevaluated if the rate limit was decided to be static.
  """

  use GenServer

  require Logger

  alias DistributedRateLimiter.Window

  # How often this process should make a complete sync with everyone else in the cluster
  # TODO: Implement this
  @complete_sync_interval_sec 60
  # Timezone needed for timestamp of state
  @timezone "Etc/UTC"
  # How big the sliding window should be
  @window_size_sec 30

  # API

  @doc """
  Checks if the access to the given rate limited resource is allowed.
  """
  @spec check_rate(GenServer.server(), any, integer) ::
          {:allow, remaining :: integer} | {:deny, limit :: integer}
  def check_rate(rate_limiter, resource, limit) do
    GenServer.call(rate_limiter, {:handle, resource, limit})
  end

  # GenServer Callbacks

  def start_link(opts) do
    # TODO: name as separate arg
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  def init(opts) do
    with {:ok, name} <- Keyword.fetch(opts, :name) do
      # Monitor connected nodes for changes
      :net_kernel.monitor_nodes(true, node_type: :visible)

      # Following is a summary of the fields specified in the state:
      # name: Name the rate limiters are registered as at the other nodes of the cluster
      # windows ({current window (struct), last window (struct)}): The windows that are used for rate limiting
      # uid: The UUID as a hex string that is used to communicate with the other nodes/processes (they reference this in their windows)

      uid = UUID.uuid5(nil, "#{Node.self()}#{inspect(self())}", :hex)

      {:ok, %{name: name, uid: uid, windows: create_windows()}, {:continue, nil}}
    else
      :error -> {:error, :options_missing}
      {:error, _} = error -> error
    end
  end

  def handle_continue(_continue, %{name: name, uid: uid} = state) do
    Logger.info("Started rate limiting node #{inspect(self())} with name '#{name}'")

    # Try to sync with other nodes of the cluster
    # TODO: Maybe make this async by abcasting and then completing the sync in another handle
    {replies, _bad_nodes} =
      GenServer.multi_call(
        Node.list(),
        name,
        {:sync_request, uid, %{}},
        @window_size_sec * 1000
      )

    # Update own windows (currently empty) with timestamps received from other processes
    {last_window, cur_window} =
      replies
      |> Enum.reduce(get_windows(state), fn {:ok, uid, resources}, {last_window, cur_window} ->
        cur_window = Window.update_foreign_requests(cur_window, uid, resources)
        last_window = Window.update_foreign_requests(last_window, uid, resources)
        {last_window, cur_window}
      end)

    {:noreply, %{state | windows: {cur_window, last_window}}}
  end

  @spec handle_call(
          {:sync_request, String.t(), %{Window.resource_key() => [Window.timestamp()]}},
          any(),
          any()
        ) ::
          {:reply,
           {:ok, uid :: String.t(),
            resources :: %{Window.resource_key() => [Window.timestamp()]}}, any()}
  def handle_call({:sync_request, uid, requests}, _from, %{uid: uid} = state) do
    # Another process wants to sync so we update our windows with the recieved timestamps and send them our
    # observed request timestamps (only requests observed in this process, not any of others)
    {last_window, cur_window} = get_windows(state)

    cur_window = Window.update_foreign_requests(cur_window, uid, requests)
    last_window = Window.update_foreign_requests(last_window, uid, requests)

    own_resources =
      Window.get_own_resource_requests(last_window)
      |> Map.merge(Window.get_own_resource_requests(cur_window), fn _k, v1, v2 -> v1 ++ v2 end)

    {:reply, {:ok, uid, own_resources}, %{state | windows: {cur_window, last_window}}}
  end

  def handle_call(
        {:handle, resource, limit},
        _from,
        %{name: name, uid: uid} = state
      ) do
    {:ok, request_time} = DateTime.now(@timezone)

    # Check if the current window in the windows tuple is still valid
    {cur_window, last_window} = get_windows(state)

    # Weight the totals of the current and last window depending on how much of the current window has already passed
    last_window_total = Window.get_total(last_window, resource)
    cur_window_total = Window.get_total(cur_window, resource)

    # Divisor cannot be 0 because the windows will always be longer than 0 seconds
    # Uses milliseconds as a unit because requests may come in shortly after starting the
    # new window and so this factor was 0 a lot of the time.
    percentage_window_passed =
      DateTime.diff(request_time, cur_window.start_time, :millisecond) /
        DateTime.diff(cur_window.end_time, cur_window.start_time, :millisecond)

    # Only weigth the prev window because all the requests in the current window must be within the rate limit interval
    weighted_total =
      (cur_window_total + (1 - percentage_window_passed) * last_window_total)
      |> Float.round(1)

    weighted_total = trunc(weighted_total)

    # Check if the weighted total will be within the limit after the request
    if weighted_total + 1 <= limit do
      Logger.debug(
        "[Rate-Limiter (UUID: #{uid})] Allowed access to resource '#{inspect(resource)}' @ #{
          request_time
        } with #{limit - weighted_total - 1} remaining"
      )

      cur_window = Window.add_request(cur_window, resource, request_time)

      # Tell other members of the cluster about the request.
      # This runs async on a worker task because we don't want to wait for this to improve throughput. At
      # the moment the worker waits for an answer from every visible node in the cluster and prints a warning
      # to the log if at least one node didn't respond in time. The worker currently waits at most twice as
      # long as the current window size for an answer because after that time both windows passed and the
      # request is meaningless. In theory this could be changed to an abcast in the future and all that would
      # be lost is the ability to print a warning to the log about nodes that could not be reached.
      # TODO: Maybe wait for requests to accumulate before sending a notification to other processes. If implemented, make it an option
      Task.start(fn ->
        {_replies, bad_nodes} =
          GenServer.multi_call(
            Node.list(),
            name,
            {:notify_request, uid, resource, request_time},
            @window_size_sec * 2 * 1000
          )

        if bad_nodes == [] do
          Logger.debug(
            "[Rate-Limiter (UUID: #{uid})] Successfully informed all other nodes in cluster about request @ #{
              request_time
            }"
          )
        else
          Logger.warn(
            "[Rate-Limiter (UUID: #{uid})] Could not notify following nodes about the request @ #{
              request_time
            }: #{inspect(bad_nodes)}"
          )
        end
      end)

      {:reply, {:allow, limit - weighted_total - 1},
       %{state | windows: {cur_window, last_window}}}
    else
      Logger.debug(
        "[Rate-Limiter (UUID: #{uid})] Rejected access to resource '#{inspect(resource)}' @ #{
          request_time
        }"
      )

      {:reply, {:deny, limit}, %{state | windows: {cur_window, last_window}}}
    end
  end

  def handle_call({:notify_request, uid, resource, request_time}, _from, state) do
    {last_window, cur_window} = get_windows(state)

    # Try to add the request to both windows
    # If the request timestamp is not within the start and end times each window, the request will
    # be ignored by the Window.add_foreign_requests function
    cur_window = Window.add_foreign_requests(cur_window, uid, %{resource => [request_time]})
    last_window = Window.add_foreign_requests(last_window, uid, %{resource => [request_time]})

    {:reply, :ok, %{state | windows: {last_window, cur_window}}}
  end

  def handle_call(_msg, _from, state) do
    {:reply, :ok, state}
  end

  def handle_cast(_msg, state) do
    {:noreply, state}
  end

  # Callbacks if connected nodes change

  def handle_info({:nodeup, _node, _node_type}, state) do
    {:noreply, state}
  end

  def handle_info({:nodedown, _node, _node_type}, state) do
    {:noreply, state}
  end

  defp create_windows() do
    # Create windows tuple with DateTime.now as the splitting timestamp
    {:ok, now} = DateTime.now(@timezone)
    last_window = Window.new(DateTime.add(now, -1 * @window_size_sec), now)
    cur_window = Window.new(now, DateTime.add(now, @window_size_sec))
    {cur_window, last_window}
  end

  # Returns the windows tuple from the state or a new one if the old windows are outdated
  defp get_windows(%{windows: {last_window, cur_window}} = _state) do
    {:ok, now} = DateTime.now(@timezone)

    cond do
      DateTime.compare(now, cur_window.end_time) == :lt ->
        # Current window still valid
        {cur_window, last_window}

      DateTime.diff(now, cur_window.end_time) >= @window_size_sec ->
        # Current window is no longer valid AND enough time passed that the old current window is past the last window slot
        create_windows()

      true ->
        # Current window is no longer valid BUT the old current window still fits in the last window slot
        new_end_time = DateTime.add(cur_window.end_time, @window_size_sec)
        {Window.new(cur_window.end_time, new_end_time), cur_window}
    end
  end
end
