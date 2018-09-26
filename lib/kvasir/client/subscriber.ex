defmodule Kvasir.Subscriber do
  require Logger

  @callback handle_event(Kvasir.Event.t()) :: :ok | {:error, atom}

  defmacro __using__(opts \\ []) do
    topic = opts[:topic] || raise "Need to set a topic."
    client = opts[:client] || raise "Need to set a client."

    auto =
      unless opts[:auto_start] == false do
        auto_start = Module.concat(Kvasir.Util.AutoStart, __CALLER__.module)

        quote do
          defmodule unquote(auto_start) do
            @moduledoc false

            @doc false
            @spec client :: module
            def client, do: unquote(client)

            @doc false
            @spec module :: module
            def module, do: unquote(__CALLER__.module)
          end
        end
      end

    quote do
      alias Kvasir.Subscriber
      @behaviour Subscriber

      @doc false
      @spec start_link(Keyword.t()) :: term
      def start_link(opts \\ []),
        do: Subscriber.start_link(unquote(client), __MODULE__, unquote(topic))

      @doc false
      @spec child_spec(Keyword.t()) :: map
      def child_spec(_opts \\ []), do: Subscriber.child_spec(__MODULE__)

      unquote(auto)
    end
  end

  def child_spec(subscriber) do
    %{
      id: subscriber,
      start: {subscriber, :start_link, []}
    }
  end

  def start_link(client, subscriber, topic) do
    client_id = Module.concat(subscriber, "Client1")
    group_id = inspect(subscriber)
    topics = [topic]

    group_config = [
      offset_commit_policy: :commit_to_kafka_v2,
      offset_commit_interval_seconds: 5,
      rejoin_delay_seconds: 2
    ]

    ### Start Subscriber
    :ok = :brod.start_client(client.config[:kafka], client_id, _client_config = [])

    :brod.start_link_group_subscriber(
      client_id,
      group_id,
      topics,
      group_config,
      _consumer_config = [begin_offset: :earliest],
      _callback_module = __MODULE__,
      _callback_init_args = {client_id, topic, subscriber}
    )
  end

  def init(group_id, {client_id, topic, subscriber}) do
    {:ok,
     %{
       group_id: group_id,
       client_id: client_id,
       topic: topic,
       subscriber: subscriber,
       processors: spawn_processors(client_id, topic, subscriber)
     }}
  end

  defp spawn_processors(client_id, topic, subscriber) do
    {:ok, count} = :brod.get_partitions_count(client_id, topic)

    Enum.into(
      0..(count - 1),
      %{},
      &{&1, spawn(__MODULE__, :process, [topic, &1, subscriber, self()])}
    )
  end

  def handle_message(
        topic,
        partition,
        message,
        state = %{subscriber: subscriber, processors: processors}
      ) do
    pid = processors[partition]

    {processor, state} =
      if pid && Process.alive?(pid) do
        {pid, state}
      else
        pid = spawn(__MODULE__, :process, [topic, partition, subscriber, self()])
        {pid, %{state | processors: Map.put(processors, partition, pid)}}
      end

    send(processor, message)
    {:ok, state}
  end

  def process(topic, partition, subscriber, pid) do
    with {:ok, msg} <- receive_message(),
         {:ok, event} <- Kvasir.Event.Decoder.decode(msg, topic: topic, partition: partition) do
      offset = Kvasir.Offset.partition(event.__meta__.offset, partition)

      case perform_handle(subscriber, event) do
        :ok ->
          :brod_group_subscriber.ack(pid, topic, partition, offset)

        {:error, reason} ->
          if Kvasir.Event.on_error(event) == :skip,
            do: :brod_group_subscriber.ack(pid, topic, partition, offset)

          Logger.error(fn -> "Subscriber #{subscriber}: #{inspect(reason)}" end)
      end
    else
      {:error, :timeout} -> :ok
      {:error, reason} -> Logger.error(fn -> "Subscriber #{subscriber}: #{inspect(reason)}" end)
    end

    process(topic, partition, subscriber, pid)
  end

  defp receive_message do
    receive do
      msg -> {:ok, msg}
    after
      1_000 -> {:error, :timeout}
    end
  end

  defp perform_handle(subscriber, event) do
    case subscriber.handle_event(event) do
      :ok -> :ok
      error -> error
    end
  rescue
    error -> {:error, error}
  end
end
