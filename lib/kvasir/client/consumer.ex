defmodule Kvasir.Client.Consumer do
  @moduledoc false
  require Logger

  def consume(config, topic, callback, opts) do
    start_consume(config, topic, callback, opts)
  end

  def stream(config, topic, opts) do
    Stream.resource(
      fn ->
        streamer = self()
        start_consume(config, topic, &send(streamer, {:stream, &1}), opts)
      end,
      fn state ->
        receive do
          {:stream, :end} -> {:halt, state}
          {:stream, events} -> {events, state}
        end
      end,
      fn {:ok, pid, client} ->
        :brod_topic_subscriber.stop(pid)
        :brod.stop_client(client)
      end
    )
  end

  defp start_consume(config, topic, callback, opts) do
    from = opts[:from] || :earliest
    events = opts[:events] || []

    consumerConfig = [
      begin_offset: Kvasir.Offset.partition(from, 0),
      offset_reset_policy: :reset_to_earliest
    ]

    client = String.to_atom("Kvasir.Stream" <> to_string(:rand.uniform(10_000)))
    :brod.start_client(config, client)

    to =
      case opts[:to] do
        nil -> nil
        offset when is_integer(offset) -> offset
        time -> elem(Kvasir.Client.Info.offset(client, topic, time), 1)
      end

    {:ok, pid} =
      :brod_topic_subscriber.start_link(
        client,
        topic,
        :all,
        consumerConfig,
        [],
        :message_set,
        &handle_message/3,
        %{topic: topic, callback: callback, to: to, events: events, client: client}
      )

    if is_integer(to) and (to < 0 or (is_integer(from) and to <= from)) do
      callback.(:end)
    end

    {:ok, pid, client}
  end

  def handle_message(partition, message, state = %{callback: callback, to: to, topic: topic}) do
    case Kvasir.Event.decode(message, partition: partition, topic: topic, encoding: :brod) do
      {:ok, event} ->
        callback.(event)

        if to && List.last(event).__meta__.offset >= to do
          callback.(:end)
        end

        {:ok, :ack, state}

      {:error, reason} ->
        Logger.error(fn -> "Kvasir: parse error (#{reason}), payload: #{inspect(message)}" end)
        {:ok, :nack, state}
    end
  end
end
