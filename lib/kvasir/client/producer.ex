defmodule Kvasir.Client.Producer do
  @moduledoc false
  alias Kvasir.{Event, Offset}

  def producer(client, topic), do: :brod.start_producer(client, topic, [])

  def produce(client, events) when is_list(events) do
    Enum.reduce_while(events, {:ok, Offset.create()}, fn event, {:ok, acc} ->
      case produce(client, event) do
        {:ok, offset} -> {:cont, {:ok, Offset.set(acc, 0, offset)}}
        error -> {:halt, error}
      end
    end)
  end

  def produce(client, event) do
    with {:ok, {topic, partition, key, encoded}} <- Event.encode(event, encoding: :brod) do
      :brod.produce_sync_offset(client, topic, partition, key, encoded)
    end
  end

  def produce(client, topic, partition, key, events) when is_list(events) do
    Enum.reduce_while(events, {:ok, Offset.create()}, fn event, {:ok, acc} ->
      case produce(client, topic, partition, key, event) do
        {:ok, offset} -> {:cont, {:ok, Offset.set(acc, partition, offset)}}
        error -> {:halt, error}
      end
    end)
  end

  def produce(client, topic, partition, key, event) do
    with {:ok, encoded} <- Jason.encode(event) do
      :brod.produce_sync_offset(client, topic, partition, key, encoded)
    end
  end
end
