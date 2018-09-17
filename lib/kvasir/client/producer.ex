defmodule Kvasir.Client.Producer do
  @moduledoc false
  alias Kvasir.Event

  def producer(client, topic), do: :brod.start_producer(client, topic, [])

  def produce(client, events) when is_list(events) do
    Enum.reduce_while(events, {:ok, -1}, fn event, {:ok, _} ->
      case produce(client, event) do
        {:ok, offset} -> {:cont, {:ok, offset}}
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
    Enum.reduce_while(events, {:ok, -1}, fn event, {:ok, _} ->
      case produce(client, topic, partition, key, event) do
        {:ok, offset} -> {:cont, {:ok, offset}}
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
