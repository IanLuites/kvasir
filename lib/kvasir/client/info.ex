defmodule Kvasir.Client.Info do
  @moduledoc false

  def topics(client) do
    with {:ok, %{topic_metadata: topics}} <- :brod_client.get_metadata(client, :all) do
      Enum.map(topics, & &1.topic)
    end
  end

  def leader(client, topic, partition) do
    :brod_client.get_leader_connection(client, topic, partition)
  end

  def offset(client, topic, :last) do
    with {:ok, offset} when offset > 0 <- offset(client, topic, :latest) do
      {:ok, offset - 1}
    end
  end

  def offset(client, topic, time) do
    with {:ok, %{topic_metadata: [meta]}} <- :brod_client.get_metadata(client, topic),
         partitions <- Enum.map(meta.partition_metadata, & &1.partition),
         leaders <- Enum.map(partitions, &leader(client, topic, &1)),
         true <- Enum.all?(leaders, &(elem(&1, 0) == :ok)),
         leaders <- Enum.zip(partitions, Enum.map(leaders, &elem(&1, 1))) do
      gather_offset(leaders, topic, time, 0)
    end
  end

  defp gather_offset([], _topic, _time, offset), do: {:ok, offset}

  defp gather_offset([{partition, leader} | rest], topic, time, offset) do
    with {:ok, p_offset} <- :brod_utils.resolve_offset(leader, topic, partition, time) do
      if p_offset > offset,
        do: gather_offset(rest, topic, time, p_offset),
        else: gather_offset(rest, topic, time, offset)
    end
  end
end
