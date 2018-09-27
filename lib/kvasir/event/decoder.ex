defmodule Kvasir.Event.Decoder do
  alias Kvasir.Event.Meta

  def decode(value, opts \\ [])

  def decode({:kafka_message_set, topic, _from, _to, values}, opts) do
    do_multi_decode(values, Keyword.put(opts, :topic, topic), [])
  end

  def decode({:kafka_message, offset, key, value, ts_type, ts, headers}, opts) do
    partition = opts[:partition] || 0

    decode(
      value,
      Keyword.put(
        opts,
        :meta,
        %Meta{
          partition: partition,
          topic: opts[:topic],
          offset: Kvasir.Offset.create(partition, offset),
          key: key,
          ts_type: ts_type,
          ts: ts,
          headers: headers
        }
      )
    )
  end

  def decode(value, opts) when is_binary(value) do
    case Jason.decode(value) do
      {:ok, decoded} ->
        do_decode(decoded, Meta.decode(decoded["meta"], opts[:meta]))

      _ ->
        {:ok, %Kvasir.Event{value: value, __meta__: Meta.decode(nil, opts[:meta])}}
    end
  end

  defp do_decode(data, meta) do
    if event = Kvasir.Event.Registry.lookup(data["type"]) do
      decode_event(data["payload"], meta, event) || %Kvasir.Event{value: data, __meta__: meta}
    else
      {:ok, %Kvasir.Event{value: data, __meta__: meta}}
    end
  end

  defp decode_event(data, meta, event),
    do: do_decode_event(event.__event__(:fields), data, %{__meta__: meta}, event)

  defp do_decode_event([], _data, acc, event) do
    event = struct!(event, acc)
    meta = event.__meta__
    key_type = Kvasir.Event.key_type(event)
    {:ok, %{event | __meta__: %{meta | key: key_decode(key_type, meta.key)}}}
  end

  defp do_decode_event([{property, type, opts} | props], data, acc, event) do
    with {:ok, value} <- Map.fetch(data, to_string(property)),
         {:ok, parsed_value} <- Kvasir.Type.load(type, value, opts) do
      do_decode_event(props, data, Map.put(acc, property, parsed_value), event)
    else
      :error ->
        if opts[:optional],
          do: do_decode_event(props, data, acc, event),
          else: {:error, :missing_field}

      error = {:error, _} ->
        error
    end
  end

  defp do_multi_decode([], _opts, acc), do: {:ok, Enum.reverse(acc)}

  defp do_multi_decode([head | tail], opts, acc) do
    case decode(head, opts) do
      {:ok, e} -> do_multi_decode(tail, opts, [e | acc])
      error -> error
    end
  end

  defp key_decode(:integer, value) when is_integer(value), do: value
  defp key_decode(:integer, value), do: String.to_integer(value)
  defp key_decode(:string, value) when is_integer(value), do: to_string(value)
  defp key_decode(:string, value), do: value
end
