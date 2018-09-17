defmodule Kvasir.Event.Decoder do
  def decode_as(value, meta \\ %Kvasir.Event.Meta{}, event)

  def decode_as({:kafka_message, offset, key, value, ts_type, ts, headers}, _meta, event) do
    decode_as(
      value,
      %Kvasir.Event.Meta{
        offset: offset,
        key: key,
        ts_type: ts_type,
        ts: ts,
        headers: headers
      },
      event
    )
  end

  def decode_as(value, meta, event) when is_binary(value) do
    case Jason.decode(value) do
      {:ok, decoded} ->
        decode_event(decoded["payload"], update_meta(meta, decoded["meta"]), event)

      _ ->
        {:error, :invalid_payload}
    end
  end

  def decode_as!(value, event) do
    with {:ok, event} <- decode_as(value, event) do
      event
    else
      _ -> nil
    end
  end

  def decode(value, events \\ [])

  def decode({:kafka_message_set, topic, _from, _to, values}, opts) do
    do_multi_decode(values, Keyword.put(opts, :topic, topic), [])
  end

  def decode({:kafka_message, offset, key, value, ts_type, ts, headers}, opts) do
    decode(
      value,
      %Kvasir.Event.Meta{
        partition: opts[:partition],
        topic: opts[:topic],
        offset: offset,
        key: key,
        ts_type: ts_type,
        ts: ts,
        headers: headers
      },
      opts[:events] || []
    )
  end

  def decode(value, meta = %Kvasir.Event.Meta{}, events) when is_binary(value) do
    case Jason.decode(value) do
      {:ok, decoded} ->
        do_decode(decoded, update_meta(meta, decoded["meta"]), events)

      _ ->
        {:ok, %Kvasir.Event{value: value, __meta__: meta}}
    end
  end

  defp update_meta(meta, data) when is_map(data) do
    struct!(
      Kvasir.Event.Meta,
      for(
        {key, val} when val != nil <- data,
        into: Map.from_struct(meta),
        do: {String.to_atom(key), val}
      )
    )
  end

  defp update_meta(meta, _nil), do: meta

  defp do_decode(data, meta, events) do
    type = data["type"]

    cond do
      is_nil(type) ->
        {:ok, %Kvasir.Event{value: data, __meta__: meta}}

      event = Enum.find(events, &(&1.__event__(:type) == type)) ->
        decode_event(data["payload"], meta, event) || %Kvasir.Event{value: data, __meta__: meta}

      :default_event ->
        {:ok, %Kvasir.Event{value: data, __meta__: meta}}
    end
  end

  defp decode_event(data, meta, event),
    do: do_decode_event(event.__event__(:fields), data, %{__meta__: meta}, event)

  defp do_decode_event([], _data, acc, event), do: {:ok, struct!(event, acc)}

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

  defp do_multi_decode([], opts, acc), do: {:ok, Enum.reverse(acc)}

  defp do_multi_decode([head | tail], opts, acc) do
    case decode(head, opts) do
      {:ok, e} -> do_multi_decode(tail, opts, [e | acc])
      error -> error
    end
  end
end
