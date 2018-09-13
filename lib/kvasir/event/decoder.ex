defmodule Kvasir.Event.Decoder do
  def decode_as({:kafka_message, offset, key, value, ts_type, ts, headers}, event) do
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

  def decode_as(value, meta = %Kvasir.Event.Meta{}, event) when is_binary(value) do
    case Jason.decode(value) do
      {:ok, decoded} ->
        meta = update_meta(meta, decoded["meta"])

        if event = decode_event(decoded, meta, event),
          do: {:ok, event},
          else: {:error, :invalid_payload}

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

  def decode({:kafka_message_set, _topic, _from, _to, values}, events),
    do: Enum.map(values, &decode(&1, events))

  def decode({:kafka_message, offset, key, value, ts_type, ts, headers}, events) do
    decode(
      value,
      %Kvasir.Event.Meta{
        offset: offset,
        key: key,
        ts_type: ts_type,
        ts: ts,
        headers: headers
      },
      events
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
        %Kvasir.Event{value: data, __meta__: meta}

      event = Enum.find(events, &(&1.__event__(:type) == type)) ->
        decode_event(data["payload"], meta, event) || %Kvasir.Event{value: data, __meta__: meta}

      :default_event ->
        %Kvasir.Event{value: data, __meta__: meta}
    end
  end

  defp decode_event(data, meta, event),
    do: do_decode_event(event.__event__(:fields), data, struct(event, %{__meta__: meta}))

  defp do_decode_event([], _data, acc), do: acc

  defp do_decode_event([{property, type} | props], data, acc) do
    case Map.fetch(data, to_string(property)) do
      {:ok, value} ->
        do_decode_event(props, data, Map.put(acc, property, parse_type(value, type)))

      :error ->
        nil
    end
  end

  @unix ~N[1970-01-01 00:00:00]
  defp parse_type(nil, _), do: nil
  defp parse_type(value, :datetime), do: NaiveDateTime.add(@unix, value, :milliseconds)
  defp parse_type(value, _), do: value
end
