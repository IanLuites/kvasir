defmodule Kvasir.Event.Encoding do
  alias Kvasir.Event.Meta
  alias Kvasir.Type.Serializer, as: S

  ## Public
  def encode(data = %event{__meta__: meta = %{key_type: key_type}}) do
    with {:ok, m} <- Meta.encode(meta, key_type),
         {:ok, payload} <- S.encode(event.__event__(:fields), data) do
      {:ok,
       %{
         type: event.__event__(:type),
         version: to_string(event.__event__(:version)),
         payload: payload,
         meta: m
       }}
    end
  end

  def encode(topic, data = %event{__meta__: meta}, _opts \\ []) do
    with {:ok, m} <- Meta.encode(meta, topic.key),
         {:ok, payload} <- S.encode(event.__event__(:fields), data) do
      {:ok,
       %{
         type: event.__event__(:type),
         version: to_string(event.__event__(:version)),
         payload: payload,
         meta: m
       }}
    end
  end

  def decode(topic, value, opts \\ [])

  def decode(topic, value, opts) when is_binary(value) do
    with {:ok, data} <- Jason.decode(value), do: decode(topic, data, opts)
  end

  def decode(%{event_lookup: lookup, key: key}, data, opts) do
    if event = lookup.(MapX.get(data, :type)) do
      decode(event, key, data, opts)
    else
      {:error, :unknown_event_type}
    end
  end

  def decode(event, key, data, _opts) do
    with {:ok, meta} <- Meta.decode(MapX.get(data, :meta), key),
         {:ok, version} <- Version.parse(MapX.get(data, :version)),
         {:ok, payload} <- event.upgrade(version, MapX.get(data, :payload)),
         {:ok, e} <- S.decode(event.__event__(:fields), payload),
         do: {:ok, %{struct!(event, e) | __meta__: %{meta | key_type: key}}}
  end

  def create(event, data, opts \\ []),
    do: decode(event, nil, %{version: to_string(event.__event__(:version)), payload: data}, opts)

  def binary_decode(event, key, version, packed) do
    unpacked =
      case Msgpax.unpack(packed) do
        {:ok, [m, p]} -> {:ok, {m, p}}
        {:ok, p} -> {:ok, {%{}, p}}
        err -> err
      end

    with {:ok, {m, p}} <- unpacked,
         {:ok, meta} <- Meta.decode(m, key),
         {:ok, payload} <- event.upgrade(version, p),
         {:ok, e} <- S.decode(event.__event__(:fields), payload),
         do: {:ok, %{struct!(event, e) | __meta__: %{meta | key_type: key}}}
  end
end
