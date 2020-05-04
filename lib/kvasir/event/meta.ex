defmodule Kvasir.Event.Meta do
  @type t :: %__MODULE__{
          source: module,
          topic: String.t(),
          partition: non_neg_integer,
          offset: non_neg_integer,
          key: term,
          sub_key: nil | [atom | {atom, term}],
          timestamp: UTCDateTime.t(),
          meta: map
        }

  defstruct [
    :source,
    :topic,
    :partition,
    :offset,
    :key,
    :key_type,
    :sub_key,
    :timestamp,
    :meta
  ]

  defimpl Inspect do
    def inspect(%{offset: nil}, _opts), do: "#Kvasir.Event.Meta<UnPublished>"
    def inspect(%{offset: offset}, _opts), do: "#Kvasir.Event.Meta<#{offset}>"
  end

  @doc false
  @spec encode(t) :: map
  def encode(meta) do
    meta
    |> Map.from_struct()
    |> Map.delete(:key_type)
    |> Map.update(:timestamp, nil, &ts_encode/1)
    |> MapX.update_if_exists(:sub_key, fn
      nil ->
        nil

      path ->
        Enum.map(path, fn
          {a, b} -> [a, b]
          a -> a
        end)
    end)
    |> Enum.reject(&nil_value/1)
    |> Enum.into(%{})
  end

  @doc false
  @spec decode(map | nil) :: t
  def decode(nil), do: %__MODULE__{}

  def decode(data) when is_map(data) do
    struct!(
      __MODULE__,
      data
      |> Enum.reject(&nil_value/1)
      |> Enum.into(%{}, &atomize/1)
      |> Map.update(:timestamp, nil, &ts_decode/1)
      |> Map.update(:sub_key, nil, fn path ->
        Enum.map(path, fn
          [a, b] -> {String.to_atom(a), b}
          a -> String.to_atom(a)
        end)
      end)
    )
  end

  @doc false
  @spec encode(t, module) :: {:ok, map} | {:error, atom}
  def encode(meta, key) do
    case encode(meta) do
      data = %{key: k} when k != nil ->
        with {:ok, k} <- key.dump(k, []), do: {:ok, %{data | key: k}}

      data ->
        {:ok, data}
    end
  end

  @doc false
  @spec decode(map | nil, module) :: {:ok, t} | {:error, atom}
  def decode(data, key) do
    case decode(data) do
      meta = %{key: nil} -> {:ok, meta}
      meta = %{key: k} -> with {:ok, k} <- key.parse(k, []), do: {:ok, %{meta | key: k}}
    end
  end

  defp ts_decode(nil), do: nil
  defp ts_decode(v), do: UTCDateTime.from_unix!(v, :millisecond)

  defp ts_encode(nil), do: nil
  defp ts_encode(v), do: UTCDateTime.to_unix(v, :millisecond)

  defp atomize(e = {k, _}) when is_atom(k), do: e
  defp atomize({k, v}), do: {String.to_existing_atom(k), v}

  defp nil_value({_, nil}), do: true
  defp nil_value(_), do: false
end
