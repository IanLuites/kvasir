defmodule Kvasir.Event.Meta do
  @type t :: %__MODULE__{}

  defstruct [
    :topic,
    :partition,
    :offset,
    :key,
    :ts_type,
    :ts,
    :headers,
    :command
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
    |> Enum.reject(&is_nil(elem(&1, 1)))
    |> Enum.map(fn {k, v} -> {k, if(is_tuple(v), do: Tuple.to_list(v), else: v)} end)
    |> Enum.into(%{})
  end

  @doc false
  @spec encode(map) :: t
  def decode(data, meta \\ %__MODULE__{})
  def decode(data, nil), do: decode(data)
  def decode(nil, meta), do: meta

  def decode(data, meta) when is_map(data) do
    struct!(
      __MODULE__,
      for(
        {key, val} when val != nil <- data,
        into: Map.from_struct(meta),
        do: parse_meta(to_string(key), val)
      )
    )
  end

  @spec parse_meta(String.t(), any) :: {atom, any}
  defp parse_meta("offset", nil), do: {:offset, nil}
  defp parse_meta("offset", offset), do: {:offset, Kvasir.Offset.create(offset)}
  defp parse_meta(key, value), do: {String.to_existing_atom(key), value}
end
