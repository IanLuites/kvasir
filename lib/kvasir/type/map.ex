defmodule Kvasir.Type.Map do
  @moduledoc ~S"""

  """
  use Kvasir.Type

  @impl Kvasir.Type
  def parse(data, opts \\ [])

  def parse(data, opts) when is_map(data) do
    acc = if(s = opts[:struct], do: [__struct__: s], else: [])

    data
    |> Enum.to_list()
    |> transform(parser(opts[:key]), parser(opts[:value]), acc)
  end

  def parse(data, opts) when is_binary(data) do
    with {:error, _} <- Jason.decode(data, opts), do: {:error, :invalid_json}
  end

  def parse(_, _opts), do: {:error, :invalid_map}

  @impl Kvasir.Type
  def dump(data, opts \\ []) do
    data
    |> Map.delete(:__struct__)
    |> Enum.to_list()
    |> transform(dumper(opts[:key]), dumper(opts[:value]), [])
  end

  @impl Kvasir.Type
  def obfuscate(map, _opts), do: {:ok, Map.keys(map)}

  ### Helpers ###

  defp transform([], _key, _value, acc) do
    {:ok,
     acc
     |> :lists.reverse()
     |> :maps.from_list()}
  end

  defp transform([{item_k, item_v} | rest], key, value, acc) do
    with {:ok, k} <- key.(item_k),
         {:ok, v} <- value.(item_v) do
      transform(rest, key, value, [{k, v} | acc])
    end
  end

  defp parser(nil), do: &Kvasir.Util.identity/1

  defp parser({type, opts}) do
    t = Kvasir.Type.lookup(type)
    &t.parse(&1, opts)
  end

  defp parser(type) do
    t = Kvasir.Type.lookup(type)
    &t.parse/1
  end

  defp dumper(nil), do: &Kvasir.Util.identity/1

  defp dumper({type, opts}) do
    t = Kvasir.Type.lookup(type)
    &t.dump(&1, opts)
  end

  defp dumper(type) do
    t = Kvasir.Type.lookup(type)
    &t.dump(&1, [])
  end
end
