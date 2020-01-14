defmodule Kvasir.Offset do
  defstruct partitions: %{}

  def create, do: %__MODULE__{}
  def create(offset) when is_integer(offset), do: %__MODULE__{partitions: %{0 => offset}}
  def create(partitions), do: %__MODULE__{partitions: partitions}
  def create(partition, offset), do: %__MODULE__{partitions: %{partition => offset}}

  def chunk(offset, chunks \\ 0)
  def chunk(offset, 0), do: chunk_every(offset, 1)
  def chunk(offset, 1), do: offset

  def chunk(offset = %__MODULE__{partitions: p}, chunks) do
    chunk_every(offset, trunc(Float.ceil(map_size(p) / chunks)))
  end

  def chunk_every(%__MODULE__{partitions: partitions}, every) do
    partitions
    |> Enum.chunk_every(every)
    |> Enum.map(&%Kvasir.Offset{partitions: Map.new(&1)})
  end

  def get(%__MODULE__{partitions: p}, partition), do: p[partition] || 0

  def set(o = %__MODULE__{partitions: p}, partition, offset),
    do: %{o | partitions: Map.put(p, partition, offset)}

  def merge(o = %__MODULE__{partitions: p1}, %__MODULE__{partitions: p2}) do
    %{o | partitions: Map.merge(p1, p2)}
  end

  def bump(o = %__MODULE__{partitions: p}),
    do: %{o | partitions: Map.new(p, fn {k, v} -> {k, v + 1} end)}

  def bump_merge(o = %__MODULE__{}, nil), do: bump(o)
  def bump_merge(nil, o = %__MODULE__{}), do: bump(o)

  def bump_merge(o = %__MODULE__{partitions: p0}, %__MODULE__{partitions: p1}) do
    p =
      Enum.reduce(p1, p0, fn {k, v}, acc ->
        Map.update(acc, k, v + 1, &if(&1 == v, do: v, else: v + 1))
      end)

    %{o | partitions: p}
  end

  @doc ~S"""

  ## Examples

  ```elixir
  iex> a = create(%{0 => 24, 1 => 37})
  iex> b = create(%{0 => 24, 1 => 37})
  iex> compare(a, b)
  :eq
  ```

  ```elixir
  iex> a = create(%{0 => 12, 1 => 23})
  iex> b = create(%{0 => 24, 1 => 37})
  iex> compare(a, b)
  :lt
  ```

  ```elixir
  iex> a = create(%{0 => 24, 1 => 37})
  iex> b = create(%{0 => 12, 1 => 23})
  iex> compare(a, b)
  :gt
  ```

  Using `:earliest`:
  ```elixir
  iex> a = create(%{0 => :earliest})
  iex> b = create(%{0 => 0})
  iex> compare(a, b)
  :lt

  iex> a = create(%{0 => :earliest})
  iex> b = create(%{0 => :earliest})
  iex> compare(a, b)
  :eq
  ```

  """
  def compare(partition_a, partition_b) when is_map(partition_a) and is_map(partition_b) do
    p0 = partitions(partition_a)
    p1 = partitions(partition_b)

    Enum.reduce_while(p0, :eq, fn {k, v0}, acc ->
      case compare_value(v0, p1[k]) do
        nil -> {:cont, acc}
        :eq when acc == :eq -> {:cont, :eq}
        :lt when acc != :gt -> {:cont, :lt}
        :gt when acc != :lt -> {:cont, :gt}
        _ -> {:halt, :mixed}
      end
    end)
  end

  def compare(offset_a, offset_b), do: compare_value(offset_a, offset_b)

  defp partitions(%__MODULE__{partitions: p}), do: p
  defp partitions(p), do: p

  defp compare_value(a, b)
  defp compare_value(nil, _), do: nil
  defp compare_value(_, nil), do: nil
  defp compare_value(eq, eq), do: :eq
  defp compare_value(:earliest, _), do: :lt
  defp compare_value(_, :earliest), do: :gt
  defp compare_value(:latest, _), do: :gt
  defp compare_value(_, :latest), do: :lt
  defp compare_value(a, b), do: if(a < b, do: :lt, else: :gt)

  defimpl Jason.Encoder, for: __MODULE__ do
    alias Jason.Encoder.Map
    def encode(%{partitions: p}, opts), do: Map.encode(p, opts)
  end

  defimpl Inspect, for: __MODULE__ do
    import Inspect.Algebra

    def inspect(%{partitions: partitions}, opts) do
      parts = Enum.sort_by(partitions, &elem(&1, 0))

      if parts == [] do
        concat(["#Offset<", to_doc("EMPTY", opts), ">"])
      else
        prepared =
          parts
          |> Enum.intersperse(",")
          |> Enum.reduce(["#Offset<"], fn
            {p, o}, acc -> [to_doc(o, opts), ":", to_doc(p, opts) | acc]
            p, acc -> [p | acc]
          end)

        concat(:lists.reverse([">" | prepared]))
      end
    end
  end
end
