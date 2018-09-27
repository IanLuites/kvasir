defmodule Kvasir.Offset do
  @moduledoc ~S"""
  Kafka offset per partition.
  """

  @typedoc @moduledoc
  @type t :: %__MODULE__{} | :earliest
  defstruct partitions: %{},
            offset: 0

  @spec create(map | list) :: t
  def create(partitions \\ [])

  def create(offset = %__MODULE__{}), do: offset

  def create(partitions) do
    %__MODULE__{
      partitions:
        Enum.into(partitions, %{}, fn
          {p, o} when is_binary(p) -> {String.to_integer(p), o}
          {p, o} -> {p, o}
          p -> {p, 0}
        end)
    }
  end

  @spec create(non_neg_integer, non_neg_integer) :: t
  def create(partition, offset), do: %__MODULE__{partitions: %{partition => offset}}

  @spec offset(t) :: non_neg_integer
  def offset(%__MODULE__{partitions: partitions}) do
    partitions
    |> Map.values()
    |> Enum.sum()
  end

  @spec set(t, t) :: t
  def set(:earliest, set), do: set(create(), set)

  def set(t = %__MODULE__{partitions: partitions}, %__MODULE__{partitions: set}),
    do: %{t | partitions: Map.merge(partitions, set)}

  @spec set(t, non_neg_integer, non_neg_integer) :: t
  def set(t = %__MODULE__{partitions: partitions}, partition, offset),
    do: %{t | partitions: Map.put(partitions, partition, offset)}

  @spec partition(t, non_neg_integer) :: t
  def partition(:earliest, _partition), do: :earliest

  def partition(%__MODULE__{partitions: partitions}, partition),
    do: Map.get(partitions, partition, 0)

  @spec partitions(t) :: [non_neg_integer]
  def partitions(%__MODULE__{partitions: partitions}), do: Map.keys(partitions)

  @spec compare(t, t | map | list) :: :lt | :eg | :gt
  def compare(:earliest, :earliest), do: :eq
  def compare(:earliest, %__MODULE__{partitions: to}), do: if(to == %{}, do: :eq, else: :lt)
  def compare(%__MODULE__{partitions: from}, :earliest), do: if(from == %{}, do: :eq, else: :gt)

  def compare(%__MODULE__{partitions: partitions}, %__MODULE__{partitions: to}),
    do: do_compare(partitions, Enum.to_list(to))

  def compare(%__MODULE__{partitions: partitions}, to),
    do: do_compare(partitions, Enum.to_list(to))

  @spec compare(map, list) :: :lt | :eg | :gt
  def do_compare(_, []), do: :eq

  def do_compare(partitions, [{p, o} | t]) do
    cmp = Map.get(partitions, p)

    cond do
      is_nil(cmp) -> do_compare(partitions, t)
      cmp == o -> do_compare(partitions, t)
      cmp < o -> :lt
      cmp > o -> :gt
    end
  end

  defimpl Inspect, for: __MODULE__ do
    def inspect(offset, _opts), do: "#Kvasir.Offset<#{offset}>"
  end

  defimpl String.Chars, for: __MODULE__ do
    def to_string(%Kvasir.Offset{partitions: partitions}) do
      case Enum.to_list(partitions) do
        [] -> "0"
        [{0, o}] -> Kernel.to_string(o)
        offsets -> offsets |> Enum.map(fn {p, o} -> "#{p}:#{o}" end) |> Enum.join(",")
      end
    end
  end

  defimpl Jason.Encoder, for: __MODULE__ do
    alias Jason.Encoder.Map
    def encode(%Kvasir.Offset{partitions: partitions}, opts), do: Map.encode(partitions, opts)
  end
end
