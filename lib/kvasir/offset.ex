defmodule Kvasir.Offset do
  defstruct partitions: %{}

  def create, do: %__MODULE__{}
  def create(offset), do: %__MODULE__{partitions: %{0 => offset}}
  def create(partition, offset), do: %__MODULE__{partitions: %{partition => offset}}

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

  def compare(%__MODULE__{partitions: p0}, %__MODULE__{partitions: p1}) do
    Enum.reduce_while(p0, :eq, fn {k, v0}, acc ->
      v1 = p1[k]

      cond do
        is_nil(v1) -> {:cont, acc}
        v0 < v1 -> if acc != :gt, do: {:cont, :lt}, else: {:halt, :mixed}
        v0 > v1 -> if acc != :lt, do: {:cont, :gt}, else: {:halt, :mixed}
        v0 == v1 -> if acc == :eq, do: {:cont, :eq}, else: {:halt, :mixed}
      end
    end)
  end

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
