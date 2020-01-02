defmodule Kvasir.Key.String do
  @moduledoc ~S"""
  String key is partitioned by summing the character values
  and modulo-ing them with the partitions.
  """
  use Kvasir.Key, type: Kvasir.Type.String

  @impl Kvasir.Key
  def partition(value, partitions),
    do: {:ok, value |> String.to_charlist() |> Enum.sum() |> rem(partitions)}
end
