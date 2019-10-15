defmodule Kvasir.Key.Email do
  @moduledoc ~S"""
  Email key applies the same partitioning logic as strings.
  """
  use Kvasir.Key, type: Kvasir.Type.Email

  @impl Kvasir.Key
  def partition(value, partitions),
    do: value |> String.to_charlist() |> Enum.sum() |> rem(partitions)
end
