defmodule Kvasir.Key.Integer do
  @moduledoc ~S"""

  """
  use Kvasir.Key, type: Kvasir.Type.PosInteger

  @impl Kvasir.Key
  def partition(value, partitions), do: {:ok, rem(value, partitions)}
end
