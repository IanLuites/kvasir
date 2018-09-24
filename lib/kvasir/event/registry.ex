defmodule Kvasir.Event.Registry do
  @doc ~S"""
  Lookup events.
  """
  @spec lookup(String.t()) :: module | nil
  def lookup(_type), do: nil
end
