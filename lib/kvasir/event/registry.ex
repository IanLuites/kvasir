defmodule Kvasir.Event.Registry do
  @doc ~S"""
  Lookup events.
  """
  @spec lookup(String.t()) :: module | nil
  def lookup(type), do: nil
end
