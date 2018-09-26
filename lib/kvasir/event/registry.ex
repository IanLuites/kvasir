defmodule Kvasir.Event.Registry do
  @moduledoc ~S"""
  Event registry stub.

  Overloaded with actual module at startup.
  """

  @doc ~S"""
  Lookup events.
  """
  @spec lookup(String.t()) :: module | nil
  def lookup(_type), do: nil

  @doc ~S"""
  List all events.
  """
  @spec list :: [module]
  def list, do: []

  @doc ~S"""
  List all events matching the filter.
  """
  @spec list(filter :: Keyword.t()) :: [module]
  def list(_filter), do: []

  @doc ~S"""
  All available events indexed on type.
  """
  @spec events :: map
  def events, do: %{}
end
