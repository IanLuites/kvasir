defmodule Kvasir.Util.AutoStart do
  @moduledoc ~S"""
  AutoStart registry stub.

  Overloaded with actual module at startup.
  """

  @doc ~S"""
  Start all auto start modules for a client
  """
  @spec start_link(module) :: term
  def start_link(_client), do: {:error, :stub}

  @doc ~S"""
  All available auto starts based on client.
  """
  @spec auto_starts :: map
  def auto_starts, do: %{}
end
