defmodule Kvasir.Type.String do
  @moduledoc ~S"""

  """
  use Kvasir.Type

  @impl Kvasir.Type
  def parse(value, opts \\ [])
  def parse(value, _opts) when is_binary(value), do: {:ok, value}
  def parse(_value, _opts), do: {:error, :invalid_string}

  @impl Kvasir.Type
  def obfuscate(value, _opts \\ []) do
    c = max(3, String.length(value) - 2)
    {:ok, String.first(value) <> String.duplicate("*", c) <> String.last(value)}
  end
end
