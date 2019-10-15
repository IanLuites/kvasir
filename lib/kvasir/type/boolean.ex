defmodule Kvasir.Type.Boolean do
  @moduledoc ~S"""

  """
  use Kvasir.Type

  @impl Kvasir.Type
  def parse(boolean, opts \\ [])
  def parse(true, _opts), do: {:ok, true}
  def parse(false, _opts), do: {:ok, false}

  def parse(binary, _opts) when is_binary(binary) do
    binary = String.downcase(binary)

    cond do
      binary in ["true", "1"] -> {:ok, true}
      binary in ["false", "0"] -> {:ok, false}
      :error -> {:error, :invalid_boolean}
    end
  end

  def parse(_, _opts), do: {:error, :invalid_boolean}
end
