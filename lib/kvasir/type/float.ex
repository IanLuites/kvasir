defmodule Kvasir.Type.Float do
  @moduledoc ~S"""

  """
  use Kvasir.Type

  @impl Kvasir.Type
  def parse(number, opts \\ [])
  def parse(number, _opts) when is_float(number), do: {:ok, number}

  def parse(number, opts) when is_binary(number) do
    case Float.parse(number) do
      {number, ""} -> parse(number, opts)
      _ -> {:error, :invalid_float}
    end
  end

  def parse(_, _opts), do: {:error, :invalid_float}
end
