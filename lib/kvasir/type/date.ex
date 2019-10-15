defmodule Kvasir.Type.Date do
  @moduledoc ~S"""

  """
  use Kvasir.Type

  @impl Kvasir.Type
  def parse(date, opts \\ [])
  def parse(date = %Date{}, _opts), do: {:ok, date}

  def parse(date, _opts) when is_binary(date) do
    with {:error, _} <- Date.from_iso8601(date), do: {:error, :invalid_date_format}
  end

  def parse(_, _opts), do: {:error, :invalid_date}
end
