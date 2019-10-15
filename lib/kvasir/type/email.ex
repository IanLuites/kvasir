defmodule Kvasir.Type.Email do
  @moduledoc ~S"""

  """
  use Kvasir.Type

  @impl Kvasir.Type
  def parse(value, opts \\ [])

  def parse(value, _opts) when is_binary(value) do
    if String.contains?(value, "@") do
      {:ok, value}
    else
      {:error, :invalid_email}
    end
  end

  def parse(_value, _opts), do: {:error, :invalid_email}

  @impl Kvasir.Type
  def obfuscate(value, _opts) do
    {name, domain} = value |> String.split("@") |> Enum.split(-1)
    name = Enum.join(name, "")
    {:ok, "#{String.first(name)}***#{String.last(name)}@#{domain}"}
  end
end
