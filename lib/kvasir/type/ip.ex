defmodule Kvasir.Type.IP do
  @moduledoc ~S"""
  An IPv4 or IPv6 address.
  """
  use Kvasir.Type

  @impl Kvasir.Type
  def parse(value, opts \\ [])
  def parse(ip = {_, _, _, _}, _opts), do: {:ok, ip}
  def parse(ip = {_, _, _, _, _, _, _, _}, _opts), do: {:ok, ip}

  def parse(value, _opts) when is_binary(value) do
    with {:error, _} <- :inet.parse_address(String.to_charlist(value)),
         do: {:error, :invalid_ip_address}
  end

  def parse(_value, _opts), do: {:error, :invalid_ip_address}

  @impl Kvasir.Type
  def dump(value, _opts \\ []), do: {:ok, value |> :inet.ntoa() |> to_string()}

  @impl Kvasir.Type
  def obfuscate(value, opts \\ [])
  def obfuscate({a, _b, _c, d}, _opts), do: {:ok, "#{a}.*.*.#{d}"}

  def obfuscate({a, _b, _c, _d, _e, _f, _g, h}, _opts),
    do:
      {:ok,
       {a, 0, 0, 0, 0, 0, 0, h} |> :inet.ntoa() |> to_string() |> String.replace("::", ":*:")}
end
