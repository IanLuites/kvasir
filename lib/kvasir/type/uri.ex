defmodule Kvasir.Type.URI do
  @moduledoc ~S"""
  Any valid URI.

  The URI validation can enforce specific `host` or `scheme` values,
  by default `host` and `scheme` can not be `nil`.

  The following type of checks are allow:

    - `nil`, everything but `nil` allowed.
    - `binary`, any string to match.
    - `regex`, any regex to match.
    - `list`, any list of (string) values.
  """
  use Kvasir.Type

  @impl Kvasir.Type
  def parse(value, opts \\ [])

  def parse(uri = %URI{host: host, scheme: scheme}, opts) do
    cond do
      not eq?(host, opts[:host]) -> {:invalid_uri_host}
      not eq?(scheme, opts[:scheme]) -> {:invalid_uri_scheme}
      true -> {:ok, uri}
    end
  end

  def parse(value, opts) when is_binary(value), do: value |> Elixir.URI.parse() |> parse(opts)
  def parse(_value, _opts), do: {:error, :invalid_uri_format}

  @impl Kvasir.Type
  def dump(value, _opts \\ []), do: {:ok, to_string(value)}

  @impl Kvasir.Type
  def obfuscate(value, opts \\ []), do: value |> to_string |> Kvasir.Type.String.obfuscate(opts)

  @spec eq?(term, nil | String.t() | Regex.t() | list) :: boolean
  defp eq?(nil, _), do: false
  defp eq?(_, nil), do: true
  defp eq?(value, match) when is_binary(match), do: value == match
  defp eq?(value, match = %Regex{}), do: value =~ match
  defp eq?(value, match) when is_list(match), do: value in match
end
