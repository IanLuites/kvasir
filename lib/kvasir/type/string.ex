defmodule Kvasir.Type.String do
  @moduledoc ~S"""
  Any valid printable string.
  """
  use Kvasir.Type

  @impl Kvasir.Type
  def parse(value, opts \\ [])

  def parse(value, opts) when is_binary(value) do
    value = if(opts[:trim], do: String.trim(value), else: value)

    value =
      case opts[:case] do
        :lower -> String.downcase(value)
        :upper -> String.upcase(value)
        _ -> value
      end

    valid? = if r = opts[:regex], do: Regex.match?(r, value), else: true

    case {valid?, opts[:min], opts[:max]} do
      {false, _, _} ->
        {:error, opts[:error] || :string_not_valid}

      {_, nil, nil} ->
        {:ok, value}

      {_, min, nil} ->
        if(String.length(value) < min, do: {:error, :string_too_short}, else: {:ok, value})

      {_, nil, max} ->
        if(String.length(value) > max, do: {:error, :string_too_long}, else: {:ok, value})

      {_, min, max} ->
        length = String.length(value)

        cond do
          length < min -> {:error, :string_too_short}
          length > max -> {:error, :string_too_long}
          :ok -> {:ok, value}
        end
    end
  end

  def parse(_value, _opts), do: {:error, :invalid_string}

  @impl Kvasir.Type
  def obfuscate(value, _opts \\ []) do
    c = max(3, String.length(value) - 2)
    {:ok, String.first(value) <> String.duplicate("*", c) <> String.last(value)}
  end
end
