defmodule Kvasir.Type.Timestamp do
  @moduledoc ~S"""
  A timestamp containing date and time.

  By default the timestamp is UTC only.
  """
  use Kvasir.Type

  @impl Kvasir.Type
  def parse(timestamp, opts \\ [])

  def parse(timestamp = %t{}, _opts) when t in [UTCDateTime, DateTime, NaiveDateTime],
    do: {:ok, timestamp}

  def parse(timestamp, opts) when is_binary(timestamp) do
    case opts[:format] do
      nil -> utc_from_8601(timestamp)
      UTCDateTime -> utc_from_8601(timestamp)
      DateTime -> dt_from_8601(timestamp)
      NaiveDateTime -> ndt_from_8601(timestamp)
    end
  end

  def parse(timestamp, opts) when is_integer(timestamp) do
    case opts[:format] do
      nil -> utc_from_unix(timestamp)
      UTCDateTime -> utc_from_unix(timestamp)
      DateTime -> dt_from_unix(timestamp)
      NaiveDateTime -> ndt_from_unix(timestamp)
    end
  end

  def parse(_, _opts), do: {:error, :invalid_timestamp}

  @impl Kvasir.Type
  def dump(data, opts \\ [])
  def dump(data = %UTCDateTime{}, _opts), do: {:ok, UTCDateTime.to_rfc3339(data)}
  def dump(data = %DateTime{}, _opts), do: {:ok, DateTime.to_iso8601(data)}
  def dump(data = %NaiveDateTime{}, _opts), do: {:ok, NaiveDateTime.to_iso8601(data) <> "Z"}

  ### Helpers ###

  defp dt_from_8601(timestamp) do
    case NaiveDateTime.from_iso8601(timestamp) do
      {:ok, t, _} -> {:ok, t}
      _ -> {:error, :invalid_timestamp}
    end
  end

  defp ndt_from_8601(timestamp) do
    with {:error, _} <- NaiveDateTime.from_iso8601(timestamp), do: {:error, :invalid_timestamp}
  end

  defp utc_from_8601(timestamp) do
    with {:error, _} <- UTCDateTime.from_iso8601(timestamp), do: {:error, :invalid_timestamp}
  end

  defp dt_from_unix(unix), do: DateTime.from_unix(unix)
  defp ndt_from_unix(unix), do: NaiveDateTime.add(~N[1970-01-01 00:00:00], unix)

  defp utc_from_unix(unix),
    do: ~N[1970-01-01 00:00:00] |> NaiveDateTime.add(unix) |> UTCDateTime.from_naive()
end
