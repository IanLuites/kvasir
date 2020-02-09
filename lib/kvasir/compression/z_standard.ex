defmodule Kvasir.Compression.ZStandard do
  defmacro compress(data, opts) do
    ensure_zstd!()
    level = opts[:level] || 3

    quote do
      {:ok, :zstd.compress(unquote(data), unquote(level))}
    end
  end

  defmacro decompress(data, _opts) do
    ensure_zstd!()

    quote do
      {:ok, :zstd.decompress(unquote(data))}
    end
  end

  require Logger

  defp ensure_zstd! do
    unless Code.ensure_compiled?(:zstd) do
      Logger.error(fn ->
        """
        Missing `:zstd` required for ZStandard compression.

        Include `{:zstd, "~> 0.2"}` in your `mix.exs` dependencies
        to enable ZStandard compression support.
        """
      end)
    end

    :ok
  end
end
