defmodule Kvasir.Compression.ZStandard do
  defmacro compress(data, opts) do
    ensure_zstd!()
    level = opts[:level] || 3

    quote do
      {:ok, :ZStandard.compress(unquote(data), unquote(level))}
    end
  end

  defmacro decompress(data, _opts) do
    ensure_zstd!()

    quote do
      {:ok, :ZStandard.decompress(unquote(data))}
    end
  end

  require Logger

  defp ensure_zstd! do
    unless CodeX.ensure_compiled?(ZStandard) do
      Logger.error(fn ->
        """
        Missing `:z_standard` required for ZStandard compression.

        Include `{:z_standard, "~> 0.0.1"}` in your `mix.exs` dependencies
        to enable ZStandard compression support.
        """
      end)
    end

    :ok
  end
end
