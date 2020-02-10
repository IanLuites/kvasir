defmodule Kvasir.Compression.Snappy do
  defmacro compress(data, _opts) do
    ensure_snappy!()

    quote do
      :snappyer.compress(unquote(data))
    end
  end

  defmacro decompress(data, _opts) do
    ensure_snappy!()

    quote do
      :snappyer.decompress(unquote(data))
    end
  end

  require Logger

  defp ensure_snappy! do
    unless Code.ensure_compiled?(:zstd) do
      Logger.error(fn ->
        """
        Missing `:snappyer` required for Snappy compression.

        Include `{:snappyer, "~> 1.2"}` in your `mix.exs` dependencies
        to enable Snappy compression support.
        """
      end)
    end

    :ok
  end
end
