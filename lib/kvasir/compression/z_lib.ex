defmodule Kvasir.Compression.ZLib do
  defmacro compress(data, opts) do
    if Keyword.get(opts, :headers, true) do
      quote do
        {:ok, :zlib.gzip(unquote(data))}
      end
    else
      quote do
        {:ok, :zlib.zip(unquote(data))}
      end
    end
  end

  defmacro decompress(data, opts) do
    if Keyword.get(opts, :headers, true) do
      quote do
        {:ok, :zlib.gunzip(unquote(data))}
      end
    else
      quote do
        {:ok, :zlib.unzip(unquote(data))}
      end
    end
  end
end
