defmodule Kvasir.Event.Meta do
  defstruct [
    :topic,
    :partition,
    :offset,
    :key,
    :ts_type,
    :ts,
    :headers,
    :command
  ]

  defimpl Inspect do
    def inspect(%{offset: nil}, _opts), do: "#Kvasir.Event.Meta<UnPublished>"
    def inspect(%{offset: offset}, _opts), do: "#Kvasir.Event.Meta<#{offset}>"
  end
end
