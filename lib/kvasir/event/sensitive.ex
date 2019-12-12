defmodule Kvasir.Event.Sensitive do
  @moduledoc false
  defstruct ~w(value type opts)a

  defimpl Inspect, for: __MODULE__ do
    def inspect(%Kvasir.Event.Sensitive{value: value, type: type, opts: o}, opts) do
      case type.obfuscate(value, o) do
        {:ok, v} -> Inspect.Algebra.to_doc(v, opts)
        :obfuscate -> "🔒"
        {:error, _} -> value
      end
    end
  end
end
