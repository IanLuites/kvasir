defmodule Kvasir.Event do
  defstruct [
    :value,
    :__meta__
  ]

  defmacro __using__(_opts \\ []) do
    quote do
      import Kvasir.Event, only: [defevent: 2]
    end
  end

  defmacro field(name, type) do
    quote do
      Module.put_attribute(__MODULE__, :event_fields, {unquote(name), unquote(type)})
    end
  end

  defmacro defevent(type, do: block) do
    quote do
      Module.register_attribute(__MODULE__, :event_fields, accumulate: true)

      try do
        import Kvasir.Event, only: [field: 2]
        unquote(block)
      after
        :ok
      end

      @struct_fields Enum.reverse(@event_fields)
      defstruct Enum.map(@struct_fields, &elem(&1, 0)) ++ [__meta__: %Kvasir.Event.Meta{}]

      @doc false
      def __event__(:type), do: unquote(to_string(elem(type, 0)))
      def __event__(:fields), do: @struct_fields

      defimpl Jason.Encoder, for: __MODULE__ do
        def encode(value, opts) do
          Jason.Encode.map(Kvasir.Event.Encoder.encode(value), opts)
        end
      end
    end
  end
end
