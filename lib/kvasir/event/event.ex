defmodule Kvasir.Event do
  @type t :: map

  defstruct [
    :value,
    :__meta__
  ]

  defmacro __using__(opts \\ []) do
    on_error = opts[:on_error] || :halt
    key_type = opts[:key_type] || :string

    quote do
      import Kvasir.Event, only: [event: 2]
      @on_error unquote(on_error)
      @key_type unquote(key_type)
    end
  end

  defmacro field(name, type \\ :string, opts \\ []) do
    quote do
      Module.put_attribute(__MODULE__, :fields, {unquote(name), unquote(type), unquote(opts)})
    end
  end

  defmacro event(type, do: block) do
    registry = Module.concat(Kvasir.Event.Registry, __CALLER__.module)

    quote do
      Module.register_attribute(__MODULE__, :fields, accumulate: true)

      try do
        import Kvasir.Event, only: [field: 1, field: 2, field: 3]
        unquote(block)
      after
        :ok
      end

      defmodule unquote(registry) do
        @moduledoc false

        @doc false
        @spec type :: String.t()
        def type, do: unquote(Kvasir.Util.name(type))

        @doc false
        @spec module :: module
        def module, do: unquote(__CALLER__.module)
      end

      @event_fields Enum.reverse(@fields)
      defstruct Enum.map(@event_fields, fn {f, _, opts} -> {f, opts[:default]} end) ++
                  [__meta__: %Kvasir.Event.Meta{}]

      @doc false
      @spec __event__(atom) :: term
      def __event__(:type), do: unquote(Kvasir.Util.name(type))
      def __event__(:fields), do: @event_fields
      def __event__(:on_error), do: @on_error
      def __event__(:key_type), do: @key_type

      defimpl Jason.Encoder, for: __MODULE__ do
        alias Jason.EncodeError
        alias Jason.Encoder.Map
        alias Kvasir.Event.Encoder

        def encode(value, opts) do
          case Encoder.encode(value, encoding: :raw) do
            {:ok, data} -> Map.encode(data, opts)
            {:error, error} -> %EncodeError{message: "Event Encoding Error: #{error}"}
          end
        end
      end
    end
  end

  defdelegate encode(value, opts \\ []), to: Kvasir.Event.Encoder
  defdelegate decode(value, opts \\ []), to: Kvasir.Event.Decoder

  @doc ~S"""
  """
  @spec event?(any) :: boolean
  def event?(%event{}), do: event?(event)
  def event?(event) when is_atom(event), do: :erlang.function_exported(event, :__event__, 1)
  def event?(_), do: false

  @unix ~N[1970-01-01 00:00:00]
  @spec timestamp(t) :: NaiveDateTime.t()
  def timestamp(%{__meta__: %{ts: ts}}) when is_integer(ts),
    do: NaiveDateTime.add(@unix, ts, :millisecond)

  def timestamp(_), do: nil

  @spec id(t) :: term
  def id(event), do: key(event)

  @spec key(t) :: term
  def key(%{__meta__: %{key: key}}), do: key
  def key(_), do: nil

  @spec key(t) :: :string | :integer
  def key_type(%event{}), do: event.__event__(:key_type)
  def key_type(event) when is_atom(event), do: event.__event__(:key_type)
  def key_type(_), do: :string

  @spec on_error(t) :: :halt | :skip
  def on_error(%__MODULE__{}), do: :halt
  def on_error(%event{}), do: event.__event__(:on_error)

  @spec type(t) :: String.t() | nil
  def type(%event{}), do: event.__event__(:type)
  def type(event) when is_atom(event), do: event.__event__(:type)
  def type(_), do: nil
end
