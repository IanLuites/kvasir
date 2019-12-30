defmodule Kvasir.Event do
  require Logger

  @type t :: map

  defstruct [
    :value,
    :__meta__
  ]

  defmacro __using__(opts \\ []) do
    on_error = opts[:on_error] || :halt

    if opts[:key_type] do
      Logger.warn(fn ->
        """
        Kvasir: #{inspect(__CALLER__.module)} set `:key_type`. This has been deprecated.

          `:key_type` is now set per topic in the `Kvasir.EventSource`.
        """
      end)
    end

    quote do
      import Kvasir.Event, only: [event: 2, upgrade: 2, version: 1, version: 2]
      @before_compile Kvasir.Event
      @on_error unquote(on_error)

      # Version Tracking
      Module.register_attribute(__MODULE__, :version, persist: true, accumulate: true)
      @version {Version.parse!("1.0.0"), nil, "Create event."}
    end
  end

  defmacro __before_compile__(env) do
    upgrades =
      env.module
      |> Module.get_attribute(:upgrades, [])
      |> Enum.reduce(nil, fn {version, func}, acc ->
        quote do
          def upgrade(
                %Version{
                  major: unquote(Macro.var(:major, nil)),
                  minor: unquote(Macro.var(:minor, nil)),
                  patch: unquote(Macro.var(:patch, nil))
                },
                event
              )
              when unquote(guard(version)) do
            _ = unquote(Macro.var(:major, nil))
            _ = unquote(Macro.var(:minor, nil))
            _ = unquote(Macro.var(:patch, nil))
            unquote(func)(event)
          end

          unquote(acc)
        end
      end)

    quote do
      @doc ~S"""
      Upgrade event payload from older to current version.

      ## Examples

      ```elixir
      iex> upgrade(#Version<1.0.0>, %Event{...})
      ```
      """
      @spec upgrade(Version.t(), map) :: {:ok, map} | {:error, atom}
      def upgrade(version, event)
      unquote(upgrades)
      def upgrade(_, event), do: {:ok, event}
    end
  end

  defmacro field(name, type \\ :string, opts \\ []) do
    opts = Keyword.put_new(opts, :sensitive, false)

    quote do
      Module.put_attribute(
        __MODULE__,
        :fields,
        {unquote(name), unquote(Kvasir.Type.lookup(type)),
         unquote(opts)
         |> Keyword.put_new_lazy(:doc, fn ->
           case Module.delete_attribute(__MODULE__, :doc) do
             {_, doc} -> doc
             _ -> nil
           end
         end)}
      )
    end
  end

  defmacro version(version, updated \\ nil) do
    precision = version |> String.graphemes() |> Enum.count(&(&1 == "."))
    v = Version.parse!(version <> String.duplicate(".0", 2 - precision))

    quote do
      @version {unquote(Macro.escape(v)), unquote(updated),
                elem(Module.delete_attribute(__MODULE__, :doc) || {0, ""}, 1)}
    end
  end

  defmacro upgrade(version, do: block) do
    upgrades = Module.get_attribute(__CALLER__.module, :upgrades, [])
    func = :"__upgrade_#{Enum.count(upgrades)}"
    Module.put_attribute(__CALLER__.module, :upgrades, [{version, func} | upgrades])

    quote do
      defp unquote(func)(unquote(Macro.var(:event, __CALLER__.context))) do
        unquote(block)
      end
    end
  end

  defmacro event(type, do: block) do
    config = Mix.Project.config()
    app = Keyword.get(config, :app)
    version = Keyword.get(config, :version)
    docs = Keyword.get(config, :docs, [])
    package = Keyword.get(config, :package, [])
    dep_depth = config |> Keyword.get(:deps_path) |> Path.split() |> Enum.count()
    path = __CALLER__.file |> Path.split() |> Enum.slice((dep_depth + 1)..-1) |> Path.join()

    {hex, hexdocs} =
      case {Keyword.get(package, :name), Keyword.get(package, :organization)} do
        {nil, _} ->
          nil

        {app, nil} ->
          {"https://hex.pm/packages/#{app}/#{version}",
           "https://hexdocs.pm/#{app}/#{version}/#{inspect(__CALLER__.module)}.html"}

        {app, org} ->
          {"https://hex.pm/packages/#{org}/#{app}/#{version}",
           "https://#{org}.hexdocs.pm/#{app}/#{version}/#{inspect(__CALLER__.module)}.html"}
      end

    source =
      case {Keyword.get(docs, :source_url), Keyword.get(docs, :source_ref)} do
        {nil, _} -> nil
        {url, nil} -> url <> "/src/#{path}"
        {url, ref} -> url <> "/src/#{ref}/#{path}"
      end

    quote do
      Module.register_attribute(__MODULE__, :fields, accumulate: true)

      try do
        import Kvasir.Event, only: [field: 1, field: 2, field: 3]
        unquote(block)
      after
        :ok
      end

      @sensitive_fields @fields
                        |> Enum.filter(fn {_, _, o} -> o[:sensitive] end)
                        |> Enum.map(&elem(&1, 0))
      @event_fields Enum.reverse(@fields)
      defstruct Enum.map(@event_fields, fn {f, _, opts} -> {f, opts[:default]} end) ++
                  [__meta__: %Kvasir.Event.Meta{}]

      Module.register_attribute(__MODULE__, :__event__, persist: true)
      Module.put_attribute(__MODULE__, :__event__, unquote(Kvasir.Util.name(type)))

      @version_history Enum.sort(@version, &(Version.compare(elem(&1, 0), elem(&2, 0)) != :gt))
      @current_version @version_history
                       |> List.last()
                       |> elem(0)

      @doc false
      @spec __event__(atom) :: term
      def __event__(:type), do: unquote(Kvasir.Util.name(type))
      def __event__(:fields), do: @event_fields
      def __event__(:on_error), do: @on_error
      def __event__(:sensitive), do: @sensitive_fields
      def __event__(:doc), do: @moduledoc
      def __event__(:version), do: @current_version
      def __event__(:history), do: @version_history
      def __event__(:app), do: {unquote(app), unquote(version)}
      def __event__(:hex), do: unquote(hex)
      def __event__(:hexdocs), do: unquote(hexdocs)
      def __event__(:source), do: unquote(source)

      @field_type Map.new(@fields, fn {k, v, _} -> {k, v} end)
      @doc false
      @spec __event__(atom, atom) :: term
      def __event__(:type, field), do: @field_type[field]

      @doc ~S"""
      Create an event based on given fields.

      ## Examples

      ```elixir
      iex> create(field: :value)
      ```
      """
      @spec create(Keyword.t()) :: {:ok, Event.t()} | {:error, reason :: atom}
      def create(fields \\ []), do: Kvasir.Event.Encoding.create(__MODULE__, Map.new(fields))

      @doc ~S"""
      Create an event based on given fields.

      ## Examples

      ```elixir
      iex> create!(field: :value)
      ```
      """
      @spec create!(Keyword.t()) :: Event.t() | no_return
      def create!(fields \\ []) do
        case create(fields) do
          {:ok, event} -> event
          {:error, reason} -> raise("#{inspect(__MODULE__)}: Create failed #{inspect(reason)}")
        end
      end

      defoverridable create: 1

      defimpl Jason.Encoder, for: __MODULE__ do
        alias Jason.EncodeError
        alias Jason.Encoder.Map
        alias Kvasir.Event.Encoding

        def encode(value, opts) do
          case Encoding.encode(value) do
            {:ok, data} -> Map.encode(data, opts)
            {:error, error} -> %EncodeError{message: "Event Encoding Error: #{error}"}
          end
        end
      end

      defimpl Inspect, for: __MODULE__ do
        import Inspect.Algebra

        def inspect(data = %event{}, opts) do
          fields = event.__event__(:fields)

          a =
            data
            |> Map.drop(~w(__meta__ __struct__)a)
            |> Map.new(fn {k, v} ->
              if v != nil and k in event.__event__(:sensitive) do
                o = Enum.find_value(fields, [], fn {f, _, o} -> if(f == k, do: o) end)
                {k, %Kvasir.Event.Sensitive{opts: o, value: v, type: event.__event__(:type, k)}}
              else
                {k, v}
              end
            end)

          offset =
            if o = data.__meta__.offset do
              "#{data.__meta__.topic}:#{data.__meta__.partition}:#{o}"
            else
              "UNPUBLISHED"
            end

          concat([
            {:doc_color, :doc_nil, [:reset]},
            "⊰",
            inspect(data.__struct__),
            {:doc_color, :doc_nil, [:italic, :yellow]},
            "<",
            offset,
            ">",
            {:doc_color, :doc_nil, :reset},
            remove(to_doc(a, opts)),
            {:doc_color, :doc_nil, :reset},
            "⊱"
          ])
        end

        defp remove({a, "%{", c}), do: {a, "{", c}
        defp remove({a, b, c}), do: {a, remove(b), c}
        defp remove({a, b, c, d}), do: {a, remove(b), c, d}
      end
    end
  end

  defdelegate encode(event), to: Kvasir.Event.Encoding
  defdelegate encode(topic, event, opts \\ []), to: Kvasir.Event.Encoding
  defdelegate decode(topic, event, opts \\ []), to: Kvasir.Event.Encoding

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

  @doc ~S"""
  Set a key for an event.
  """
  @spec set_key(t, term) :: t
  def set_key(e = %{__meta__: meta}, key), do: %{e | __meta__: %{meta | key: key}}

  @doc ~S"""
  Set an offset for an event.
  """
  @spec set_offset(t, term) :: t
  def set_offset(e = %{__meta__: meta}, offset), do: %{e | __meta__: %{meta | offset: offset}}

  @doc ~S"""
  Set a topic for an event.
  """
  @spec set_topic(t, term) :: t
  def set_topic(e = %{__meta__: meta}, topic), do: %{e | __meta__: %{meta | topic: topic}}

  @doc ~S"""
  Set a partition for an event.
  """
  @spec set_partition(t, term) :: t
  def set_partition(e = %{__meta__: meta}, partition),
    do: %{e | __meta__: %{meta | partition: partition}}

  # @spec key(t) :: :string | :integer
  # def key_type(%event{}), do: event.__event__(:key_type)
  # def key_type(event) when is_atom(event), do: event.__event__(:key_type)
  # def key_type(_), do: :string

  @spec on_error(t) :: :halt | :skip
  def on_error(%__MODULE__{}), do: :halt
  def on_error(%event{}), do: event.__event__(:on_error)

  @spec type(t) :: String.t() | nil
  def type(%event{}), do: event.__event__(:type)
  def type(event) when is_atom(event), do: event.__event__(:type)
  def type(_), do: nil

  ### Guard ###

  defp guard(version) do
    version
    |> Version.Parser.lexer([])
    |> to_guard(nil)
  end

  @major Macro.var(:major, nil)
  @minor Macro.var(:minor, nil)
  @patch Macro.var(:patch, nil)

  defp to_guard([], acc), do: acc
  defp to_guard([:|| | rest], acc), do: guard_or(acc, to_guard(rest, nil))
  defp to_guard([:&& | rest], acc), do: guard_and(acc, to_guard(rest, nil))

  defp to_guard([a, b | rest], _acc), do: to_guard(rest, condition(a, b))

  defp condition(:==, version) do
    version
    |> parse_parts()
    |> Enum.reduce(nil, fn {k, v}, acc ->
      guard_and(acc, quote(do: unquote(Macro.var(k, nil)) == unquote(v)))
    end)
  end

  defp condition(:~>, version) do
    case parse_version(version) do
      [_] -> condition(:>=, version)
      [a, _] -> guard_and(condition(:>=, version), condition(:<, "#{a + 1}.0"))
      [a, b, _] -> guard_and(condition(:>=, version), condition(:<, "#{a}.#{b + 1}.0"))
    end
  end

  defp condition(:>, version) do
    case parse_version(version) do
      [a] ->
        quote do: unquote() > unquote(a)

      [a, b] ->
        quote do:
                unquote(@major) > unquote(a) or
                  (unquote(@major) == unquote(a) and unquote(@minor) > unquote(b))

      [a, b, c] ->
        quote do:
                unquote(@major) > unquote(a) or
                  (unquote(@major) == unquote(a) and unquote(@minor) > unquote(b)) or
                  (unquote(@major) == unquote(a) and unquote(@minor) == unquote(b) and
                     unquote(@patch) > unquote(c))
    end
  end

  defp condition(:>=, version) do
    case parse_version(version) do
      [a] ->
        quote do: unquote(@major) >= unquote(a)

      [a, b] ->
        quote do:
                unquote(@major) > unquote(a) or
                  (unquote(@major) == unquote(a) and unquote(@minor) >= unquote(b))

      [a, b, c] ->
        quote do:
                unquote(@major) > unquote(a) or
                  (unquote(@major) == unquote(a) and unquote(@minor) > unquote(b)) or
                  (unquote(@major) == unquote(a) and unquote(@minor) == unquote(b) and
                     unquote(@patch) >= unquote(c))
    end
  end

  defp condition(:<, version) do
    case parse_version(version) do
      [a] ->
        quote do: unquote(@major) < unquote(a)

      [a, b] ->
        quote do:
                unquote(@major) < unquote(a) or
                  (unquote(@major) == unquote(a) and unquote(@minor) < unquote(b))

      [a, b, c] ->
        quote do:
                unquote(@major) < unquote(a) or
                  (unquote(@major) == unquote(a) and unquote(@minor) < unquote(b)) or
                  (unquote(@major) == unquote(a) and unquote(@minor) == unquote(b) and
                     unquote(@patch) < unquote(c))
    end
  end

  defp condition(:<=, version) do
    case parse_version(version) do
      [a] ->
        quote do: unquote(@major) <= unquote(a)

      [a, b] ->
        quote do:
                unquote(@major) < unquote(a) or
                  (unquote(@major) == unquote(a) and unquote(@minor) <= unquote(b))

      [a, b, c] ->
        quote do:
                unquote(@major) < unquote(a) or
                  (unquote(@major) == unquote(a) and unquote(@minor) < unquote(b)) or
                  (unquote(@major) == unquote(a) and unquote(@minor) == unquote(b) and
                     unquote(@patch) <= unquote(c))
    end
  end

  defp guard_and(a, nil), do: a
  defp guard_and(nil, b), do: b

  defp guard_and(a, b) do
    quote do: unquote(a) and unquote(b)
  end

  defp guard_or(a, nil), do: a
  defp guard_or(nil, b), do: b

  defp guard_or(a, b) do
    quote do: unquote(a) or unquote(b)
  end

  defp parse_version(version) do
    case String.split(version, ".") do
      [] -> :error
      [a] -> [String.to_integer(a)]
      [a, b] -> [String.to_integer(a), String.to_integer(b)]
      [a, b, c | _] -> [String.to_integer(a), String.to_integer(b), String.to_integer(c)]
    end
  end

  defp parse_parts(version) do
    case parse_version(version) do
      [] -> :error
      [a] -> [major: a]
      [a, b] -> [major: a, minor: b]
      [a, b, c | _] -> [major: a, minor: b, patch: c]
    end
  end
end
