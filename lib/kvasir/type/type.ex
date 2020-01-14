defmodule Kvasir.Type do
  @moduledoc ~S"""
  """

  @typedoc ~S""
  @type t :: atom

  @typedoc ~S""
  @type base :: atom | boolean | integer | String.t() | [base] | %{optional(base) => base}

  @doc ~S"""
  Type documentation.
  """
  @callback doc :: String.t()

  @doc ~S"""
  Type descriptive name.
  """
  @callback name :: String.t()
  @callback parse(value :: any, opts :: Keyword.t()) :: {:ok, term} | {:error, atom}
  @callback dump(value :: term, opts :: Keyword.t()) :: {:ok, term} | {:error, atom}
  @callback obfuscate(value :: term, opts :: Keyword.t()) ::
              {:ok, term} | :obfuscate | {:error, atom}

  defmacro __using__(opts \\ []) do
    {app, version, hex, hexdocs, source} = Kvasir.Util.documentation(__CALLER__)

    name =
      if n = opts[:name],
        do: n,
        else: __CALLER__.module |> Module.split() |> List.last() |> Macro.underscore()

    quote location: :keep do
      @behaviour unquote(__MODULE__)

      @spec name :: String.t()
      @impl unquote(__MODULE__)
      def name, do: unquote(name)

      @shared_doc @moduledoc || ""
      @spec doc :: String.t()
      @impl unquote(__MODULE__)
      def doc, do: @shared_doc

      @spec parse(value :: any, opts :: Keyword.t()) :: {:ok, term} | {:error, atom}
      @impl unquote(__MODULE__)
      def parse(value, _opts), do: {:ok, value}

      @spec dump(value :: term, opts :: Keyword.t()) :: {:ok, term} | {:error, atom}
      @impl unquote(__MODULE__)
      def dump(value, _opts), do: {:ok, value}

      @spec obfuscate(value :: term, opts :: Keyword.t()) ::
              {:ok, term} | :obfuscate | {:error, atom}
      @impl unquote(__MODULE__)
      def obfuscate(_value, _opts), do: :obfuscate

      @doc false
      @spec __type__(atom) :: term
      def __type__(:name), do: unquote(name)
      def __type__(:app), do: {unquote(app), unquote(version)}
      def __type__(:doc), do: @shared_doc
      def __type__(:hex), do: unquote(hex)
      def __type__(:hexdocs), do: unquote(hexdocs)
      def __type__(:source), do: unquote(source)

      defoverridable parse: 2, dump: 2, obfuscate: 2
    end
  end

  @base_types %{
    any: __MODULE__.Any,
    atom: __MODULE__.Atom,
    boolean: __MODULE__.Boolean,
    date: __MODULE__.Date,
    email: __MODULE__.Email,
    float: __MODULE__.Float,
    integer: __MODULE__.Integer,
    ip: __MODULE__.IP,
    list: __MODULE__.List,
    map: __MODULE__.Map,
    non_neg_integer: __MODULE__.NonNegInteger,
    pos_integer: __MODULE__.PosInteger,
    string: __MODULE__.String,
    timestamp: __MODULE__.Timestamp,
    uri: __MODULE__.URI
  }

  @doc ~S"""
  Lookup a base type.
  """
  @spec lookup(atom) :: atom
  def lookup(type), do: @base_types[type] || type
end
