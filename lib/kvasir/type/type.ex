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
    name =
      if n = opts[:name],
        do: n,
        else: __CALLER__.module |> Module.split() |> List.last() |> Macro.underscore()

    quote location: :keep do
      @behaviour unquote(__MODULE__)

      @impl unquote(__MODULE__)
      def name, do: unquote(name)

      @shared_doc @moduledoc || ""
      @impl unquote(__MODULE__)
      def doc, do: @shared_doc

      @impl unquote(__MODULE__)
      def parse(value, _opts), do: {:ok, value}

      @impl unquote(__MODULE__)
      def dump(value, _opts), do: {:ok, value}

      @impl unquote(__MODULE__)
      def obfuscate(_value, _opts), do: :obfuscate

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
    pos_integer: __MODULE__.PosInteger,
    non_neg_integer: __MODULE__.NonNegInteger,
    map: __MODULE__.Map,
    string: __MODULE__.String,
    timestamp: __MODULE__.Timestamp
  }

  @doc ~S"""
  Lookup a base type.
  """
  @spec lookup(atom) :: atom
  def lookup(type), do: @base_types[type] || type
end
