defmodule Kvasir.Key do
  @moduledoc ~S"""

  """

  @doc ~S"""
  Key documentation.
  """
  @callback doc :: String.t()

  @doc ~S"""
  Key descriptive name.
  """
  @callback name :: String.t()
  @callback parse(value :: any, opts :: Keyword.t()) :: {:ok, term} | {:error, atom}
  @callback dump(value :: term, opts :: Keyword.t()) :: {:ok, term} | {:error, atom}
  @callback partition(value :: term, partitions :: pos_integer) ::
              {:ok, non_neg_integer} | {:error, atom}
  @callback obfuscate(value :: term, opts :: Keyword.t()) ::
              {:ok, term} | :obfuscate | {:error, atom}

  defmacro __using__(opts \\ []) do
    {app, version, hex, hexdocs, source} = Kvasir.Util.documentation(__CALLER__)

    name =
      if n = opts[:name],
        do: n,
        else: __CALLER__.module |> Module.split() |> List.last() |> Macro.underscore()

    base =
      if t = opts[:type] do
        quote do
          require unquote(t)

          @spec parse(value :: any, opts :: Keyword.t()) :: {:ok, term} | {:error, atom}
          @impl unquote(__MODULE__)
          def parse(value, opts), do: unquote(t).parse(value, opts)

          @spec dump(value :: term, opts :: Keyword.t()) :: {:ok, term} | {:error, atom}
          @impl unquote(__MODULE__)
          def dump(value, opts), do: unquote(t).dump(value, opts)

          @spec obfuscate(value :: term, opts :: Keyword.t()) ::
                  {:ok, term} | :obfuscate | {:error, atom}
          @impl unquote(__MODULE__)
          def obfuscate(value, opts), do: unquote(t).obfuscate(value, opts)
        end
      else
        quote do
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
        end
      end

    quote do
      @behaviour unquote(__MODULE__)

      @spec name :: String.t()
      @impl unquote(__MODULE__)
      def name, do: unquote(name)

      @shared_doc @moduledoc || ""
      @spec doc :: String.t()
      @impl unquote(__MODULE__)
      def doc, do: @shared_doc

      @doc false
      @spec __key__(atom) :: term
      def __key__(:name), do: unquote(name)
      def __key__(:app), do: {unquote(app), unquote(version)}
      def __key__(:doc), do: @shared_doc
      def __key__(:hex), do: unquote(hex)
      def __key__(:hexdocs), do: unquote(hexdocs)
      def __key__(:source), do: unquote(source)

      unquote(base)
      defoverridable parse: 2, dump: 2, obfuscate: 2
    end
  end
end
