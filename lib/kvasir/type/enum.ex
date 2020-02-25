defmodule Kvasir.Type.Enum do
  defmacro __using__(_opts \\ []) do
    quote do
      @before_compile unquote(__MODULE__)
      import unquote(__MODULE__), only: [option: 1, option: 2]
      Module.register_attribute(__MODULE__, :options, accumulate: true)
    end
  end

  defmacro option(value, opts \\ []) do
    quote do
      Module.put_attribute(
        __MODULE__,
        :options,
        {unquote(value),
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

  defmacro __before_compile__(env) do
    {parse, dump, all} =
      env.module
      |> Module.get_attribute(:options)
      |> Enum.reduce({nil, nil, []}, fn {value, opts}, {parse, dump, acc} ->
        encoded = Keyword.get_lazy(opts, :encoded, fn -> to_string(value) end)

        {quote do
           unquote(parse)
           def parse(unquote(value), _opts), do: {:ok, unquote(value)}
           def parse(unquote(encoded), _opts), do: {:ok, unquote(value)}
         end,
         quote do
           unquote(dump)
           def dump(unquote(value), _opts), do: {:ok, unquote(encoded)}
         end, [{value, encoded, Keyword.take(opts, ~w(doc)a)} | acc]}
      end)

    quote do
      import unquote(__MODULE__), only: []
      use Kvasir.Type

      @impl Kvasir.Type
      def parse(value, opts \\ [])
      unquote(parse)
      def parse(_, _), do: {:error, :invalid_enum_value}

      @impl Kvasir.Type
      def dump(value, opts \\ [])
      unquote(dump)

      @impl Kvasir.Type
      def describe(value), do: to_string(value)

      @doc """
      List all possible values for the given enum.

      ## Example

      ```elixir
      iex> values()
      #{unquote(inspect(Enum.map(all, &elem(&1, 0))))}
      ```
      """
      @spec values :: [atom]
      def values, do: unquote(Enum.map(all, &elem(&1, 0)))

      @doc false
      @spec __enum__ :: [{term, term, Keyword.t()}]
      def __enum__, do: unquote(Macro.escape(all))
    end
  end
end
