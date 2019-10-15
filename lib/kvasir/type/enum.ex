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
      @options {unquote(value), unquote(opts)}
    end
  end

  defmacro __before_compile__(env) do
    {parse, dump} =
      env.module
      |> Module.get_attribute(:options)
      |> Enum.reduce({nil, nil}, fn {value, opts}, {parse, dump} ->
        encoded = Keyword.get_lazy(opts, :encoded, fn -> to_string(value) end)

        {quote do
           unquote(parse)
           def parse(unquote(value), _opts), do: {:ok, unquote(value)}
           def parse(unquote(encoded), _opts), do: {:ok, unquote(value)}
         end,
         quote do
           unquote(dump)
           def dump(unquote(value), _opts), do: {:ok, unquote(encoded)}
         end}
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
    end
  end
end
