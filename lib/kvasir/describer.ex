defmodule Kvasir.Describer do
  defmacro event(type) do
    module = __CALLER__.module

    if description = Module.get_attribute(module, :describe) do
      {setup, vars} = gather(description)

      if vars == [] do
        quote do
          def describe(unquote({:key, [], nil}), _event) do
            <<unquote_splicing(setup)>>
          end
        end
      else
        describe =
          Enum.reduce(vars, nil, fn v, acc ->
            t = Module.get_attribute(module, :"field_#{v}")

            quote do
              unquote(acc)

              unquote(Macro.var(v, nil)) =
                unquote(t).describe(unquote({{:., [], [{:event, [], nil}, v]}, [], []}))
            end
          end)

        quote do
          def describe(unquote({:key, [], nil}), unquote({:event, [], nil})) do
            unquote(describe)
            <<unquote_splicing(setup)>>
          end
        end
      end
    else
      quote do
        def describe(key, _event),
          do: "#{key} #{unquote(type |> Kvasir.Util.name() |> String.replace(~r/\.|\_/, " "))}."
      end
    end
  end

  defp gather(text, description \\ [], vars \\ [])

  defp gather(text, description, vars) do
    case :binary.split(text, "$") do
      [t] ->
        {:lists.reverse([t | description]), :lists.reverse(vars)}

      [h, t] ->
        d = [h | description]

        case Regex.run(~r/^(\?|[a-z_]+)(.*)$/, t) do
          nil ->
            gather(t, d, vars)

          [_, "?", rest] ->
            gather(rest, [binary_var({:key, [], nil}) | d], vars)

          [_, var, rest] ->
            v = String.to_atom(var)

            gather(rest, [binary_var({v, [], nil}) | d], [v | vars])
        end
    end
  end

  defp binary_var(var), do: {:"::", [], [var, {:binary, [], Elixir}]}
end
