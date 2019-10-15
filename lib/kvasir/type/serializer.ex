defmodule Kvasir.Type.Serializer do
  @type field :: {name :: atom, type :: module, opts :: Keyword.t()}

  @spec encode([field], map, Keyword.t()) ::
          {:ok, %{required(atom) => term}} | {:error, reason :: atom}
  def encode(fields, data, opts \\ []), do: do_encode(fields, data, opts[:into] || %{})

  @spec decode([field], map, Keyword.t()) ::
          {:ok, %{required(atom) => term}} | {:error, reason :: atom}
  def decode(fields, data, opts \\ []), do: do_decode(fields, data, opts[:into] || %{})

  ### Helpers ###

  @spec do_encode([field], map, map) :: {:ok, map} | {:error, atom}
  defp do_encode([], _data, acc), do: {:ok, acc}

  defp do_encode([{field, type, opts} | fields], data, acc) do
    with {:ok, value} when value != nil <- Map.fetch(data, field),
         {:ok, encoded} <- type.dump(value, opts) do
      do_encode(fields, data, Map.put(acc, field, encoded))
    else
      {:ok, nil} ->
        do_encode(fields, data, acc)

      :error ->
        if opts[:default] || opts[:optional],
          do: do_encode(fields, data, acc),
          else: {:error, :"missing_#{field}_field"}

      error = {:error, _} ->
        error
    end
  end

  @spec do_decode([field], map, map) :: {:ok, map} | {:error, atom}
  defp do_decode([], _data, acc), do: {:ok, acc}

  defp do_decode([{field, type, opts} | fields], data, acc) do
    with {:ok, value} <- MapX.fetch(data, field),
         {:ok, parsed_value} <- type.parse(value, opts) do
      do_decode(fields, data, Map.put(acc, field, parsed_value))
    else
      :error ->
        cond do
          default = opts[:default] -> do_decode(fields, data, Map.put(acc, field, default))
          opts[:optional] -> do_decode(fields, data, acc)
          :missing -> {:error, :"missing_#{field}_field"}
        end

      error = {:error, _} ->
        error
    end
  end
end
