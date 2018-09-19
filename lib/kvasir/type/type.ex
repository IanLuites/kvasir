defmodule Kvasir.Type do
  @callback load(value :: any, opts :: Keyword.t()) :: {:ok, term} | {:error, atom}
  @callback save(value :: term, opts :: Keyword.t()) :: {:ok, term} | {:error, atom}

  @type t :: atom

  @base ~w(
    any
    array
    binary
    boolean
    date
    datetime
    float
    integer
    map
    naive_datetime
    string
    time
    utc_datetime
  )a
  @composite ~w(array map)a
  @unix ~N[1970-01-01 00:00:00]

  @doc ~S"""
  Load a value with a specific type.
  """
  @spec load(atom, any, Keyword.t()) :: {:ok, term} | {:error, atom}
  def load(type, value, opts \\ [])

  def load(type, value, opts) when type in @base, do: base_load(type, value, opts)
  def load(t = {type, _}, value, opts) when type in @composite, do: composite_load(t, value, opts)

  def load(type, value, opts),
    do: if(Code.ensure_loaded?(type), do: type.load(value, opts), else: {:error, :unknown, type})

  def dump(_type, value, _opts) do
    {:ok, value}
  end

  ### Util for outside ###

  @doc false
  @spec do_encode(map, list, map) :: {:ok, map} | {:error, atom}
  def do_encode(_data, [], acc), do: {:ok, acc}

  def do_encode(data, [{field, type, opts} | fields], acc) do
    with {:ok, value} <- Map.fetch(data, field),
         {:ok, encoded} <- dump(type, value, opts) do
      do_encode(data, fields, Map.put(acc, field, encoded))
    else
      :error ->
        if opts[:optional], do: do_encode(data, fields, acc), else: {:error, :missing_field}

      error = {:error, _} ->
        error
    end
  end

  ### Base Implementations

  @spec base_load(atom, any, Keyword.t()) :: {:ok, term} | {:error, atom}
  defp base_load(:map, value, opts) when is_binary(value),
    do: with({:ok, data} <- Jason.decode(value, opts), do: base_load(:map, data, opts))

  defp base_load(:map, value, opts) when is_map(value) do
    case opts[:keys] do
      nil -> {:ok, value}
      :atoms -> {:ok, Kvasir.Util.keys_to_atoms(value)}
      :atoms! -> {:ok, Kvasir.Util.keys_to_atoms!(value)}
    end
  end

  defp base_load(type, value, opts)
       when type in ~w(any binary boolean float integer string map array)a do
    if match_base_type?(type, value) or (opts[:optional] && is_nil(value)),
      do: {:ok, value},
      else: {:error, :value_has_invalid_type}
  end

  defp base_load(type, value, opts) when type in ~w(datetime naive_datetime)a do
    cond do
      is_binary(value) -> NaiveDateTime.from_iso8601(value)
      is_integer(value) -> {:ok, NaiveDateTime.add(@unix, value, opts[:unit] || :millisecond)}
      :invalid_format -> {:error, :invalid_date_format}
    end
  end

  defp base_load(:utc_datetime, value, opts) do
    cond do
      is_binary(value) -> DateTime.from_iso8601(value)
      is_integer(value) -> DateTime.from_unix(value, opts[:unit] || :millisecond)
      :invalid_format -> {:error, :invalid_date_format}
    end
  end

  defp base_load(:date, value, _opts), do: Date.from_iso8601(value)
  defp base_load(:time, value, _opts), do: Time.from_iso8601(value)

  @spec composite_load({atom, t}, any, Keyword.t()) :: {:ok, term} | {:error, atom}
  defp composite_load(t = {:map, _}, value, opts) when is_binary(value) do
    with {:ok, decoded} <- Jason.decode(value, opts), do: composite_load(t, decoded, opts)
  end

  defp composite_load({:map, type}, value, opts) when is_list(value) or is_map(value) do
    Enum.reduce_while(value, {:ok, %{}}, fn {k, v}, {:ok, acc} ->
      case load(type, v, opts) do
        {:ok, parsed_value} -> {:cont, {:ok, Map.put(acc, k, parsed_value)}}
        error -> {:halt, error}
      end
    end)
  end

  defp composite_load({:array, type}, value, opts) when is_list(value),
    do: Enum.map(value, &load(type, &1, opts))

  defp composite_load(_, _, _), do: {:error, :invalid_composite_data}

  ### Helpers ###

  @spec match_base_type?(t, any) :: boolean
  defp match_base_type?(:any, _), do: true
  defp match_base_type?(:array, value), do: is_list(value)
  defp match_base_type?(:binary, value), do: is_binary(value)
  defp match_base_type?(:boolean, value), do: is_boolean(value)
  defp match_base_type?(:float, value), do: is_float(value)
  defp match_base_type?(:integer, value), do: is_integer(value)
  defp match_base_type?(:map, value), do: is_map(value)
  defp match_base_type?(:string, value), do: is_binary(value)
end
