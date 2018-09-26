defmodule Kvasir.Util do
  @moduledoc false

  @doc false
  @spec name(any) :: String.t()
  def name(name) when is_binary(name), do: name
  def name(name) when is_atom(name), do: to_string(name)
  def name({{:., _, names}, _, _}), do: names |> Enum.map(&name/1) |> Enum.join(".")
  def name({name, _, nil}) when is_atom(name), do: to_string(name)

  @doc false
  @spec keys_to_atoms(map) :: map
  def keys_to_atoms(map) when is_map(map),
    do: for({k, v} <- map, into: %{}, do: {String.to_atom(k), keys_to_atoms(v)})

  def keys_to_atoms(v), do: v

  @doc false
  @spec keys_to_atoms!(map) :: map | no_return
  def keys_to_atoms!(map) when is_map(map),
    do: for({k, v} <- map, into: %{}, do: {String.to_existing_atom(k), keys_to_atoms(v)})

  def keys_to_atoms!(v), do: v

  @doc false
  @spec identity(any) :: any
  def identity(value), do: {:ok, value}
end
