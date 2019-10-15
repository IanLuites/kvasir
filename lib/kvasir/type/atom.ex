defmodule Kvasir.Type.Atom do
  @moduledoc ~S"""

  """
  use Kvasir.Type

  @impl Kvasir.Type
  def parse(atom, opts \\ [])
  def parse(atom, _opts) when is_atom(atom), do: {:ok, atom}

  def parse(atom, opts) when is_binary(atom) do
    if opts[:strict] do
      {:ok, String.to_existing_atom(atom)}
    else
      {:ok, String.to_atom(atom)}
    end
  rescue
    _ -> {:error, :unknown_atom}
  end

  def parse(_value, _opts), do: {:error, :invalid_atom}

  @impl Kvasir.Type
  def dump(atom, _opts \\ []), do: {:ok, to_string(atom)}
end
