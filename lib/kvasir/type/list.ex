defmodule Kvasir.Type.List do
  @moduledoc ~S"""

  """
  use Kvasir.Type

  @impl Kvasir.Type
  def parse(data, opts \\ [])
  def parse(data, opts) when is_list(data), do: EnumX.map(data, parser(opts[:type]))
  def parse(_, _opts), do: {:error, :invalid_list}

  @impl Kvasir.Type
  def dump(data, opts \\ []), do: EnumX.map(data, dumper(opts[:type]))

  @impl Kvasir.Type
  def obfuscate(data, opts), do: EnumX.map(data, obfuscator(opts[:type]))

  ### Helpers ###

  defp obfuscator(nil), do: &Kvasir.Util.identity/1

  defp obfuscator({type, opts}) do
    t = Kvasir.Type.lookup(type)
    &t.obfuscate(&1, opts)
  end

  defp obfuscator(type) do
    t = Kvasir.Type.lookup(type)
    &t.obfuscate(&1, [])
  end

  defp parser(nil), do: &Kvasir.Util.identity/1

  defp parser({type, opts}) do
    t = Kvasir.Type.lookup(type)
    &t.parse(&1, opts)
  end

  defp parser(type) do
    t = Kvasir.Type.lookup(type)
    &t.parse(&1, [])
  end

  defp dumper(nil), do: &Kvasir.Util.identity/1

  defp dumper({type, opts}) do
    t = Kvasir.Type.lookup(type)
    &t.dump(&1, opts)
  end

  defp dumper(type) do
    t = Kvasir.Type.lookup(type)
    &t.dump(&1, [])
  end
end
