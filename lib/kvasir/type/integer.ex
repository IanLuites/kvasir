defmodule Kvasir.Type.Integer do
  @moduledoc ~S"""

  """
  use Kvasir.Type

  @impl Kvasir.Type
  def parse(number, opts \\ [])
  def parse(number, _opts) when is_integer(number), do: {:ok, number}

  def parse(number, opts) when is_binary(number) do
    case Integer.parse(number) do
      {number, ""} -> parse(number, opts)
      _ -> {:error, :invalid_integer}
    end
  end

  def parse(_, _opts), do: {:error, :invalid_integer}
end

defmodule Kvasir.Type.PosInteger do
  @moduledoc ~S"""

  """
  use Kvasir.Type

  @impl Kvasir.Type
  def parse(number, opts \\ [])
  def parse(number, _opts) when is_integer(number) and number > 0, do: {:ok, number}

  def parse(number, opts) when is_binary(number) do
    case Integer.parse(number) do
      {number, ""} -> parse(number, opts)
      _ -> {:error, :invalid_integer}
    end
  end

  def parse(_, _opts), do: {:error, :invalid_integer}
end

defmodule Kvasir.Type.NonNegativeInteger do
  @moduledoc ~S"""

  """
  use Kvasir.Type

  @impl Kvasir.Type
  def parse(number, opts \\ [])
  def parse(number, _opts) when is_integer(number) and number >= 0, do: {:ok, number}

  def parse(number, opts) when is_binary(number) do
    case Integer.parse(number) do
      {number, ""} -> parse(number, opts)
      _ -> {:error, :invalid_integer}
    end
  end

  def parse(_, _opts), do: {:error, :invalid_integer}
end
