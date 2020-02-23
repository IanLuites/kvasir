defmodule Kvasir.InvalidKey do
  @doc ~S"""
  Invalid key has been given.
  """
  defexception [:value, :key, :reason, message: "Invalid key."]

  def message(%{value: value, key: key, reason: reason}) do
    """
    Invalid #{inspect(key)} key.

    Could not parse #{inspect(value)}, because #{inspect(reason)}.
    """
  end
end

defmodule Kvasir.InvalidType do
  @doc ~S"""
  Invalid type has been given.
  """

  defexception [:value, :type, :reason, message: "Invalid type."]

  def message(%{value: value, type: type, reason: reason}) do
    """
    Invalid #{inspect(type)} type.

    Could not parse #{inspect(value)}, because #{inspect(reason)}.
    """
  end
end
