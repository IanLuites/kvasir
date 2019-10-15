defmodule Kvasir do
  @type topic :: String.t()
  @type partition :: non_neg_integer
  @type offset :: Kvasir.Offset.t()
end
