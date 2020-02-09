defmodule Kvasir.Encryption do
  @type setting :: :always | :sensitive_only | false
  @type settings :: module | {module, Keyword.t()}
end
