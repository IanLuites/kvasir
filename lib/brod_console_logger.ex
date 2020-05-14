defmodule BrodConsoleLogger do
  @moduledoc false

  @behaviour :gen_event
  alias Logger.Backends.Console

  defdelegate handle_call(a, b), to: Console
  defdelegate handle_info(a, b), to: Console
  defdelegate code_change(a, b, c), to: Console
  defdelegate terminate(a, b), to: Console

  @spec init(any) ::
          {:error, :ignore}
          | {:ok,
             %{
               colors: %{debug: any, enabled: any, error: any, info: any, warn: any},
               device: any,
               format:
                 [:date | :level | :levelpad | :message | :metadata | :node | :time | binary]
                 | {atom, atom},
               level: any,
               max_buffer: any,
               metadata: :all | [any]
             }}
  @doc false
  def init(_), do: Console.init(:console)

  @doc false
  @spec handle_event(any, any) :: {:ok, any}
  def handle_event(
        e = {:info, _, {Logger, m, _, _}},
        state
      ) do
    # Ignore
    if match?(
         [
           83,
           116,
           97,
           114,
           116,
           105,
           110,
           103,
           32,
           103,
           114,
           111,
           117,
           112,
           95,
           115,
           117,
           98,
           115,
           99,
           114,
           105,
           98,
           101,
           114 | _
         ],
         m
       ) or
         match?("[supervisor: {#PID<" <> _, m) do
      {:ok, state}
    else
      Console.handle_event(e, state)
    end
  end

  def handle_event(e, state), do: Console.handle_event(e, state)
end
