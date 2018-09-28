defmodule Kvasir.Test do
  alias Kvasir.Event.Encodings.Raw
  @unix ~N[1970-01-01 00:00:00]

  def event(event, payload, opts \\ []) do
    Raw.decode(
      %{
        payload: payload,
        meta: %{
          topic: opts[:topic] || "test",
          partition: opts[:partition] || 0,
          offset: opts[:offset] || %{0 => 1},
          key: opts[:instance],
          ts_type: "woef",
          ts:
            NaiveDateTime.diff(opts[:timestamp] || NaiveDateTime.utc_now(), @unix, :millisecond),
          headers: [],
          command: opts[:command]
        },
        type: event.__event__(:type)
      },
      opts
    )
  end
end
