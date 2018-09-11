defmodule Kvasir.Consumer do
  @callback consume_init(any) :: {:ok, any}
  @callback handle_event(term, any) :: {:ok, any}

  defmacro __using__(opts \\ []) do
    topic = opts[:topic]
    partition = opts[:partition] || :all

    config =
      cond do
        otp = opts[:otp_app] ->
          quote do
            config = Application.get_env(unquote(otp), __MODULE__, [])
          end

        client = opts[:client] ->
          quote do
            config = unquote(client).config()
          end

        :no_config ->
          raise "Need to either set `client` or `otp_app` + config."
      end

    quote do
      @behaviour :brod_topic_subscriber
      @behaviour Kvasir.Consumer

      def start_link(opts \\ []) do
        from = opts[:from] || :earliest

        consumerConfig = [
          begin_offset: from,
          offset_reset_policy: :reset_to_earliest
        ]

        unquote(config)
        kafka = config[:kafka]
        events = Kvasir.Event.Discovery.discover(config[:events])

        :brod.start_client(kafka, __MODULE__)

        :brod_topic_subscriber.start_link(
          __MODULE__,
          unquote(topic),
          unquote(partition),
          consumerConfig,
          __MODULE__,
          %{events: events, state: opts[:state] || nil}
        )
      end

      def child_spec(opts \\ []) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [opts]}
        }
      end

      @impl :brod_topic_subscriber
      def init(_topic, state) do
        with {:ok, updated_state} <- consume_init(state.state) do
          {:ok, [], %{state | state: updated_state}}
        end
      end

      @impl :brod_topic_subscriber
      def handle_message(_partition, message, state) do
        with event <- Kvasir.Event.Decoder.decode(message, state.events),
             {:ok, updated_state} <- handle_event(event, state.state) do
          {:ok, :ack, %{state | state: updated_state}}
        end
      end

      @impl Kvasir.Consumer
      def consume_init(state), do: {:ok, state}

      defoverridable consume_init: 1
    end
  end
end
