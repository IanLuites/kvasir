defmodule Kvasir.Event.Encoding.Topic do
  @spec create(Kvasir.Topic.t(), term) :: term
  def generate(topic, extra \\ nil) do
    {_mod, code} = generate_module(topic, extra: extra, overwrite: true, events: :all)
    code
  end

  @spec create(Kvasir.Topic.t(), Keyword.t()) :: module
  def create(topic, opts \\ []) do
    {mod, code} = generate_module(topic, opts)

    if code do
      Code.compiler_options(ignore_module_conflict: true)
      Code.compile_quoted(code)
      Code.compiler_options(ignore_module_conflict: false)
    end

    mod
  end

  @spec generate_module(Kvasir.Topic.t(), Keyword.t()) :: {module, term}
  defp generate_module(topic, opts) do
    {events, mod} =
      case opts[:only] do
        all when all in [nil, :all] ->
          {topic.events, topic.module}

        e ->
          es = if(is_list(e), do: e, else: [e])
          types = es |> Enum.map(& &1.__event__(:type)) |> Enum.sort()
          hash = :md5 |> :crypto.hash(types) |> Base.encode16()

          {es, Module.concat(topic.module, "F" <> hash)}
      end

    if not Keyword.get(opts, :overwrite, false) and Code.ensure_loaded?(mod) do
      {mod, nil}
    else
      key = topic.key
      decoder = Kvasir.Event.Encoding
      {bin_decode, event} = gen_event_matchers(key, events)

      {encrypter, encrypter_opts} = topic.encryption_opts
      {compressor, compressor_opts} = topic.compression_opts

      {encryption, decryption} =
        case topic.encryption do
          false ->
            {quote do
               defp encrypt(_, data), do: {:ok, data}
             end,
             quote do
               defp decrypt(_, data), do: {:ok, data}
             end}

          :always ->
            {quote do
               defp encrypt(_, data),
                 do: unquote(encrypter).encrypt(data, unquote(encrypter_opts))
             end,
             quote do
               defp decrypt(_, data),
                 do: unquote(encrypter).decrypt(data, unquote(encrypter_opts))
             end}

          :sensitive_only ->
            # For now always
            Enum.reduce(
              topic.events,
              {quote do
                 defp encrypt(_, data), do: {:ok, data}
               end,
               quote do
                 defp decrypt(_, data), do: {:ok, data}
               end},
              fn e, acc = {a, b} ->
                if e.__event__(:sensitive) != [] do
                  {quote do
                     defp encrypt(unquote(e), data),
                       do: unquote(encrypter).encrypt(data, unquote(encrypter_opts))

                     unquote(a)
                   end,
                   quote do
                     defp decrypt(unquote(e), data),
                       do: unquote(encrypter).decrypt(data, unquote(encrypter_opts))

                     unquote(b)
                   end}
                else
                  acc
                end
              end
            )
        end

      {compression, decompression} =
        case topic.compression do
          false ->
            {quote do
               defp compress(_, data), do: {:ok, data}
             end,
             quote do
               defp decompress(_, data), do: {:ok, data}
             end}

          :always ->
            {quote do
               defp compress(_, data),
                 do: unquote(compressor).compress(data, unquote(compressor_opts))
             end,
             quote do
               defp decompress(_, data),
                 do: unquote(compressor).decompress(data, unquote(compressor_opts))
             end}

          :event ->
            # For now always
            Enum.reduce(
              topic.events,
              {quote do
                 defp compress(_, data), do: {:ok, data}
               end,
               quote do
                 defp decompress(_, data), do: {:ok, data}
               end},
              fn e, {a, b} ->
                if e.__event__(:compress) do
                  {quote do
                     defp compress(unquote(e), data),
                       do: unquote(compressor).compress(data, unquote(compressor_opts))

                     unquote(a)
                   end,
                   quote do
                     defp decompress(unquote(e), data),
                       do: unquote(compressor).decompress(data, unquote(compressor_opts))

                     unquote(b)
                   end}
                else
                  {a, b}
                end
              end
            )
        end

      {mod,
       quote do
         defmodule unquote(mod) do
           @moduledoc """
           Event encoding and decoding for the `"#{unquote(topic.topic)}"` topic.
           """
           import unquote(decoder), only: [encode: 3, binary_decode: 4, decode: 4]

           require unquote(compressor)
           require unquote(encrypter)

           ### Encoders ###

           @doc ~S"""
           Encode an event to JSON.

           ## Examples

           ```elixir
           iex> encode(⊰Event{...}⊱)
           {:ok, "{...}"}
           ```
           """
           @spec encode(Kvasir.Event.t()) :: {:ok, binary} | {:error, term}
           def encode(event)

           def encode(event = %t{}) do
             with {:ok, data} <- encode(unquote(Macro.escape(topic)), event, []),
                  {:ok, encoded} <- Jason.encode(data),
                  {:ok, compressed} <- encrypt(t, encoded),
                  do: encrypt(t, compressed)
           end

           @doc ~S"""
           Encode an event to binary.

           ## Examples

           ```elixir
           iex> bin_encode(⊰Event{...}⊱)
           {:ok, <<...>>}
           ```
           """
           @spec bin_encode(Kvasir.Event.t()) :: {:ok, binary} | {:error, term}
           def bin_encode(event)

           def bin_encode(event = %t{__meta__: meta}) do
             with {:ok, m} <- Kvasir.Event.Meta.encode(meta, unquote(key)),
                  {:ok, p} <- Kvasir.Type.Serializer.encode(t.__event__(:fields), event),
                  unpacked <- if(m == %{}, do: p, else: [m, p]),
                  {:ok, packed} <- Msgpax.pack(unpacked, iodata: false),
                  {:ok, compressed} <- compress(t, packed),
                  {:ok, data} <- encrypt(t, compressed) do
               type = t.__event__(:type)
               %{major: major, minor: minor, patch: patch} = t.__event__(:version)

               {:ok,
                <<type::binary, 0, major::size(16), minor::size(16), patch::size(16),
                  data::binary>>}
             end
           end

           defp compress(event, data)
           unquote(compression)

           defp encrypt(event, data)
           unquote(encryption)

           ### Decoders ###

           @doc ~S"""
           Decode a JSON encoded event.

           ## Examples

           ```elixir
           iex> decode("{...}")
           {:ok, ⊰Event{...}⊱}
           ```
           """
           @spec decode(binary) :: {:ok, Kvasir.Event.t()} | {:error, term}
           def decode(event) do
             with {:ok, unpacked} <- Jason.decode(event),
                  {:ok, e} <- event(MapX.get(unpacked, :type)) do
               decode(e, unquote(key), unpacked, [])
             end
           end

           @doc ~S"""
           Decode a binary encoded event.

           ## Examples

           ```elixir
           iex> bin_decode(<<...>>)
           {:ok, ⊰Event{...}⊱}
           ```
           """
           @spec bin_decode(binary) :: {:ok, Kvasir.Event.t()} | {:error, term}
           def bin_decode(event)
           unquote(bin_decode)
           def bin_decode(_), do: {:error, :unknown_event_type}

           defp event(type)
           unquote(event)
           defp event(_), do: {:error, :unknown_event_type}

           defp bin_decode(event, key, version, data) do
             with {:ok, decrypted} <- decrypt(event, data),
                  {:ok, decompressed} <- decompress(event, decrypted) do
               binary_decode(event, key, version, decompressed)
             end
           end

           defp decompress(event, data)
           unquote(decompression)

           defp decrypt(event, data)
           unquote(decryption)

           unquote(opts[:extra])
         end
       end}
    end
  end

  defp gen_event_matchers(key, events) do
    {Enum.reduce(events, nil, fn e, acc ->
       quote do
         unquote(acc)

         def bin_decode(
               <<unquote(e.__event__(:type)), 0, major::size(16), minor::size(16),
                 patch::size(16), data::binary>>
             ),
             do:
               bin_decode(
                 unquote(e),
                 unquote(key),
                 %Version{major: major, minor: minor, patch: patch},
                 data
               )
       end
     end),
     Enum.reduce(events, nil, fn e, acc ->
       quote do
         unquote(acc)

         defp event(unquote(e.__event__(:type))), do: {:ok, unquote(e)}
       end
     end)}
  end
end
