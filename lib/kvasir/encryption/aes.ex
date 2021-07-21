defmodule Kvasir.Encryption.AES do
  @default_bits 128

  defmacro encrypt(data, runtime?, opts) do
    {key, cipher, _aead?} = configure!(runtime?, opts)

    quote do
      iv = :crypto.strong_rand_bytes(16)

      {:ok,
       <<0>> <>
         iv <> :crypto.crypto_one_time(unquote(cipher), unquote(key), iv, unquote(data), false)}
    end
  end

  defmacro decrypt(data, runtime?, opts) do
    {key, cipher, _aead?} = configure!(runtime?, opts)

    quote do
      case unquote(data) do
        <<0, iv::binary-size(16), data::binary>> ->
          try do
            {:ok, :crypto.crypto_one_time(unquote(cipher), unquote(key), iv, data, true)}
          rescue
            _ -> {:error, :invalid_encryption}
          end

        _ ->
          {:error, :invalid_encryption_payload}
      end
    end
  end

  require Logger

  @spec configure!(runtime? :: boolean, Keyword.t()) :: {binary, atom, boolean}
  defp configure!(runtime?, opts) do
    bits = Keyword.get(opts, :bits, @default_bits)
    aead? = Keyword.get(opts, :aead, false)
    key_setting = opts[:key]
    key = if runtime?, do: fetch_key!(key_setting, bits), else: fetch_key(key_setting, bits)

    check_key!(key, bits)
    if aead?, do: raise(CompileError, description: "AEAD currently not supported.")

    mode =
      case Keyword.get(opts, :mode, if(aead?, do: :gcm, else: :ctr)) do
        :gcm when not aead? ->
          raise(CompileError, description: "`:gcm` mode only supported with AEAD enabled.")

        m when aead? ->
          raise(CompileError, description: "#{inspect(m)} mode only supported with AEAD disabled.")

        :cbc ->
          Logger.warn(fn -> "Recommend `:ctr` over `:cbc`." end)
          :cbc

        m ->
          m
      end

    {key, :"aes_#{bits}_#{mode}", aead?}
  end

  @spec fetch_key(term, pos_integer) :: binary
  defp fetch_key(key, bits)
  defp fetch_key(nil, bits), do: fetch_key!(:generate, bits)
  defp fetch_key({:system, var}, bits), do: var |> System.get_env() |> fetch_key(bits)
  defp fetch_key(key, bits), do: fetch_key!(key, bits)

  @spec fetch_key!(term, pos_integer) :: binary | no_return
  defp fetch_key!(key, bits)
  defp fetch_key!(nil, _bits), do: raise("No encryption key set.")
  defp fetch_key!({:system, var}, bits), do: var |> System.get_env() |> fetch_key!(bits)

  defp fetch_key!(:generate, bits) do
    with nil <- Application.get_env(:kvasir, __MODULE__) do
      key = :crypto.strong_rand_bytes(div(bits, 8))
      Application.put_env(:kvasir, __MODULE__, key)

      Logger.warn(fn ->
        """
        Generating AES key for encryption.
        This should not be done for production.

        Generated key: #{Base.encode64(key)}
        """
      end)

      key
    end
  end

  defp fetch_key!(data, _bits) do
    case Base.decode64(data) do
      {:ok, key} -> key
      :error -> data
    end
  end

  @spec check_key!(binary, pos_integer) :: :ok | no_return
  defp check_key!(key, bits) do
    cond do
      is_nil(key) -> raise "No encryption key set."
      byte_size(key) != bits / 8 -> raise "Encryption key wrong size."
      :ok -> :ok
    end
  end
end
