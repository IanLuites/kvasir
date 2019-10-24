defmodule EventStream do
  defstruct ~w(source topic id partition from)a

  def start(stream, opts \\ []) do
    pid = opts[:pid] || self()

    spawn(fn -> Enum.each(stream, &send(pid, {:event, &1})) end)
  end

  def start_link(stream, opts \\ []) do
    pid = opts[:pid] || self()

    spawn_link(fn -> Enum.each(stream, &send(pid, {:event, &1})) end)
  end

  def slice(stream, from..to) do
    if from < 0 or to < 0, do: raise("Negative ranges not allowed.")

    stream
    |> Enumerable.reduce({:cont, {[], 0}}, fn i, {l, c} ->
      if c < to, do: {:cont, {[i | l], c + 1}}, else: {:halt, [i | l]}
    end)
    |> elem(1)
    |> :lists.reverse()
    |> Enumerable.List.slice(from, to)
  end

  defimpl Enumerable, for: __MODULE__ do
    def count(_stream), do: {:error, __MODULE__}

    def member?(_stream, _value), do: {:error, __MODULE__}

    def slice(_stream), do: {:error, __MODULE__}

    def reduce(es = %EventStream{source: source}, acc, fun) do
      fun_x = fn e, {o, a} ->
        {x, y} = fun.(e, a)
        {x, {Kvasir.Offset.set(o, e.__meta__.partition, e.__meta__.offset), y}}
      end

      {acc_t, acc_v} = acc
      acc = {acc_t, {Kvasir.Offset.create(), acc_v}}

      {_t, {_offset, cold}} =
        source.__storages__()
        |> storages(es.topic, nil)
        |> Kernel.++([{source.__source__(), Module.concat(source, Source)}])
        |> cold_storage(es, acc, fun_x)

      {:done, cold}
    end

    defp storages(storages, topic, from, acc \\ [])
    defp storages([], _topic, _from, acc), do: acc
    defp storages(storages, _topic, nil, _acc), do: :lists.reverse(storages)

    defp storages([h = {m, n} | t], topic, from, acc) do
      case m.contains?(n, topic, from) do
        true -> [h | acc]
        false -> storages(t, topic, from, [h | acc])
        :maybe -> storages(t, topic, from, [h | acc])
      end
    end

    defp cold_storage([], _es, result, _fun), do: result

    defp cold_storage([{cold, name} | tail], es, acc, fun) do
      if cold.contains?(name, es.topic, es.from) in [false] do
        cold_storage(tail, es, acc, fun)
      else
        {:ok, stream} =
          cold.stream(name, es.topic, from: es.from, id: es.id, partition: es.partition)

        {type, {new_acc, o}} =
          case Enumerable.reduce(stream, acc, fun) do
            {:halted, {offset, a}} -> {:halted, {a, offset}}
            {:suspended, {offset, a}} -> {:suspend, {a, offset}}
            {t, []} -> {t, {[], es.from}}
            {t, a = [{offset, _} | _]} -> {t, {Enum.map(a, &elem(&1, 1)), offset}}
            {t, a = {offset, _}} -> {t, {a, offset}}
          end

        cond do
          type == :halted ->
            {type, {o, new_acc}}

          type == :done and tail == [] ->
            {type, new_acc}

          :default ->
            tt = if(type == :done, do: :cont, else: type)
            o2 = Kvasir.Offset.bump_merge(es.from, o)

            cold_storage(tail, %{es | from: o2}, {tt, new_acc}, fun)
        end
      end
    end
  end

  defimpl Inspect, for: __MODULE__ do
    import Inspect.Algebra

    def inspect(%{topic: topic, id: id, partition: partition, from: from}, opts) do
      filter =
        Enum.reject(
          [
            id: id,
            partition: partition,
            from: from
          ],
          &is_nil(elem(&1, 1))
        )

      if filter == [] do
        concat(["#EventStream<", to_doc(topic.topic, opts), ">"])
      else
        f = filter(to_doc(filter, opts))
        concat(["#EventStream<", to_doc(topic.topic, opts), ",", f, ">"])
      end
    end

    @spec filter(tuple) :: tuple
    defp filter({a, {b1, {b2a, {b2b1, {b2b2a, _b2b2b, b2b2c}, b2b3}, b2c, b2d}, _b3}, c}) do
      {a,
       {b1, {b2a, {b2b1, {b2b2a, {:doc_color, " ", :default_color}, b2b2c}, b2b3}, b2c, b2d},
        {:doc_cons, {:doc_break, "", :strict},
         {:doc_cons, {:doc_color, "", :default_color}, {:doc_color, :doc_nil, [:reset, :yellow]}}}},
       c}
    end
  end
end
