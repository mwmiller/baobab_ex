defmodule Baobab.Persistence do
  alias Baobab.{Entry, Identity}

  @moduledoc """
  Functions related to Baobab values persistence
  """
  @doc """
  Interact with a Baobab persistence mechanism

  Actions closely mirror the underlying `dets` at present
  """
  def action(which, clump_id, action, value \\ nil) do
    store(which, clump_id, :open)
    retval = perform_action(which, action, value)

    case action in [:truncate, :delete, :put, :match_delete] do
      true -> recompute_hash(clump_id, which)
      false -> :ok
    end

    store(which, clump_id, :close)
    retval
  end

  defp perform_action(which, :get, key) do
    case :dets.lookup(which, key) do
      [{^key, val} | _] -> val
      [] -> nil
    end
  end

  defp perform_action(which, :foldl, fun), do: :dets.foldl(fun, [], which)
  defp perform_action(which, :truncate, _), do: :dets.delete_all_objects(which)
  defp perform_action(which, :delete, key), do: :dets.delete(which, key)
  defp perform_action(which, :put, kv), do: :dets.insert(which, kv)

  defp perform_action(which, :match_delete, key_pattern),
    do: :dets.match_delete(which, {key_pattern, :_})

  defp perform_action(which, :match, key_pattern),
    do: :dets.match(which, {key_pattern, :_})

  @doc false
  def store(which, clump_id, :open) do
    {:ok, ^which} = :dets.open_file(which, file: proper_db_path(which, clump_id))
  end

  def store(which, _clump_id, :close), do: :dets.close(which)

  @doc """
  Retrieve the current hash of the `:content` store.

  No information should be gleaned from any particular hash beyond whether
  the contents have changed since a previous check.
  """
  def content_hash(clump_id \\ "default")

  def content_hash(clump_id) do
    # I've made a real hash of this trying to generalise for the
    # majorly different
    case action(:status, clump_id, :get, {clump_id, :content}) do
      {hash, _stuff} ->
        hash

      _ ->
        recompute_hash(clump_id, :content)
        content_hash(clump_id)
    end
  end

  @doc """
  Retrieve the current stored info which is the max entry for each
  stored log
  """
  def current_stored_info(clump_id \\ "default")

  def current_stored_info(clump_id) do
    case action(:status, clump_id, :get, {clump_id, :stored_info}) do
      {:ok, si} ->
        si

      _ ->
        recompute_hash(clump_id, :content)
        current_stored_info(clump_id)
    end
  end

  @doc """
  Retrieve the current value of the `:content` store.

  """
  def current_value(clump_id \\ "default")

  def current_value(clump_id) do
    case action(:status, clump_id, :get, {clump_id, :content}) do
      {_hash, stuff} ->
        stuff

      _ ->
        recompute_hash(clump_id, :content)
    end
  end

  def compact(author, log_id, clump_id) do
    store(:content, clump_id, :open)

    stored =
      action(:content, clump_id, :foldl, fn item, acc ->
        case item do
          {{^author, ^log_id, _} = entry, _} ->
            [entry | acc]

          _ ->
            acc
        end
      end)

    {_, _, tip} = Enum.max_by(stored, fn {_, _, e} -> e end)
    keep = Lipmaa.cert_pool(tip)

    Enum.each(stored, fn {a, l, e} ->
      case e in keep do
        true -> :noop
        false -> perform_action(:content, :delete, {a, l, e})
      end
    end)

    recompute_hash(clump_id, :content)
    store(:content, clump_id, :close)
  end

  defp recompute_hash(clump_id, table)
  defp recompute_hash(_, :status), do: "nahnah"
  # This one should probably have this available at some point
  defp recompute_hash(_, :metadata), do: "nahnah"
  defp recompute_hash(_, :identity), do: "nahnah"

  defp recompute_hash(clump_id, which) do
    {id, stuff} =
      case which do
        :content ->
          val = all_entries(clump_id)
          recompute_si(val, clump_id)
          {clump_id, val}
      end

    hash =
      stuff
      |> :erlang.term_to_binary()
      |> then(fn d -> :crypto.hash(:blake2b, d) end)
      |> BaseX.Base62.encode()

    # Even though identities are the same in both
    # I might be convinced otherwise later
    action(:status, clump_id, :put, {{id, which}, {hash, stuff}})
    hash
  end

  defp all_entries(clump_id) do
    :content
    |> action(clump_id, :foldl, fn item, acc ->
      case item do
        {e, _} -> [e | acc]
        _ -> acc
      end
    end)
  end

  defp recompute_si(all_entries, clump_id) do
    all_entries
    |> Enum.reduce(MapSet.new(), fn {a, l, _}, c ->
      MapSet.put(c, {a, l})
    end)
    |> MapSet.to_list()
    |> make_stored_info(clump_id, [])
    |> then(fn si -> action(:status, clump_id, :put, {{clump_id, :stored_info}, {:ok, si}}) end)
  end

  defp make_stored_info([], _clump_id, acc), do: acc |> Enum.reverse()

  defp make_stored_info([{a, l} | rest], clump_id, acc) do
    a =
      case Baobab.max_seqnum(a, log_id: l, clump_id: clump_id) do
        0 -> acc
        n -> [{a, l, n} | acc]
      end

    make_stored_info(rest, clump_id, a)
  end

  @doc """
  Deal with the peristed bamboo content
  """
  def content(subject, action, entry_id, clump_id, addlval \\ nil)

  def content(subject, action, {author, log_id, seq}, clump_id, addlval) do
    key = {author |> Identity.as_base62(), log_id, seq}
    curr = action(:content, clump_id, :get, key)

    case {action, curr} do
      {:delete, nil} ->
        :ok

      {:delete, _} ->
        action(:content, clump_id, :delete, key)

      {:contents, nil} ->
        case subject do
          :both -> {:error, :error}
          _ -> :error
        end

      {:contents, map} ->
        case subject do
          :both -> {Map.get(map, :entry, :error), Map.get(map, :payload, :error)}
          key -> Map.get(map, key, :error)
        end

      {:hash, %{^subject => c}} ->
        YAMFhash.create(c, 0)

      {:write, prev} ->
        case subject do
          :both ->
            {entry, payload} = addlval
            action(:content, clump_id, :put, {key, %{:entry => entry, :payload => payload}})

          map_key ->
            map = if is_nil(prev), do: %{}, else: prev
            action(:content, clump_id, :put, {key, Map.merge(map, %{map_key => addlval})})
        end

      {:exists, nil} ->
        false

      {:exists, _} ->
        true

      {_, _} ->
        :error
    end
  end

  @doc false
  # Handle the simplest case first
  def retrieve(author, seq, {:binary, log_id, false, clump_id}) do
    entry_id = {author, log_id, seq}

    case content(:both, :contents, entry_id, clump_id) do
      {:error, _} -> :error
      {_, :error} -> :error
      {entry, payload} -> entry <> payload
    end
  end

  # This handles the other three cases:
  # :entry validated or unvalidated
  # :binary validated
  def retrieve(author, seq, {fmt, log_id, validate, clump_id}) do
    entry_id = {author, log_id, seq}
    binary = content(:entry, :contents, entry_id, clump_id)
    res = Entry.from_binaries(binary, validate, clump_id) |> hd

    case {res, fmt} do
      {{:error, :missing}, _} ->
        :error

      {:error, _} ->
        content(:entry, :delete, entry_id, clump_id)
        :error

      {entry, :entry} ->
        entry

      {_, :binary} ->
        binary
    end
  end

  defp proper_db_path(:identity, clump_id) when byte_size(clump_id) > 0,
    do: proper_db_path(:identity, "")

  defp proper_db_path(which, clump_id) when is_binary(clump_id) and is_atom(which) do
    file = Atom.to_string(which) <> ".dets"
    dir = Application.fetch_env!(:baobab, :spool_dir) |> Path.expand()
    Path.join([dir, clump_id, file]) |> to_charlist
  end

  defp proper_db_path(_, _), do: raise("Improper clump_id")
end
