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

  defp store(which, clump_id, :open) do
    {:ok, ^which} = :dets.open_file(which, file: proper_db_path(which, clump_id))
  end

  defp store(which, _clump_id, :close), do: :dets.close(which)

  @doc """
  Retrieve the current hash of the `:content` or `:identity` store.

  No information should be gleaned from any particular hash beyond whether
  the contents have changed since a previous check.
  """
  def current_hash(which, clump_id \\ "default")

  def current_hash(which, clump_id) do
    case action(:status, clump_id, :get, {clump_id, which}) do
      [{{^clump_id, ^which}, hash}] -> hash
      _ -> recompute_hash(clump_id, which)
    end
  end

  defp recompute_hash(clump_id, table)
  defp recompute_hash(_, :status), do: "nahnah"
  # This one should probably have this available at some point
  defp recompute_hash(_, :metadata), do: "nahnah"

  defp recompute_hash(clump_id, which) do
    stuff =
      case which do
        :content -> Baobab.all_entries(clump_id)
        :identity -> Baobab.Identity.list()
      end

    hash =
      stuff
      |> :erlang.term_to_binary()
      |> Blake2.hash2b(7)
      |> BaseX.Base62.encode()

    # Even though identities are the same in both
    # I might be convinced otherwise later
    action(:status, clump_id, :put, {which, hash})
    hash
  end

  @doc """
  Deal with the peristed bamboo content
  """
  def content(subject, action, entry_id, clump_id, addlval \\ nil)

  def content(subject, action, {author, log_id, seq}, clump_id, addlval) do
    store(:content, clump_id, :open)
    key = {author |> Identity.as_base62(), log_id, seq}
    curr = perform_action(:content, :get, key)

    actval =
      case {action, curr} do
        {:delete, nil} ->
          :ok

        {:delete, _} ->
          perform_action(:content, :delete, key)

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

              perform_action(:content, :put, {key, %{:entry => entry, :payload => payload}})

            map_key ->
              map = if is_nil(prev), do: %{}, else: prev
              perform_action(:content, :put, {key, Map.merge(map, %{map_key => addlval})})
          end

        {:exists, nil} ->
          false

        {:exists, _} ->
          true

        {_, _} ->
          :error
      end

    store(:content, clump_id, :close)
    actval
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
