defmodule Baobab.Interchange do
  @moduledoc """
  Functions related to the interchange of Bamboo data with
  other Bamboo sources
  """

  @doc """
  Import and store a list of log entries from their binary format.
  """
  @spec import_binaries([binary]) :: [%Baobab.Entry{} | {:error, String.t()}]
  def import_binaries(binaries, options \\ [])

  def import_binaries(binary, options) when is_binary(binary),
    do: import_binaries([binary], options)

  def import_binaries(binaries, options) when is_list(binaries) do
    import_listed_binaries(binaries, options |> Baobab.optvals([:replace, :clump_id]), [])
  end

  def import_binaries(_, _), do: [{:error, "Import requires a list of binaries"}]
  defp import_listed_binaries([], _, acc), do: acc

  defp import_listed_binaries([binary | rest], {overwrite, clump_id} = opts, acc) do
    result =
      binary
      |> Baobab.Entry.from_binaries(true, clump_id)
      |> Enum.map(fn e -> Baobab.Entry.store(e, clump_id, overwrite) end)

    import_listed_binaries(rest, opts, acc ++ result)
  end

  @doc """
  Import a bamboo store

  At present this only handles an exported directory as produced by `export_store/1`
  """
  def import_store(path) do
    # Right now we just mirror the export assuming everything
    # is "in its place".  Eventually we should have some switches
    # to allow partial imports.. and from other sources
    top = path |> Path.expand()

    case Path.wildcard(Path.join([top, "identities/*.keyfile.json"])) do
      [] -> notours()
      ids -> import_store_identities(ids)
    end

    case Path.wildcard(Path.join([top, "content/*/*.bamboo.log"])) do
      [] -> notours()
      logs -> import_store_logs(logs)
    end
  end

  defp import_store_identities([]), do: :ok

  defp import_store_identities([json_file | rest]) do
    case json_file |> File.read!() |> Jason.decode!() do
      # This is surprisingly liberal given our stance on current importing
      %{"identity" => id, "secret_key" => sk, "public_key" => pk} ->
        case Baobab.Identity.create(id, sk) do
          ^pk -> :ok
          _ -> notours()
        end

      _ ->
        notours()
    end

    import_store_identities(rest)
  end

  defp import_store_logs([]), do: :ok

  defp import_store_logs([bamboo_file | rest]) do
    # The logs themselves contain all of the info for their structure
    # except for the `clump_id` so we'll need that
    cid = clump_from_path(bamboo_file)

    bamboo_file
    |> File.read!()
    |> then(fn contents -> import_binaries([contents], replace: false, clump_id: cid) end)

    import_store_logs(rest)
  end

  @doc """
  Export full store contents to the provided directory

  Produces:
    - JSON keyfiles for each identity
    - Per clump directories containing a file for each author, `log_id` pair
  """
  def export_store(path) do
    where = Path.expand(path)
    id_path = Path.join([where, "identities"])
    :ok = File.mkdir_p(id_path)
    :ok = File.chmod(id_path, 0o700)
    export_store_identities(Baobab.Identity.list(), id_path)
    bb_path = Path.join(where, "content")
    :ok = File.mkdir_p(bb_path)
    :ok = File.chmod(bb_path, 0o700)
    export_store_clumps(Baobab.clumps(), bb_path)
    where
  end

  defp export_store_identities([], _), do: :ok

  defp export_store_identities([{i, pk} | rest], path) do
    file = Path.join(path, i <> ".keyfile.json")

    {:ok, json} =
      %{
        "source" => "baobab",
        "key_encoding" => "base62",
        "key_type" => "ed25519",
        "identity" => i,
        "public_key" => pk,
        "secret_key" => Baobab.Identity.key(i, :secret) |> BaseX.Base62.encode()
      }
      |> Jason.encode()

    :ok = File.write(file, json)
    :ok = File.chmod(file, 0o600)
    export_store_identities(rest, path)
  end

  defp export_store_clumps([], _), do: :ok

  defp export_store_clumps([cid | rest], path) do
    export_store_clump(cid, path)
    export_store_clumps(rest, path)
  end

  defp export_store_clump(cid, path) do
    dir = Path.join(path, cid)
    :ok = File.mkdir_p(dir)
    export_store_logs(Baobab.stored_info(cid), cid, dir)
  end

  defp export_store_logs([], _, _), do: :ol

  defp export_store_logs([{a, l, _} | rest], cid, dir) do
    file = Path.join([dir, a <> "_" <> Integer.to_string(l) <> ".bamboo.log"])
    log = Baobab.full_log(a, log_id: l, clump_id: cid, format: :binary) |> Enum.join("")
    :ok = File.write(file, log)
    :ok = File.chmod(file, 0o700)
    export_store_logs(rest, cid, dir)
  end

  @doc false
  def clump_from_path(path) do
    [_, clump | _] = path |> Path.split() |> Enum.reverse()
    clump
  end

  defp notours(), do: raise("Not a Baobab export structure")
end
