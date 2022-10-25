defmodule Baobab do
  @moduledoc """
  Baobab is a pure Elixir implementation of the 
  [Bamboo](https://github.com/AljoschaMeyer/bamboo) append-only log.

  It is fairly opinionated about the DETS persistence of the logs.
  They are considered to be a spool of the logs as retreived.

  Consumers of this library may wish to place a local copy of the logs in
  a store with better indexing and query properties.

  ### Configuration

  config :baobab, spool_dir: "/tmp"

  ### Options

  - `format`: `:entry` or `:binary`, default: `:entry`
  - `log_id`: the author's log identifier, default `0`
  - `clump_id`: the bamboo clump with which associated, default: `"default"`
  - `revalidate`: confirm the store contents are unchanged, default: `false`
  - `replace`: rewrite log contents even if it exists, default: `false`
  """
  @defaults %{format: :entry, log_id: 0, revalidate: false, replace: false, clump_id: "default"}

  BaseX.prepare_module(
    "Base62",
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
    32
  )

  @doc """
  Resolve an identity to its Base62 representation

  Attempts to resolve `~short` using stored logs
  """
  # Looks like a short base62
  def b62identity(identity)
  def b62identity(id) when not is_binary(id), do: {:error, "Unresolvable identity"}

  def b62identity(<<"~", short::binary>>) do
    case Enum.filter(stored_identities(), fn a -> String.starts_with?(a, short) end) do
      [] -> {:error, "Unknown identity: ~" <> short}
      [id] -> id
      _ -> {:error, "Ambiguous identity: ~" <> short}
    end
  end

  # Looks like a base62-encoded key
  def b62identity(identity) when byte_size(identity) == 43, do: identity
  # Looks like a proper key
  def b62identity(identity) when byte_size(identity) == 32, do: BaseX.Base62.encode(identity)
  # I guess it's a stored identity?
  def b62identity(identity) do
    case identity_key(identity, :public) do
      :error -> {:error, "Unknown identity"}
      key -> BaseX.Base62.encode(key)
    end
  end

  defp stored_identities() do
    stored_info() |> Enum.map(fn {a, _, _} -> a end) |> Enum.uniq()
  end

  @doc """
  Create and store a new log entry for a stored identity
  """
  def append_log(payload, identity, options \\ []) do
    {log_id, clump_id} = options |> optvals([:log_id, :clump_id])
    Baobab.Entry.create(payload, clump_id, identity, log_id)
  end

  @doc """
  Compact log contents to only items in the certificate pool for
  the latest entry.  This allows validation while reducing space used
  """
  def compact(author, options \\ []) do
    a = author |> b62identity
    {log_id, clump_id} = options |> optvals([:log_id, :clump_id])

    case all_seqnum(a, options) do
      [] ->
        []

      entries ->
        last = List.last(entries)
        pool = certificate_pool(a, last, log_id, clump_id) |> MapSet.new()
        eset = entries |> MapSet.new()

        for e <- MapSet.difference(eset, pool) do
          {Baobab.Entry.delete(a, e, log_id, clump_id), e}
        end
    end
  end

  @doc """
  Import and store a list of log entries from their binary format.
  """
  @spec import([binary]) :: [%Baobab.Entry{} | {:error, String.t()}]
  def import(binaries, options \\ [])

  def import(binaries, options) when is_list(binaries) do
    do_import(binaries, options |> optvals([:replace, :clump_id]), [])
  end

  def import(_, _), do: [{:error, "Import requires a list of binaries"}]
  defp do_import([], _, acc), do: acc

  defp do_import([binary | rest], {overwrite, clump_id} = opts, acc) do
    result =
      binary
      |> Baobab.Entry.from_binaries(true, clump_id)
      |> Enum.map(fn e -> Baobab.Entry.store(e, clump_id, overwrite) end)

    do_import(rest, opts, acc ++ result)
  end

  @doc """
  Retrieve an author log at a particular sequence number.
  Includes the available certificate pool for its verification.

  Using `:max` as the sequence number will use the latest
  """
  def log_at(author, seqnum, options \\ []) do
    which =
      case seqnum do
        :max -> max_seqnum(author, options)
        n -> n
      end

    ak = author |> b62identity

    {_, log_id, _, clump_id} =
      opts = options |> optvals([:format, :log_id, :revalidate, :clump_id])

    certificate_pool(ak, which, log_id, clump_id)
    |> Enum.reverse()
    |> Enum.map(fn n -> Baobab.Entry.retrieve(ak, n, opts) end)
  end

  @doc """
  Retrieve all available author log entries over a specified range: `{first, last}`.
  """
  def log_range(author, range, options \\ [])

  def log_range(_, {first, last}, _) when first < 2 or last < first,
    do: {:error, "Improper range specification"}

  def log_range(author, {first, last}, options) do
    ak = author |> b62identity

    {_, log_id, _, clump_id} =
      opts = options |> optvals([:format, :log_id, :revalidate, :clump_id])

    first..last
    |> Enum.filter(fn n ->
      manage_content_store(clump_id, {author, log_id, n}, {:entry, :exists})
    end)
    |> Enum.map(fn n -> Baobab.Entry.retrieve(ak, n, opts) end)
  end

  @doc """
  Purges a given log.

  `:all` may be specified for `author` and/or the `log_id` option.
  Specifying both effectively clears the entire store.

  Returns `Baobab.stored_info/0`

  ## Examples

  iex> Baobab.purge(:all, log_id: :all)
  []

  """
  def purge(author, options \\ []) do
    {log_id, clump_id} = optvals(options, [:log_id, :clump_id])

    case {author, log_id} do
      {:all, :all} -> spool(:content, clump_id, :truncate)
      {:all, n} -> spool(:content, clump_id, :match_delete, {:_, n, :_})
      {author, :all} -> spool(:content, clump_id, :match_delete, {author |> b62identity, :_, :_})
      {author, n} -> spool(:content, clump_id, :match_delete, {author |> b62identity, n, :_})
    end

    Baobab.stored_info(clump_id)
  end

  @doc """
  Retrieve all available entries in a particular log
  """
  def full_log(author, options \\ []) do
    opts = options |> optvals([:format, :log_id, :revalidate, :clump_id])

    author |> b62identity |> gather_all_entries(opts, max_seqnum(author, options), [])
  end

  defp gather_all_entries(_, _, 0, acc), do: acc

  defp gather_all_entries(author, opts, n, acc) do
    newacc =
      case Baobab.Entry.retrieve(author, n, opts) do
        :error -> acc
        entry -> [entry | acc]
      end

    gather_all_entries(author, opts, n - 1, newacc)
  end

  @doc false
  def certificate_pool(author, seq, log_id, clump_id) do
    max = max_seqnum(author, log_id: log_id, clump_id: clump_id)

    seq
    |> Lipmaa.cert_pool()
    |> Enum.reject(fn n ->
      n > max or not manage_content_store(clump_id, {author, log_id, n}, {:entry, :exists})
    end)
  end

  @doc """
  Retrieve the latest sequence number on a particular log identified by the
  author key and log number
  """
  def max_seqnum(author, options \\ []) do
    case all_seqnum(author, options) |> List.last() do
      nil -> 0
      max -> max
    end
  end

  @doc """
  Retrieve the list of sequence numbers on a particular log identified by the
  author key and log number
  """
  def all_seqnum(author, options \\ []) do
    auth = author |> b62identity

    {log_id, clump_id} = options |> optvals([:log_id, :clump_id])

    :content
    |> spool(clump_id, :match, {auth, log_id, :"$1"})
    |> List.flatten()
    |> Enum.sort()
  end

  @doc """
  Retreive a paticular entry by author and sequence number.

  `:max` for the sequence number retrieves the latest known entry
  """
  def log_entry(author, seqnum, options \\ [])

  def log_entry(author, seqnum, options) do
    which =
      case seqnum do
        :max -> max_seqnum(author, options)
        n -> n
      end

    opts = options |> optvals([:format, :log_id, :revalidate, :clump_id])
    author |> b62identity |> Baobab.Entry.retrieve(which, opts)
  end

  @doc """
  Create and store a new identity

  An optional secret key to be associated with the identity may provided, either
  raw or base62 encoded. The public key will be derived therefrom.
  """
  def create_identity(identity, secret_key \\ nil) do
    # This is just unrolling how Ed25519 works
    secret =
      case secret_key do
        nil -> :crypto.strong_rand_bytes(32)
        <<raw::binary-size(32)>> -> raw
        <<b62::binary-size(43)>> -> BaseX.Base62.decode(b62)
      end

    # Despite appearances, enacl does not derive public
    # from secret.  Instead it counts on the fact that the
    # two are concatenated. So this stays.
    pair = {secret, Ed25519.derive_public_key(secret)}
    ident_store(:put, {identity, pair})
    elem(pair, 1) |> b62identity
  end

  @doc """
  Rename an extant identity leaving its keys intact.
  """
  def rename_identity(identity, new_name) do
    {sk, _} = ident_store(:get, identity)
    ident_store(:delete, identity)
    # We'll do the extra work to regen the public key
    create_identity(new_name, sk)
  end

  @doc """
  Drop a stored identity. `Baobab` will be unable to recover keys
  (notably `:secret` keys) destroyed herewith.
  """
  def drop_identity(identity), do: ident_store(:delete, identity)

  @doc """
  A list of {author, log_id, max_seqnum} tuples in the configured store
  """
  # This is all crazy inefficient, but I will clean it up at some
  # point in the future if I care enough.
  def stored_info(clump_id \\ "default")
  def stored_info(clump_id), do: stored_info(logs(clump_id), clump_id, [])

  defp stored_info([], _, acc), do: acc |> Enum.sort()

  defp stored_info([{a, l} | rest], clump_id, acc) do
    a =
      case max_seqnum(a, log_id: l, clump_id: clump_id) do
        0 -> acc
        n -> [{a, l, n} | acc]
      end

    stored_info(rest, clump_id, a)
  end

  @doc """
  A list of all {author, log_id, seqnum} tuples in the configured store
  """
  def all_entries(clump_id \\ "default")

  def all_entries(clump_id) do
    :content
    |> spool(clump_id, :foldl, fn item, acc ->
      case item do
        {e, _} -> [e | acc]
        _ -> acc
      end
    end)
  end

  defp logs(clump_id) do
    clump_id
    |> all_entries()
    |> Enum.reduce(MapSet.new(), fn {a, l, _}, c ->
      MapSet.put(c, {a, l})
    end)
    |> MapSet.to_list()
  end

  @doc """
  Retrieve the key for a stored identity.

  Can be either the `:public` or `:secret` key
  """
  def identity_key(identity, which) do
    case ident_store(:get, identity) do
      {secret, public} ->
        case which do
          :secret -> secret
          :public -> public
          :signing -> secret <> public
          _ -> :error
        end

      _ ->
        :error
    end
  end

  @doc """
  List all known identities with their base62 public key representation
  """
  @spec identities() :: [{String.t(), String.t()}]
  def identities() do
    ident_store(:foldl, fn item, acc ->
      case item do
        {a, {_, public}} -> [{a, b62identity(public)} | acc]
        _ -> acc
      end
    end)
    |> Enum.sort()
  end

  @doc """
  Retrieve the current hash of the `:content` or `:identity` store.

  No information should be gleaned from any particular hash beyond whether
  the contents have changed since a previous check.
  """
  def current_hash(which, clump_id \\ "default")

  def current_hash(which, clump_id) do
    key = {clump_id, which}

    case spool(:status, clump_id, :get, key) do
      [{^key, hash}] -> hash
      _ -> recompute_hash(key)
    end
  end

  @doc """
  Retrieve a list of all populated clumps
  """

  def clumps() do
    spool = Application.fetch_env!(:baobab, :spool_dir) |> Path.expand()

    Path.join([spool, "*/content.dets"])
    |> Path.wildcard()
    |> Enum.map(fn p -> clump_from_path(p) end)
    |> Enum.sort()
  end

  @doc """
  Import a bamboo store

  At present this only handles an exported directory as produced by `store_export/1`
  """
  def store_import(path) do
    # Right now we just mirror the export assuming everything
    # is "in its place".  Eventually we should have some switches
    # to allow partial imports.. and from other sources
    top = path |> Path.expand()

    case Path.wildcard(Path.join([top, "identities/*.keyfile.json"])) do
      [] -> notours()
      ids -> import_identities(ids)
    end

    case Path.wildcard(Path.join([top, "content/*/*.bamboo.log"])) do
      [] -> notours()
      logs -> import_logs(logs)
    end
  end

  defp clump_from_path(path) do
    [_, clump | _] = path |> Path.split() |> Enum.reverse()
    clump
  end

  defp notours(), do: raise("Not a Baobab export structure")

  defp import_logs([]), do: :ok

  defp import_logs([bamboo_file | rest]) do
    # The logs themselves contain all of the info for their structure
    # except for the `clump_id` so we'll need that
    cid = clump_from_path(bamboo_file)

    bamboo_file
    |> File.read!()
    |> then(fn contents -> Baobab.import([contents], replace: false, clump_id: cid) end)

    import_logs(rest)
  end

  defp import_identities([]), do: :ok

  defp import_identities([json_file | rest]) do
    case json_file |> File.read!() |> Jason.decode!() do
      # This is surprisingly liberal given our stance on current importing
      %{"identity" => id, "secret_key" => sk, "public_key" => pk} ->
        case create_identity(id, sk) do
          ^pk -> :ok
          _ -> notours()
        end

      _ ->
        notours()
    end

    import_identities(rest)
  end

  @doc """
  Export full store contents to the provided directory
  """
  def store_export(path) do
    where = Path.expand(path)
    id_path = Path.join([where, "identities"])
    :ok = File.mkdir_p(id_path)
    :ok = File.chmod(id_path, 0o700)
    export_identities(identities(), id_path)
    bb_path = Path.join(where, "content")
    :ok = File.mkdir_p(bb_path)
    :ok = File.chmod(bb_path, 0o700)
    export_clumps(Baobab.clumps(), bb_path)
    where
  end

  defp export_clumps([], _), do: :ok

  defp export_clumps([cid | rest], path) do
    export_clump(cid, path)
    export_clumps(rest, path)
  end

  defp export_clump(cid, path) do
    dir = Path.join(path, cid)
    :ok = File.mkdir_p(dir)
    export_logs(Baobab.stored_info(cid), cid, dir)
  end

  defp export_logs([], _, _), do: :ol

  defp export_logs([{a, l, _} | rest], cid, dir) do
    file = Path.join([dir, a <> "_" <> Integer.to_string(l) <> ".bamboo.log"])
    log = Baobab.full_log(a, log_id: l, clump_id: cid, format: :binary) |> Enum.join("")
    :ok = File.write(file, log)
    :ok = File.chmod(file, 0o700)
    export_logs(rest, cid, dir)
  end

  defp export_identities([], _), do: :ok

  defp export_identities([{i, pk} | rest], path) do
    file = Path.join(path, i <> ".keyfile.json")

    {:ok, json} =
      %{
        "source" => "baobab",
        "key_encoding" => "base62",
        "key_type" => "ed25519",
        "identity" => i,
        "public_key" => pk,
        "secret_key" => Baobab.identity_key(i, :secret) |> BaseX.Base62.encode()
      }
      |> Jason.encode()

    :ok = File.write(file, json)
    :ok = File.chmod(file, 0o600)
    export_identities(rest, path)
  end

  @doc false
  def ident_store(action, value \\ nil), do: spool(:identity, "", action, value)

  def spool(which, clump_id, action, value \\ nil) do
    spool_store(which, clump_id, :open)
    retval = spool_act(which, action, value)

    case action in [:truncate, :delete, :put, :match_delete] do
      true -> recompute_hash({clump_id, which})
      false -> :ok
    end

    spool_store(which, clump_id, :close)
    retval
  end

  defp spool_act(which, :get, key) do
    case :dets.lookup(which, key) do
      [{^key, val} | _] -> val
      [] -> nil
    end
  end

  defp spool_act(which, :foldl, fun), do: :dets.foldl(fun, [], which)
  defp spool_act(which, :truncate, _), do: :dets.delete_all_objects(which)
  defp spool_act(which, :delete, key), do: :dets.delete(which, key)
  defp spool_act(which, :put, kv), do: :dets.insert(which, kv)

  defp spool_act(which, :match_delete, key_pattern),
    do: :dets.match_delete(which, {key_pattern, :_})

  defp spool_act(which, :match, key_pattern),
    do: :dets.match(which, {key_pattern, :_})

  defp spool_store(which, clump_id, :open) do
    {:ok, ^which} = :dets.open_file(which, file: proper_db_path(which, clump_id))
  end

  defp spool_store(which, _clump_id, :close), do: :dets.close(which)

  @doc false
  def manage_content_store(clump_id, entry_id, {name, how}),
    do: manage_content_store(clump_id, entry_id, {name, how, nil})

  def manage_content_store(clump_id, {author, log_id, seq}, {name, how, content}) do
    spool_store(:content, clump_id, :open)
    key = {author |> Baobab.b62identity(), log_id, seq}
    curr = spool_act(:content, :get, key)

    actval =
      case {how, curr} do
        {:delete, nil} ->
          :ok

        {:delete, _} ->
          spool_act(:content, :delete, key)

        {:contents, nil} ->
          case name do
            :both -> {:error, :error}
            _ -> :error
          end

        {:contents, map} ->
          case name do
            :both -> {Map.get(map, :entry, :error), Map.get(map, :payload, :error)}
            key -> Map.get(map, key, :error)
          end

        {:hash, %{^name => c}} ->
          YAMFhash.create(c, 0)

        {:write, prev} ->
          case name do
            :both ->
              {entry, payload} = content

              spool_act(:content, :put, {key, %{:entry => entry, :payload => payload}})

            map_key ->
              map = if is_nil(prev), do: %{}, else: prev
              spool_act(:content, :put, {key, Map.merge(map, %{map_key => content})})
          end

        {:exists, nil} ->
          false

        {:exists, _} ->
          true

        {_, _} ->
          :error
      end

    spool_store(:content, clump_id, :close)
    actval
  end

  defp recompute_hash({_, :status}), do: "nahnah"

  defp recompute_hash({clump_id, which}) do
    stuff =
      case which do
        :content -> all_entries(clump_id)
        :identity -> identities()
      end

    hash =
      stuff
      |> :erlang.term_to_binary()
      |> Blake2.hash2b(7)
      |> BaseX.Base62.encode()

    # Even though identities are the same in both
    # I might be convinced otherwise later
    spool(:status, clump_id, :put, {which, hash})
    hash
  end

  defp proper_db_path(:identity, clump_id) when byte_size(clump_id) > 0,
    do: proper_db_path(:identity, "")

  defp proper_db_path(which, clump_id) when is_binary(clump_id) and is_atom(which) do
    file = Atom.to_string(which) <> ".dets"
    dir = Application.fetch_env!(:baobab, :spool_dir) |> Path.expand()
    Path.join([dir, clump_id, file]) |> to_charlist
  end

  defp proper_db_path(_, _), do: raise("Improper clump_id")

  @doc false
  def optvals(opts, keys), do: optvals(opts, keys, [])
  def optvals(_, [], acc), do: Enum.reverse(acc) |> List.to_tuple()

  def optvals(opts, [k | rest], acc),
    do: optvals(opts, rest, [Keyword.get(opts, k, @defaults[k]) | acc])
end
