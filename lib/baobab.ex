defmodule Baobab do
  alias Baobab.{Entry, Identity, Interchange, Persistence}

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

  @doc """
  Create and store a new log entry for a stored identity
  """
  def append_log(payload, identity, options \\ []) do
    {log_id, clump_id} = options |> optvals([:log_id, :clump_id])
    Entry.create(payload, clump_id, identity, log_id)
  end

  @doc """
  Compact log contents to only items in the certificate pool for
  the latest entry.  This allows validation while reducing space used
  """
  def compact(author, options \\ []) do
    a = author |> Identity.as_base62()
    {log_id, clump_id} = options |> optvals([:log_id, :clump_id])

    case all_seqnum(a, options) do
      [] ->
        []

      entries ->
        last = List.last(entries)
        pool = certificate_pool(a, last, log_id, clump_id) |> MapSet.new()
        eset = entries |> MapSet.new()

        for e <- MapSet.difference(eset, pool) do
          {Entry.delete(a, e, log_id, clump_id), e}
        end
    end
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

    ak = author |> Identity.as_base62()

    {_, log_id, _, clump_id} =
      opts = options |> optvals([:format, :log_id, :revalidate, :clump_id])

    certificate_pool(ak, which, log_id, clump_id)
    |> Enum.reverse()
    |> Enum.map(fn n -> Persistence.retrieve(ak, n, opts) end)
  end

  @doc """
  Retrieve all available author log entries over a specified range: `{first, last}`.
  """
  def log_range(author, range, options \\ [])

  def log_range(_, {first, last}, _) when first < 2 or last < first,
    do: {:error, "Improper range specification"}

  def log_range(author, {first, last}, options) do
    ak = author |> Identity.as_base62()

    {_, log_id, _, clump_id} =
      opts = options |> optvals([:format, :log_id, :revalidate, :clump_id])

    first..last
    |> Enum.filter(fn n ->
      Persistence.content(:entry, :exists, {author, log_id, n}, clump_id)
    end)
    |> Enum.map(fn n -> Persistence.retrieve(ak, n, opts) end)
  end

  @doc """
  Purges a given log.

  `:all` may be specified for `author` and/or the `log_id` option.
  Specifying both effectively clears the entire store.

  Returns `stored_info/0`

  ## Examples

  iex> Baobab.purge(:all, log_id: :all)
  []

  """
  def purge(author, options \\ []) do
    {log_id, clump_id} = optvals(options, [:log_id, :clump_id])

    case {author, log_id} do
      {:all, :all} ->
        Persistence.action(:content, clump_id, :truncate)

      {:all, n} ->
        Persistence.action(:content, clump_id, :match_delete, {:_, n, :_})

      {author, :all} ->
        Persistence.action(
          :content,
          clump_id,
          :match_delete,
          {author |> Identity.as_base62(), :_, :_}
        )

      {author, n} ->
        Persistence.action(
          :content,
          clump_id,
          :match_delete,
          {author |> Identity.as_base62(), n, :_}
        )
    end

    stored_info(clump_id)
  end

  @doc """
  Retrieve all available entries in a particular log
  """
  def full_log(author, options \\ []) do
    opts = options |> optvals([:format, :log_id, :revalidate, :clump_id])

    author |> Identity.as_base62() |> gather_all_entries(opts, max_seqnum(author, options), [])
  end

  defp gather_all_entries(_, _, 0, acc), do: acc

  defp gather_all_entries(author, opts, n, acc) do
    newacc =
      case Persistence.retrieve(author, n, opts) do
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
      n > max or
        not Persistence.content(:entry, :exists, {author, log_id, n}, clump_id)
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
    auth = author |> Identity.as_base62()

    {log_id, clump_id} = options |> optvals([:log_id, :clump_id])

    :content
    |> Persistence.action(clump_id, :match, {auth, log_id, :"$1"})
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
    author |> Identity.as_base62() |> Persistence.retrieve(which, opts)
  end

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
    |> Persistence.action(clump_id, :foldl, fn item, acc ->
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
  Retrieve a list of all populated clumps
  """

  def clumps() do
    spool = Application.fetch_env!(:baobab, :spool_dir) |> Path.expand()

    Path.join([spool, "*/content.dets"])
    |> Path.wildcard()
    |> Enum.map(fn p -> Interchange.clump_from_path(p) end)
    |> Enum.sort()
  end

  @doc false
  def optvals(opts, keys), do: optvals(opts, keys, [])
  def optvals(_, [], acc), do: Enum.reverse(acc) |> List.to_tuple()

  def optvals(opts, [k | rest], acc),
    do: optvals(opts, rest, [Keyword.get(opts, k, @defaults[k]) | acc])
end
