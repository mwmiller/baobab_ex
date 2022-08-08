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
  - `revalidate`: confirm the store contents are unchanged, default: `false`
  - `replace`: rewrite log contents even if it exists, default: `false`
  """

  BaseX.prepare_module(
    "Base62",
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
    32
  )

  defp parse_options(opts) do
    {Keyword.get(opts, :format, :entry), Keyword.get(opts, :log_id, 0),
     Keyword.get(opts, :revalidate, false), Keyword.get(opts, :replace, false)}
  end

  @doc """
  Create and store a new log entry for a stored identity
  """
  def append_log(payload, identity, options \\ []) do
    {_, log_id, _, _} = parse_options(options)
    Baobab.Entry.create(payload, identity, log_id)
  end

  @doc """
  Compact log contents to only items in the certificate pool for
  the latest entry.  This allows validation while reducing space used
  """
  def compact(author, options \\ []) do
    a = author |> b62identity
    opts = parse_options(options)

    case all_seqnum(a, options) do
      [] ->
        []

      entries ->
        last = List.last(entries)
        pool = certificate_pool(a, last, opts) |> MapSet.new()
        eset = entries |> MapSet.new()

        for e <- MapSet.difference(eset, pool) do
          {Baobab.Entry.delete(a, e, opts), e}
        end
    end
  end

  @doc """
  Import and store a list of log entries from their binary format.
  """
  @spec import([binary]) :: [%Baobab.Entry{} | :error]
  def import(binaries, opts \\ [])

  def import(binaries, opts) when is_list(binaries) do
    {_, _, _, overwrite} = parse_options(opts)
    do_import(binaries, overwrite, [])
  end

  def import(_, _), do: [:error]
  defp do_import([], _, acc), do: Enum.reverse(acc)

  defp do_import([binary | rest], overwrite, acc) do
    entry = binary |> Baobab.Entry.from_binary(false) |> Baobab.Entry.store(overwrite)
    do_import(rest, overwrite, [entry | acc])
  end

  @doc """
  Retrieve the latest entry.

  Includes the available certificate pool for its verification.
  """
  def latest_log(author, options \\ []) do
    author |> b62identity |> log_at(max_seqnum(author, options), options)
  end

  @doc """
  Retrieve an author log at a particular sequence number.

  Includes the available certificate pool for its verification.
  """
  def log_at(author, seq, options \\ []) do
    ak = author |> b62identity
    opts = parse_options(options)

    certificate_pool(ak, seq, opts)
    |> Enum.reverse()
    |> Enum.map(fn n -> Baobab.Entry.retrieve(ak, n, opts) end)
  end

  @doc """
  Retrieve all available entries in a particular log
  """
  def full_log(author, options \\ []) do
    opts = parse_options(options)
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
  def certificate_pool(author, seq, {_, log_id, _, _}) do
    max = max_seqnum(author, log_id: log_id)
    seq |> Lipmaa.cert_pool() |> Enum.reject(fn n -> n > max end)
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
  Retrieve the list of  sequence numbers on a particular log identified by the
  author key and log number
  """
  def all_seqnum(author, options \\ []) do
    auth = author |> b62identity

    {_, log_id, _, _} = parse_options(options)

    :content
    |> db
    |> Pockets.keys_stream()
    |> Stream.filter(fn {a, l, _} -> a == auth and l == log_id end)
    |> Stream.map(fn {_, _, e} -> e end)
    |> Enum.sort()
  end

  @doc """
  Retrieve the latest entry on a particular log identified by the
  author key and log number
  """
  def max_entry(author, options \\ [])

  def max_entry(author, options) do
    opts = parse_options(options)
    author |> b62identity |> Baobab.Entry.retrieve(max_seqnum(author, options), opts)
  end

  @doc """
  Create and store a new identity
  """
  # Maybe make it possible to provide secret ket or both
  # No overwiting? Error handling?
  def create_identity(identity) do
    {secret, public} = Ed25519.generate_key_pair()
    Pockets.put(db(:identity), identity, {secret, public})
    public |> b62identity
  end

  @doc """
  A list of {author, log_id, max_seqnum} tuples in the configured store
  """
  # This is all crazy inefficient, but I will clean it up at some
  # point in the future if I care enough.
  def stored_info(), do: stored_info(logs(), [])

  defp stored_info([], acc), do: acc |> Enum.sort()

  defp stored_info([{a, l} | rest], acc) do
    a =
      case max_seqnum(a, log_id: l) do
        0 -> acc
        n -> [{a, l, n} | acc]
      end

    stored_info(rest, a)
  end

  defp logs do
    :content
    |> db
    |> Pockets.keys_stream()
    |> Stream.map(fn {a, l, _} -> {a, l} end)
    |> Enum.uniq()
  end

  @doc """
  Retrieve the key for a stored identity.

  Can be either the `:public` or `:secret` key
  """
  def identity_key(identity, which) do
    case Pockets.get(db(:identity), identity) do
      {secret, public} ->
        case which do
          :secret -> secret
          :public -> public
          _ -> :error
        end

      _ ->
        :error
    end
  end

  @doc false
  def db(which, action \\ :open) do
    case which do
      :all ->
        for db <- [:identity, :content] do
          pockets_act(db, action)
        end

      which ->
        pockets_act(which, action)
    end

    which
  end

  defp pockets_act(which, :open) do
    {:ok, ^which} =
      Pockets.open(which, Path.join([proper_config_path(), Atom.to_string(which) <> ".dets"]),
        create?: true
      )
  end

  defp pockets_act(which, :close), do: Pockets.close(which)

  defp proper_config_path do
    Application.fetch_env!(:baobab, :spool_dir) |> Path.expand()
  end

  @doc """
  Resolve an identity to its Base62 representation
  """
  # Looks like a base62-encoded key
  def b62identity(author) when byte_size(author) == 43, do: author
  # Looks like a proper key
  def b62identity(author) when byte_size(author) == 32, do: BaseX.Base62.encode(author)
  # I guess it's a stored identity?
  def b62identity(author) do
    case identity_key(author, :public) do
      :error -> raise "Cannot resolve author: " <> author
      key -> BaseX.Base62.encode(key)
    end
  end
end
