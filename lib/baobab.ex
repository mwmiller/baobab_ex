defmodule Baobab do
  @moduledoc """
  Baobab is a pure Elixir implementation of the 
  [Bamboo](https://github.com/AljoschaMeyer/bamboo) append-only log.

  It is fairly opinionated about the filesystem persistence of the logs.
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
    a = author |> b62identity

    {_, log_id, _, _} = parse_options(options)

    [log_dir(a, log_id), "**", "{entry_*}"]
    |> Path.join()
    |> Path.wildcard()
    |> Enum.map(fn n -> Path.basename(n) end)
    |> Enum.reduce(0, fn n, a ->
      Enum.max([a, n |> String.split("_") |> List.last() |> String.to_integer()])
    end)
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
    where = id_dir(identity)
    File.mkdir_p(where)
    File.write!(Path.join([where, "secret"]), secret)
    File.chmod!(Path.join([where, "secret"]), 0o600)
    File.write!(Path.join([where, "public"]), public)
    File.chmod!(Path.join([where, "secret"]), 0o644)

    public |> b62identity
  end

  @doc """
  A list of {author, log_id, max_seqnum} tuples in the configured store
  """
  # This is all crazy inefficient, but I will clean it up at some
  # point in the future if I care enough.
  def stored_info(), do: stored_info(logs(), [])

  defp stored_info([], acc), do: Enum.reverse(acc)

  defp stored_info([{a, l} | rest], acc) do
    a =
      case max_seqnum(a, log_id: l) do
        0 -> acc
        n -> [{a, String.to_integer(l), n} | acc]
      end

    stored_info(rest, a)
  end

  defp logs do
    cd = content_dir()

    Path.join([cd, "*", "*"])
    |> Path.wildcard()
    |> Enum.map(fn d -> Path.relative_to(d, cd) |> Path.split() |> List.to_tuple() end)
  end

  @doc """
  Retrieve the key for a stored identity.

  Can be either the `:public` or `:secret` key
  """
  def identity_key(identity, which) do
    case Path.join([id_dir(identity), Atom.to_string(which)]) |> File.read() do
      {:ok, key} -> key
      _ -> :error
    end
  end

  @doc false
  def log_dir(author, log_id) when is_integer(log_id),
    do: log_dir(author, Integer.to_string(log_id))

  def log_dir(author, log_id) do
    Path.join([content_dir(), author, log_id]) |> ensure_exists
  end

  defp id_dir(identity),
    do: Path.join([proper_config_path(), "identity", identity]) |> ensure_exists

  defp content_dir() do
    Path.join([proper_config_path(), "content"])
  end

  defp proper_config_path do
    Application.fetch_env!(:baobab, :spool_dir) |> Path.expand() |> ensure_exists
  end

  defp ensure_exists(path) do
    case File.stat(path) do
      {:ok, _info} ->
        path

      {:error, :enoent} ->
        File.mkdir_p(path)
        ensure_exists(path)

      {:error, error} ->
        raise "Unrecoverable error with " <> path <> ":" <> Atom.to_string(error)
    end
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
