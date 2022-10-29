defmodule Baobab.Identity do
  @moduledoc """
  Functions related too Baobab identity (keypair) handling
  """
  BaseX.prepare_module(
    "Base62",
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
    32
  )

  @doc """
  Resolve an identity to its Base62 representation

  Attempts to resolve `~short` using stored logs
  """
  @spec as_base62(String.t()) :: String.t() | {:error, String.t()}
  def as_base62(identity)
  def as_base62(id) when not is_binary(id), do: {:error, "Unresolvable identity"}

  # Looks like a short base62
  def as_base62(<<"~", short::binary>>) do
    case Enum.filter(stored_authors(), fn a -> String.starts_with?(a, short) end) do
      [] -> {:error, "Unknown identity: ~" <> short}
      [id] -> id
      _ -> {:error, "Ambiguous identity: ~" <> short}
    end
  end

  # Looks like a base62-encoded key
  def as_base62(identity) when byte_size(identity) == 43, do: identity
  # Looks like a proper key
  def as_base62(identity) when byte_size(identity) == 32, do: BaseX.Base62.encode(identity)
  # I guess it's a stored identity?
  def as_base62(identity) do
    case key(identity, :public) do
      :error -> {:error, "Unknown identity"}
      key -> BaseX.Base62.encode(key)
    end
  end

  defp stored_authors() do
    Baobab.stored_info() |> Enum.map(fn {a, _, _} -> a end) |> Enum.uniq()
  end

  @doc """
  Create and store a new identity string

  An optional secret key to be associated with the identity may provided, either
  raw or base62 encoded. The public key will be derived therefrom.
  """
  @spec create(String.t(), binary | nil) ::
          String.t() | {:error, String.t()}
  def create(identity, secret_key \\ nil)
  def create(identity, nil), do: create(identity, :crypto.strong_rand_bytes(32))

  def create(identity, sk) when byte_size(sk) == 43 do
    try do
      create(identity, BaseX.Base62.decode(sk))
    rescue
      _ -> {:error, "Improper Base62 key"}
    end
  end

  def create(identity, secret_key)
      when is_binary(identity) and is_binary(secret_key) and byte_size(secret_key) == 32 do
    # Despite appearances, enacl does not derive public
    # from secret.  Instead it counts on the fact that the
    # two are concatenated. So this stays.
    pair = {secret_key, Ed25519.derive_public_key(secret_key)}
    ident_store(:put, {identity, pair})
    elem(pair, 1) |> as_base62
  end

  def create(_, _), do: {:error, "Improper arguments"}

  @doc """
  Rename an extant identity leaving its keys intact.
  """
  @spec rename(String.t(), String.t()) :: String.t() | {:error, String.t()}
  # No guard against extant non-string to allow migration
  def rename(identity, new_name) when is_binary(new_name) do
    {sk, _} = ident_store(:get, identity)
    ident_store(:delete, identity)
    # We'll do the extra work to regen the public key
    create(new_name, sk)
  end

  def rename(_, _), do: {:error, "Identities must be strings"}

  @doc """
  Drop a stored identity. `Baobab` will be unable to recover keys
  (notably `:secret` keys) destroyed herewith.
  """
  @spec drop(String.t()) :: :ok | {:error, String.t()}
  # I am not removing the ability to drop identities which can no
  # longer be created.  If it's in there the consumer should be able to get it out
  def drop(identity) do
    case ident_store(:get, identity) do
      {_sk, _pk} -> ident_store(:delete, identity)
      _ -> {:error, "No such identity"}
    end
  end

  @doc """
  Retrieve the key for a stored identity.

  Can be either the `:public` or `:secret` key
  """
  @spec key(String.t(), atom) :: binary | :error
  def key(identity, which) do
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
  @spec list() :: [{String.t(), String.t()}]
  def list() do
    ident_store(:foldl, fn item, acc ->
      case item do
        {a, {_, public}} -> [{a, as_base62(public)} | acc]
        _ -> acc
      end
    end)
    |> Enum.sort()
  end

  @doc false
  def ident_store(action, value \\ nil), do: Baobab.spool(:identity, "", action, value)
end
