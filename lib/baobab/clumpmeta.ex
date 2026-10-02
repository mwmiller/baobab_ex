defmodule Baobab.ClumpMeta do
  alias Baobab.{Identity, Persistence}

  @moduledoc """
  Functions for interacting with clump metadata
  May be useful between consumers to communicate intent
  """

  @max_log :math.pow(2, 64) |> trunc
  @fun_err {:error, "Unresolvable parameters"}

  @doc """
  Create a block of a given type:

  - author: 32 byte-raw or 43-byte base62-encoded value
  - log_id: 64 bit unsigned integer
  - log_spec: `{author, log_id}`

  Returns the current block list
  """
  @spec block(term, binary) :: [term] | {:error, String.t()}
  def block(item, clump_id \\ "default")

  def block(author, clump_id) when is_binary(author) do
    with {:ok, id} <- check_author(author),
         {:ok, cid} <- check_clump_id(clump_id) do
      case blocked?(id, cid) do
        false ->
          Baobab.purge(author, log_id: :all, clump_id: cid)
          do_block(id, cid)

        true ->
          blocks_list(cid)
      end
    else
      err -> err
    end
  end

  def block(log_id, clump_id) when is_integer(log_id) do
    with {:ok, lid} <- check_log_id(log_id),
         {:ok, cid} <- check_clump_id(clump_id) do
      case blocked?(lid, cid) do
        false ->
          Baobab.purge(:all, log_id: lid, clump_id: cid)
          do_block(lid, cid)

        true ->
          blocks_list(cid)
      end
    else
      err -> err
    end
  end

  def block({author, log_id}, clump_id) do
    with {:ok, id} <- check_author(author),
         {:ok, lid} <- check_log_id(log_id),
         {:ok, cid} <- check_clump_id(clump_id) do
      case blocked?({author, log_id}, cid) do
        false ->
          Baobab.purge(id, log_id: lid, clump_id: cid)
          do_block({id, lid}, cid)

        true ->
          blocks_list(cid)
      end
    else
      err -> err
    end
  end

  def block(_, _, _), do: @fun_err

  defp do_block(val, cid) do
    new =
      case Persistence.action(:metadata, cid, :get, :blocks) do
        %MapSet{} = curr -> MapSet.put(curr, val)
        nil -> MapSet.new([val])
      end

    Persistence.action(:metadata, cid, :put, {:blocks, new})
    MapSet.to_list(new)
  end

  defp check_clump_id(cid) do
    case Enum.any?(Baobab.clumps(), fn c -> c == cid end) do
      true -> {:ok, cid}
      false -> {:error, "Unknown clump_id"}
    end
  end

  defp check_author(a) do
    case Identity.as_base62(a) do
      {:error, _} ->
        {:error, "Improper author supplied"}

      id ->
        case Enum.any?(Baobab.Identity.list(), fn {_n, k} -> k == id end) do
          true -> {:error, "May not block identities controlled by Baobab"}
          false -> {:ok, id}
        end
    end
  end

  defp check_log_id(lid) when is_integer(lid) and lid >= 0 and lid <= @max_log, do: {:ok, lid}
  defp check_log_id(_), do: {:error, "Improper log_id"}

  @doc """
  Remove an extant block specified as per `block/2`

  Returns the current block list
  """
  @spec unblock(term, binary) :: :ok | {:error, String.t()}
  def unblock(item, clump_id \\ "default")

  # We can be more liberal saying we'll remove things we never saved.
  def unblock(item, clump_id) do
    with {:ok, cid} <- check_clump_id(clump_id) do
      do_unblock(item, cid)
    else
      err -> err
    end
  end

  defp do_unblock(val, cid) do
    case Persistence.action(:metadata, cid, :get, :blocks) do
      nil ->
        :ok

      %MapSet{} = curr ->
        new = MapSet.delete(curr, val)

        Persistence.action(
          :metadata,
          cid,
          :put,
          {:blocks, new}
        )

        MapSet.to_list(new)
    end
  end

  @doc """
  Check whether a log_id is blocked by any means: literal block
  (author, log_id, or {author, log_id}) or pattern block.
  """
  @spec blocked?(term, binary) :: boolean | {:error, String.t()}
  def blocked?(item, clump_id \\ "default")

  def blocked?({author, log_id, _seq}, clump_id) do
    with {:ok, cid} <- check_clump_id(clump_id) do
      check_block(get_blocks(cid), author, log_id) or
        pattern_matches?(log_id, cid)
    else
      err -> err
    end
  end

  def blocked?(log_id, clump_id) when is_integer(log_id) do
    with {:ok, cid} <- check_clump_id(clump_id) do
      do_blocked_check(log_id, cid) or pattern_matches?(log_id, cid)
    else
      err -> err
    end
  end

  def blocked?(item, clump_id) do
    with {:ok, cid} <- check_clump_id(clump_id) do
      do_blocked_check(item, cid)
    else
      err -> err
    end
  end

  defp do_blocked_check(val, clump_id), do: clump_id |> get_blocks |> MapSet.member?(val)

  defp get_blocks(cid) do
    case Persistence.action(:metadata, cid, :get, :blocks) do
      nil -> MapSet.new()
      ms -> ms
    end
  end

  @doc """
  Lists current blocks on the supplied clump_id
  """
  @spec blocks_list(binary) :: [term] | {:error, String.t()}
  def blocks_list(clump_id \\ "default") do
    with {:ok, cid} <- check_clump_id(clump_id) do
      cid
      |> get_blocks()
      |> MapSet.to_list()
      |> Enum.map(fn
        {a, l} -> [a, l]
        i -> i
      end)
    else
      err -> err
    end
  end

  @doc """
  Block log_ids matching a bitwise pattern.

  The pattern is `%{op: :eq, mask: mask, v: value}`. Any log_id where
  `Bitwise.band(log_id, mask) == value` is considered blocked.

  Returns the current patterns list.
  """
  @spec block_pattern(map, binary) :: [map] | {:error, String.t()}
  def block_pattern(pattern, clump_id \\ "default")

  def block_pattern(%{op: :eq, mask: mask, v: value}, clump_id)
      when is_integer(mask) and is_integer(value) do
    with {:ok, cid} <- check_clump_id(clump_id) do
      do_block_pattern(mask, value, cid)
    else
      err -> err
    end
  end

  def block_pattern(_, _), do: {:error, "Pattern must be %{op: :eq, mask: mask, v: value}"}

  defp do_block_pattern(mask, value, cid) do
    patterns = get_patterns(cid)

    if already_has_pattern?(patterns, mask, value) do
      patterns_to_list(patterns)
    else
      new = [%{op: :eq, mask: mask, v: value} | patterns]
      save_patterns(new, cid)
      patterns_to_list(new)
    end
  end

  @spec already_has_pattern?([map], integer, integer) :: boolean
  defp already_has_pattern?(patterns, mask, value) do
    Enum.any?(patterns, fn
      %{op: :eq, mask: ^mask, v: ^value} -> true
      _ -> false
    end)
  end

  @doc """
  Remove a pattern that was blocking log_ids.

  Returns the current patterns list.
  """
  @spec unblock_pattern(map, binary) :: [map] | {:error, String.t()}
  def unblock_pattern(pattern, clump_id \\ "default")

  def unblock_pattern(%{op: :eq, mask: mask, v: value}, clump_id)
      when is_integer(mask) and is_integer(value) do
    with {:ok, cid} <- check_clump_id(clump_id) do
      patterns = get_patterns(cid)

      new =
        Enum.reject(patterns, fn
          %{op: :eq, mask: ^mask, v: ^value} -> true
          _ -> false
        end)

      save_patterns(new, cid)
      patterns_to_list(new)
    else
      err -> err
    end
  end

  def unblock_pattern(_, _), do: {:error, "Pattern must be %{op: :eq, mask: mask, v: value}"}

  @doc """
  Returns the list of active block patterns for the given clump.
  """
  @spec patterns_list(binary) :: [map] | {:error, String.t()}
  def patterns_list(clump_id \\ "default") do
    with {:ok, cid} <- check_clump_id(clump_id) do
      get_patterns(cid) |> patterns_to_list()
    else
      err -> err
    end
  end

  defp patterns_to_list(patterns), do: patterns

  defp get_patterns(cid) do
    case Persistence.action(:metadata, cid, :get, :block_patterns) do
      nil -> []
      list -> list
    end
  end

  defp save_patterns(patterns, cid) do
    Persistence.action(:metadata, cid, :put, {:block_patterns, patterns})
  end

  @doc """
  Check whether a log_id is matched by any active block pattern.
  """
  @spec pattern_matches?(integer, binary) :: boolean
  def pattern_matches?(log_id, clump_id \\ "default") when is_integer(log_id) do
    with {:ok, cid} <- check_clump_id(clump_id) do
      get_patterns(cid)
      |> Enum.any?(fn %{op: :eq, mask: m, v: v} -> Bitwise.band(log_id, m) == v end)
    else
      _ -> false
    end
  end

  @doc """
  Filter out blocked clump logs from a supplied list of entry
  tuples ({`author`, `log_id`, `seq_num`})

  Checks literal blocks (author/log_id/entry) and pattern blocks
  (bitwise mask match on log_id).
  """
  @spec filter_blocked([tuple], binary) :: [tuple] | {:error, String.t()}
  def filter_blocked(entries, clump_id \\ "default") do
    with {:ok, cid} <- check_clump_id(clump_id) do
      block_filter(entries, get_blocks(cid), get_patterns(cid), [])
    else
      err -> err
    end
  end

  defp block_filter([], _, _, acc), do: Enum.reverse(acc)

  defp block_filter(entries, ms, patterns, acc) do
    {base_logs, families} = classify_patterns(patterns)
    do_block_filter(entries, ms, base_logs, families, acc)
  end

  defp do_block_filter([], _, _, _, acc), do: Enum.reverse(acc)

  defp do_block_filter([entry | rest], ms, base_logs, families, acc) do
    {a, l, _e} =
      case entry do
        [a, l, e] -> {a, l, e}
        {a, l, e} -> {a, l, e}
      end

    case check_block(ms, a, l) or
           MapSet.member?(base_logs, Bitwise.band(l, 0x00FFFFFFFFFFFFFF)) or
           MapSet.member?(families, Bitwise.bsr(l, 48)) do
      true -> do_block_filter(rest, ms, base_logs, families, acc)
      false -> do_block_filter(rest, ms, base_logs, families, [entry | acc])
    end
  end

  defp classify_patterns(patterns) do
    Enum.reduce(patterns, {MapSet.new(), MapSet.new()}, fn
      %{op: :eq, mask: 0x00FFFFFFFFFFFFFF, v: v}, {bl, fam} ->
        {MapSet.put(bl, v), fam}

      %{op: :eq, mask: 0x00FF000000000000, v: v}, {bl, fam} ->
        {bl, MapSet.put(fam, Bitwise.bsr(v, 48))}

      _, acc ->
        acc
    end)
  end

  defp check_block(ms, a, l),
    do: Enum.any?([a, l, {a, l}], fn ls -> MapSet.member?(ms, ls) end)
end
