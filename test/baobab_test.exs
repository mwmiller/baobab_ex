defmodule BaobabTest do
  use ExUnit.Case
  alias Baobab.{ClumpMeta, Identity, Interchange, Persistence}
  doctest Baobab

  # I do not see the case for a config variable for this
  @export_dir "/tmp/bao_test_export"

  setup do
    Baobab.create_clump("default")

    on_exit(fn ->
      File.rm_rf(Application.fetch_env!(:baobab, :spool_dir) |> Path.expand())
      File.rm_rf(@export_dir)
    end)
  end

  test "as_base62 handles raw keys beginning with ~ (0x7E)" do
    # Regression: a 32-byte key whose first byte is "~" used to fall into
    # the short-identity clause and return an error.
    key = <<0x7E, :crypto.strong_rand_bytes(31)::binary>>
    assert BaseX.Base62.encode(key) == Identity.as_base62(key)
  end

  test "import remote" do
    content_hash =
      "an40NbEEIao13pXVkt98XIKvaH7pbY9cpwhFVtxiHfRIEo2HOzGogAWlgB8ev135AChYqUw0WflMVgVJDOCAri"

    remote_entry = File.read!("test/remote_entry")

    [local_entry | _] = Interchange.import_binaries(remote_entry)

    assert %Baobab.Entry{seqnum: 1, log_id: 0, size: 33, tag: <<0>>} = local_entry
    author = local_entry.author

    assert local_entry == Baobab.log_entry(author, :max)
    assert remote_entry == Baobab.log_entry(author, :max, format: :binary)
    assert [{"7nzwZrUYdugEt4WH8FRuWLPekR4MFzrRauIudDhmBmG", 0, 1}] = Baobab.stored_info()

    assert content_hash == Persistence.content_hash()

    assert ["default"] == Baobab.clumps()

    # More interchange stuff might as well do it here
    # We demand at least one identity, so...
    Identity.create("rando")

    assert ["8nzwZrUYdugEt4WH8FRuWLPekR4MFzrRauIudDhmBmG"] ==
             Baobab.ClumpMeta.block("8nzwZrUYdugEt4WH8FRuWLPekR4MFzrRauIudDhmBmG")

    assert @export_dir == Interchange.export_store(@export_dir)
    assert [] == Baobab.ClumpMeta.unblock("8nzwZrUYdugEt4WH8FRuWLPekR4MFzrRauIudDhmBmG")
    assert [] == Baobab.purge(:all, log_id: :all)
    refute content_hash == Persistence.content_hash()
    Identity.drop("rando")
    assert :ok == Interchange.import_store(@export_dir)
    assert content_hash == Persistence.content_hash()

    assert ["8nzwZrUYdugEt4WH8FRuWLPekR4MFzrRauIudDhmBmG"] = Baobab.ClumpMeta.blocks_list()
  end

  test "local use" do
    b62author = Identity.create("testy")
    root = Baobab.append_log("An entry for testing", "testy")
    assert %Baobab.Entry{seqnum: 1, log_id: 0} = root

    assert %Baobab.Entry{seqnum: 2, log_id: 0} =
             Baobab.append_log("A second entry for testing", "testy")

    assert root == Baobab.log_entry("testy", 1, revalidate: true)

    other_root = Baobab.append_log("A whole new log!", "testy", log_id: 1)
    assert %Baobab.Entry{seqnum: 1, log_id: 1} = other_root
    assert other_root == Baobab.log_entry("testy", 1, log_id: 1)

    <<short::binary-size(5), _::binary>> = b62author
    assert b62author == Identity.as_base62("~" <> short)

    assert Baobab.full_log(b62author) |> Enum.count() == 2
    assert Baobab.full_log(b62author, log_id: 1) == [other_root]

    for n <- 3..14 do
      assert %Baobab.Entry{seqnum: ^n, log_id: 0} =
               Baobab.append_log("Entry: " <> Integer.to_string(n), "testy")
    end

    author_key = Identity.key("testy", :public)
    partial = Baobab.log_at(b62author, 5, format: :binary)
    assert Enum.count(partial) == 8
    latest = Baobab.log_at(author_key, :max, revalidate: true)
    assert Enum.count(latest) == 4
    full = Baobab.full_log(author_key, log_id: 0)
    assert Enum.count(full) == 14
    assert %Baobab.Entry{payload: "Entry: 6"} = Baobab.log_entry(author_key, 6)

    assert [^root | _] = Interchange.import_binaries(partial)
    assert [^root | _] = latest
    assert [^root | _] = full

    assert Baobab.max_seqnum("testy", log_id: 0) == 14
    assert Baobab.max_seqnum("testy", log_id: 1) == 1

    assert [{^b62author, 0, 14}, {^b62author, 1, 1}] = Baobab.stored_info()
    assert Baobab.log_range(b62author, {2, 14}) |> length() == 13

    assert :ok = Baobab.compact("testy")

    assert :error = Baobab.log_entry("testy", 2)
    assert [{^b62author, 0, 14}, {^b62author, 1, 1}] = Baobab.stored_info()

    assert Baobab.log_range(b62author, {2, 14}) |> length() == 3
    assert Baobab.all_entries() |> length() == 5
  end

  test "identity management" do
    b62id = Identity.create("first_id")
    sk = Identity.key("first_id", :secret)
    assert b62id == Identity.create("first_dupe", sk)
    assert b62id == Identity.create("first_dupe", BaseX.Base62.encode(sk))
    assert 2 == Enum.count(Identity.list())

    assert b62id == Identity.key("first_dupe", :public) |> Identity.as_base62()
    assert b62id == Identity.rename("first_dupe", "final_id")
    assert :error == Identity.key("first_dupe", :public)

    assert :ok == Identity.drop("final_id")
    assert :error == Identity.key("final_id", :public)
    assert [{"first_id", _}] = Identity.list()
  end

  test "errors or not" do
    assert {:error, "Improper arguments"} == Identity.create(:dude)
    assert {:error, "Improper arguments"} == Identity.create(nil)
    assert {:error, "Improper arguments"} = Identity.create("bad_alias", "notakey")

    assert {:error, "Improper Base62 key"} =
             Identity.create("bad_alias", "itsmaybeakeymaybeakeymaybeakeymaybeakeynah!")

    new_guy = Identity.create("newbie")
    assert {:error, "Identities must be strings"} = Identity.rename("newbie", nil)
    assert new_guy == Identity.key("newbie", :public) |> Identity.as_base62()
    assert {:error, "No such identity"} == Identity.drop(new_guy)
    assert :error = Identity.key("newb", :secret)
    assert :error = Identity.key("newb", :public)

    assert {:error, "Unknown identity: ~short"} = Identity.as_base62("~short")

    assert [{:error, "Import requires a list of binaries"}] = Interchange.import_binaries(:stuff)

    assert [] = Baobab.log_at("0123456789ABCDEF0123456789ABCDEF", 5)
    assert [] = Baobab.log_at("0123456789ABCDEF0123456789ABCDEF0123456789A", 5)

    assert {:error, "Improper author supplied"} == ClumpMeta.block({"dude", 2})
    assert {:error, "Improper log_id"} == ClumpMeta.block(-1, "FakeClumpName")
    assert {:error, "Unknown clump_id"} == ClumpMeta.block(2, "FakeClumpName")

    assert {:error, "May not block identities controlled by Baobab"} =
             ClumpMeta.block({new_guy, 2})
  end

  test "own signature validated on write" do
    {sk, _pk} = Ed25519.generate_key_pair()
    {_other_sk, other_pk} = Ed25519.generate_key_pair()
    Identity.ident_store(:put, {"mismatched", {sk, other_pk}})

    assert {:error, "Invalid signature"} =
             Baobab.append_log("This should not be persisted", "mismatched")

    assert [] == Baobab.stored_info()
  end

  test "purgeitory" do
    b62first = Identity.create("first")
    b62second = Identity.create("second")

    Baobab.append_log("The first guy says", "first")
    Baobab.append_log("The second guy says", "second")
    Baobab.append_log("jive talk", "first", log_id: 1337)
    Baobab.append_log("jive response", "second", log_id: 1337)
    Baobab.append_log("alt.binaries.bork.bork.bork", "first", log_id: 42)

    assert length(Baobab.stored_info()) == 5
    assert length(Baobab.purge(:all, log_id: 1337)) == 3
    assert length(Baobab.purge(b62second, log_id: :all)) == 2

    assert [{b62first, 0, 1}] == Baobab.purge("first", log_id: 42)

    assert [] == Baobab.purge(:all, log_id: :all)
  end

  test "blockade" do
    dude = Identity.create("dude")
    guy = Identity.create("guy")
    Baobab.append_log("Hi, you all suck", "dude", log_id: 0)
    Baobab.append_log("Hi, you all suck", "dude", log_id: 1)
    Baobab.append_log("Hi, you all suck", "dude", log_id: 2)
    Baobab.append_log("Hi, you all suck", "dude", log_id: 3)
    Baobab.append_log("dude sure is spammy", "guy")

    assert 5 == Baobab.stored_info() |> Enum.count()

    assert {:error, "May not block identities controlled by Baobab"} ==
             ClumpMeta.block(dude)

    assert [] == ClumpMeta.blocks_list()

    Identity.drop("dude")
    assert 5 == Baobab.stored_info() |> Enum.count()

    assert {:error, "Improper author supplied"} == ClumpMeta.block("dude")
    assert [dude] == ClumpMeta.block(dude)
    assert [3, dude] == ClumpMeta.block(3)
    assert true == ClumpMeta.blocked?({guy, 3, 1})
    assert ClumpMeta.blocked?(dude)
    refute ClumpMeta.blocked?(guy)
    assert [{guy, 0, 1}] == Baobab.stored_info()
    # Unblocking nonexistent changes nothing
    assert [3, dude] == ClumpMeta.unblock(guy)
    # Unblocking improper identitifier changes nothing
    assert [3, dude] == ClumpMeta.unblock("dude")
    # Reblocking extant changes nothing
    assert [3, dude] == ClumpMeta.block(dude)
    assert [3, {dude, 2}, dude] = ClumpMeta.block({dude, 2})
    assert [] == ClumpMeta.filter_blocked([{guy, 3, 1}, {guy, 3, 2}, {dude, 3, 1}, {dude, 2, 1}])
    # Removing more general block does nto remove more specific
    assert [3, {dude, 2}] == ClumpMeta.unblock(dude)
    assert [] == ClumpMeta.filter_blocked([{guy, 3, 1}, {guy, 3, 2}, {dude, 3, 1}, {dude, 2, 1}])
    assert [3] == ClumpMeta.unblock({dude, 2})

    assert [{dude, 2, 1}] ==
             ClumpMeta.filter_blocked([{guy, 3, 1}, {guy, 3, 2}, {dude, 3, 1}, {dude, 2, 1}])

    assert [] == ClumpMeta.unblock(3)
    assert [] == ClumpMeta.blocks_list()
  end

  test "pattern blocking" do
    dude = Identity.create("dude")
    guy = Identity.create("guy")

    # Block by base_log pattern (mask 0x00FFFFFFFFFFFFFF matches lower 56 bits)
    base_mask = 0x00FFFFFFFFFFFFFF
    assert [] == ClumpMeta.patterns_list()

    assert [%{op: :eq, mask: ^base_mask, v: 777}] =
             ClumpMeta.block_pattern(%{op: :eq, mask: base_mask, v: 777})

    # Duplicate pattern is a no-op
    assert [%{op: :eq, mask: ^base_mask, v: 777}] =
             ClumpMeta.block_pattern(%{op: :eq, mask: base_mask, v: 777})

    # A log_id matching the pattern is blocked
    assert ClumpMeta.pattern_matches?(777)
    refute ClumpMeta.pattern_matches?(778)
    assert ClumpMeta.blocked?(777)

    # A second pattern
    assert [_, _] = ClumpMeta.block_pattern(%{op: :eq, mask: base_mask, v: 1337})

    assert [%{op: :eq, mask: ^base_mask, v: 1337}, %{op: :eq, mask: ^base_mask, v: 777}] =
             ClumpMeta.patterns_list()

    # filter_blocked filters entries matching patterns
    entries = [
      {dude, 777, 1},
      {dude, 778, 1},
      {guy, 1337, 1},
      {guy, 42, 1}
    ]

    assert [{_, 778, _}, {_, 42, _}] = ClumpMeta.filter_blocked(entries)

    # Unblock a pattern
    assert [%{op: :eq, mask: ^base_mask, v: 1337}] =
             ClumpMeta.unblock_pattern(%{op: :eq, mask: base_mask, v: 777})

    refute ClumpMeta.pattern_matches?(777)
    assert [%{op: :eq, mask: ^base_mask, v: 1337}] = ClumpMeta.patterns_list()

    # 778 and 777 are no longer filtered, 1337 still is
    assert [{_, 777, _}, {_, 778, _}, {_, 42, _}] = ClumpMeta.filter_blocked(entries)

    # Clean up
    ClumpMeta.unblock_pattern(%{op: :eq, mask: base_mask, v: 1337})
    assert [] == ClumpMeta.patterns_list()
    Identity.drop("dude")
    Identity.drop("guy")
  end

  test "pattern blocking with upper byte mask" do
    # Upper byte mask: 0x00FF000000000000 matches bits 48..55
    # Value stored shifted: 1 << 48 = 0x0001000000000000
    upper_mask = 0x00FF000000000000
    shifted_1 = Bitwise.bsl(1, 48)

    assert [%{op: :eq, mask: ^upper_mask, v: ^shifted_1}] =
             ClumpMeta.block_pattern(%{op: :eq, mask: upper_mask, v: shifted_1})

    # log_id with bits 48..55 = 1 should match
    log_id_1 = Bitwise.bsl(1, 48)
    assert ClumpMeta.pattern_matches?(log_id_1)

    # log_id with bits 48..55 = 2 should not match
    log_id_2 = Bitwise.bsl(2, 48)
    refute ClumpMeta.pattern_matches?(log_id_2)

    # log_id with bits 48..55 = 1 and lower bits = 42 should match
    log_id_1_42 = Bitwise.bsl(1, 48) + 42
    assert ClumpMeta.pattern_matches?(log_id_1_42)

    ClumpMeta.unblock_pattern(%{op: :eq, mask: upper_mask, v: shifted_1})
  end

  test "interchange with patterns" do
    export_dir = "/tmp/bao_test_pattern_export"
    Identity.create("dude")
    Identity.create("guy")
    Baobab.append_log("need content for export", "dude")

    base_mask = 0x00FFFFFFFFFFFFFF
    ClumpMeta.block_pattern(%{op: :eq, mask: base_mask, v: 777})
    ClumpMeta.block_pattern(%{op: :eq, mask: 0x00FF000000000000, v: Bitwise.bsl(1, 48)})

    assert export_dir == Interchange.export_store(export_dir)

    # Clear patterns and reimport
    ClumpMeta.unblock_pattern(%{op: :eq, mask: base_mask, v: 777})
    ClumpMeta.unblock_pattern(%{op: :eq, mask: 0x00FF000000000000, v: Bitwise.bsl(1, 48)})
    assert [] == ClumpMeta.patterns_list()

    assert :ok == Interchange.import_store(export_dir)
    patterns = ClumpMeta.patterns_list()
    assert length(patterns) == 2
    assert Enum.any?(patterns, fn %{mask: m, v: v} -> m == base_mask and v == 777 end)
    assert Enum.any?(patterns, fn %{mask: m, v: _v} -> m == 0x00FF000000000000 end)

    # Clean up
    ClumpMeta.unblock_pattern(%{op: :eq, mask: base_mask, v: 777})
    ClumpMeta.unblock_pattern(%{op: :eq, mask: 0x00FF000000000000, v: Bitwise.bsl(1, 48)})
    Identity.drop("dude")
    Identity.drop("guy")
    File.rm_rf(export_dir)
  end
end
