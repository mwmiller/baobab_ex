defmodule BaobabTest do
  use ExUnit.Case
  doctest Baobab

  setup do
    File.mkdir_p(Application.fetch_env!(:baobab, :spool_dir) |> Path.expand())

    on_exit(fn ->
      File.rm_rf(Application.fetch_env!(:baobab, :spool_dir) |> Path.expand())
    end)
  end

  test "import remote entry" do
    remote_entry = File.read!("test/remote_entry")

    [local_entry | _] = Baobab.import([remote_entry])

    assert %Baobab.Entry{seqnum: 1, log_id: 0, size: 33, tag: <<0>>} = local_entry
    author = local_entry.author

    assert local_entry == Baobab.log_entry(author, :max)
    assert remote_entry == Baobab.log_entry(author, :max, format: :binary)
    assert [{"7nzwZrUYdugEt4WH8FRuWLPekR4MFzrRauIudDhmBmG", 0, 1}] = Baobab.stored_info()
  end

  test "local use" do
    b62author = Baobab.create_identity("testy")
    sk = Baobab.identity_key("testy", :secret)
    assert b62author == Baobab.create_identity("testy_dupe", sk)
    assert b62author == Baobab.create_identity("testy_dupe", BaseX.Base62.encode(sk))
    # This test is a little on the nose, but meh.
    assert_raise CaseClauseError, fn -> Baobab.create_identity("testy_bad", "notakey") end

    assert 2 == Enum.count(Baobab.identities())

    root = Baobab.append_log("An entry for testing", "testy")
    assert %Baobab.Entry{seqnum: 1, log_id: 0} = root

    assert %Baobab.Entry{seqnum: 2, log_id: 0} =
             Baobab.append_log("A second entry for testing", "testy")

    assert root == Baobab.log_entry("testy", 1, revalidate: true)

    other_root = Baobab.append_log("A whole new log!", "testy", log_id: 1)
    assert %Baobab.Entry{seqnum: 1, log_id: 1} = other_root
    assert other_root == Baobab.log_entry("testy", 1, log_id: 1)

    <<short::binary-size(5), _::binary>> = b62author
    assert b62author = Baobab.b62identity("~" <> short)

    assert Baobab.full_log(b62author) |> Enum.count() == 2
    assert Baobab.full_log(b62author, log_id: 1) == [other_root]

    for n <- 3..14 do
      assert %Baobab.Entry{seqnum: ^n, log_id: 0} =
               Baobab.append_log("Entry: " <> Integer.to_string(n), "testy")
    end

    author_key = Baobab.identity_key("testy", :public)
    partial = Baobab.log_at(b62author, 5, format: :binary)
    assert Enum.count(partial) == 8
    latest = Baobab.log_at(author_key, :max, revalidate: true)
    assert Enum.count(latest) == 4
    full = Baobab.full_log(author_key, log_id: 0)
    assert Enum.count(full) == 14
    assert %Baobab.Entry{payload: "Entry: 6"} = Baobab.log_entry(author_key, 6)

    assert [^root | _] = Baobab.import(partial)
    assert [^root | _] = latest
    assert [^root | _] = full

    assert Baobab.max_seqnum("testy", log_id: 0) == 14
    assert Baobab.max_seqnum("testy", log_id: 1) == 1

    assert [{^b62author, 0, 14}, {^b62author, 1, 1}] = Baobab.stored_info()
    assert Baobab.log_range(b62author, {2, 14}) |> length() == 5

    assert [
             {:ok, 2},
             {:ok, 3},
             {:ok, 5},
             {:ok, 6},
             {:ok, 7},
             {:ok, 8},
             {:ok, 9},
             {:ok, 10},
             {:ok, 11},
             {:ok, 12}
           ] = Baobab.compact("testy")

    assert {:error, :missing} = Baobab.log_entry("testy", 2)
    assert [{^b62author, 0, 14}, {^b62author, 1, 1}] = Baobab.stored_info()

    assert Baobab.log_range(b62author, {2, 14}) |> length() == 3

    assert :ok == Baobab.drop_identity("testy")
    assert :error == Baobab.identity_key("testy", :public)
  end

  test "errors or not" do
    assert :error = Baobab.identity_key("newb", :secret)
    assert :error = Baobab.identity_key("newb", :public)

    assert {:error, "Unknown identity: ~short"} = Baobab.b62identity("~short")

    assert [{:error, "Import requires a list of Baobab.Entry structs"}] = Baobab.import("")
    assert [{:error, "Truncated binary cannot be reified"}] = Baobab.import([""])

    assert [] = Baobab.log_at("0123456789ABCDEF0123456789ABCDEF", 5)
    assert [] = Baobab.log_at("0123456789ABCDEF0123456789ABCDEF0123456789A", 5)
  end

  test "purgeitory" do
    b62first = Baobab.create_identity("first")
    b62second = Baobab.create_identity("second")

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
end
