defmodule AeMdw.RocksdbManager do
  import AeMdw.Sigil

  alias AeMdw.Db.Model

  use GenServer

  @tab __MODULE__

  def cf_handle!(tab) do
    [{^tab, {db_handle, cf_handle}}] = :ets.lookup(@tab, tab)
    {db_handle, cf_handle}
  end

  def db_handle!() do
    [{:db, db_handle}] = :ets.lookup(@tab, :db)
    db_handle
  end

  def clear_table(tab) do
    GenServer.call(__MODULE__, {:clear_table, tab})
  end

  def start_link(_),
    do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  def init([]) do
    opts = [:set, :public, :named_table, {:keypos, 1}]
    :ets.new(@tab, opts)
    open_rocksdb()
    {:ok, []}
  end

  defp open_rocksdb do
    opts = [create_if_missing: true, create_missing_column_families: true]
    tabs = Model.tables()
    table_opts = Enum.map(tabs, fn tab -> {Atom.to_charlist(tab), []} end)
    {:ok, db, [default | tab_handles]} =
      :rocksdb.open_optimistic_transaction_db('data', opts, [{'default', []}|table_opts])
    :ets.insert(@tab, {:db, db})

    true = Enum.count(tabs) == Enum.count(tab_handles)
    Enum.zip(tabs, tab_handles)
    |> Enum.each(fn {tab,cf_handle} -> :ets.insert(@tab, {tab, {db, cf_handle}}) end)
  end

  def handle_call({:clear_table, tab}, _from, state) do
    {db, cf} = cf_handle!(tab)
    :ok = :rocksdb.drop_column_family(db, cf)
    name = Atom.to_charlist(tab)
    {:ok, new_cf} = :rocksdb.create_column_family(db, name, [])
    true = result = :ets.insert(@tab, {tab, {db, new_cf}})
    {:reply, :ok, state}
  end

end
