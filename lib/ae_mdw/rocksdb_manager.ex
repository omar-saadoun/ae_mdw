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
      :rocksdb.open('data', opts, [{'default', []}|table_opts])
    :ets.insert(@tab, {:db, db})

    true = Enum.count(tabs) == Enum.count(tab_handles)
    Enum.zip(tabs, tab_handles)
    |> Enum.each(fn {tab,cf_handle} -> :ets.insert(@tab, {tab, {db, cf_handle}}) end)
  end

end
