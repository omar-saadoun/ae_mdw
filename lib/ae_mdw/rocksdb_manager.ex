defmodule AeMdw.RocksdbManager do
  use GenServer

  @tab __MODULE__

  def cf_handle!(cf) do
    [{^cf, {db_handle, cf_handle}}] = :ets.lookup(@tab, cf)
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
    {:ok, db, [cf_default, cf_block]} =
      :rocksdb.open('data', opts, [{'default', []}, {'block', []}])
    :ets.insert(@tab, {:db, db})
    :ets.insert(@tab, {:block, {db, cf_block}})
  end

end
