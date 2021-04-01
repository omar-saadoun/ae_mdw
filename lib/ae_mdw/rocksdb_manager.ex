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
    {:ok, db, [default, block, tx, type, time,
               active_name_expiration, auction_expiration,
              ]} =
      :rocksdb.open('data', opts, [{'default', []},
                                   {'block', []},
                                   {'tx', []},
                                   {'type', []},
                                   {'time', []},
                                   {'active_name_expiration', []},
                                   {'auction_expiration', []},
                                  ])
    :ets.insert(@tab, {:db, db})
    :ets.insert(@tab, {~t[block], {db, block}})
    :ets.insert(@tab, {~t[tx],    {db, tx}})
    :ets.insert(@tab, {~t[type],  {db, type}})
    :ets.insert(@tab, {~t[time],  {db, time}})
    :ets.insert(@tab, {Model.ActiveNameExpiration,  {db, active_name_expiration}})
    :ets.insert(@tab, {Model.AuctionExpiration,  {db, auction_expiration}})
  end

end
