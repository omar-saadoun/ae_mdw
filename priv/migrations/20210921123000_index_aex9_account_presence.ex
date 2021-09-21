defmodule AeMdw.Migrations.IndexAex9AccountPresence do
  @moduledoc """
  Indexes missing Aex9AccountPresence based on contract raw logs.
  """
  alias AeMdw.Application, as: MdwApp
  alias AeMdw.Node, as: AE
  alias AeMdw.Node.Db, as: DBN

  alias AeMdw.Contract
  alias AeMdw.Db.Contract, as: DbContract
  alias AeMdw.Db.Format
  alias AeMdw.Db.Model
  alias AeMdw.Db.Util
  alias AeMdw.Validate
  alias AeMdwWeb.Helpers.Aex9Helper
  alias AeMdwWeb.Views.Aex9ControllerView
  # alias AeMdw.Db.Sync.Transaction, as: SyncTx
  alias AeMdw.Sync.Supervisor, as: SyncSup

  require Model
  require Ex2ms

  defmodule Account2Fix do
    defstruct [:account_pk, :contract_pk, :since_height]
  end

  @doc """

  """
  @spec run() :: {:ok, {pos_integer(), pos_integer()}}
  def run do
    begin = DateTime.utc_now()

    if :ok != Application.ensure_started(:ae_mdw) do
      IO.puts("Ensure sync tables...")
      SyncSup.init_tables()
      MdwApp.init(:contract_cache)
      MdwApp.init(:db_state)
    end

    indexed_count =
      fetch_aex9_contracts()
      |> accounts_without_balance(60, 30)
      |> reindex_account_aex9_txs()

    duration = DateTime.diff(DateTime.utc_now(), begin)
    IO.puts("Indexed #{indexed_count} records in #{duration}s")

    {:ok, {indexed_count, duration}}
  end

  defp reindex_account_aex9_txs(account_list) do
    account_list
    |> Enum.reduce(0, fn %Account2Fix{
                           since_height: since_height
                         } = account2fix,
                         acc ->
      {_key_block, micro_blocks} = AE.Db.get_blocks(since_height)
      acc + write_aex9_records_on_match(micro_blocks, account2fix)
    end)
  end

  defp write_aex9_records_on_match(micro_blocks, %Account2Fix{
         account_pk: account_pk,
         contract_pk: contract_pk,
         since_height: since_height
       }) do
    Enum.reduce(micro_blocks, 0, fn mblock, total_count_acc ->
      transfer_count =
        mblock
        |> :aec_blocks.txs()
        |> Enum.map(fn signed_tx ->
          {mod, tx} = :aetx.specialize_callback(:aetx_sign.tx(signed_tx))
          {mod.type(), tx}
        end)
        |> Enum.filter(&tx_type_and_contract_match?(&1, contract_pk))
        |> Enum.reduce(0, fn {_type, tx}, acc ->
          # TODO: do the same for other calls
          raw_logs = contract_call_tx_logs(tx, contract_pk, since_height)

          txi_count =
            raw_logs
            |> find_txis_by_event_log(nil)
            |> Enum.map(fn txi ->
              case DbContract.write_aex9_records(contract_pk, raw_logs, txi, account_pk) do
                :ok ->
                  account_id = Aex9Helper.enc_id(account_pk)
                  contract_id = Aex9Helper.enc_ct(contract_pk)
                  IO.puts("Fixed account #{account_id} balance for #{contract_id}")
                  1

                _any ->
                  IO.puts("Failed to write")
                  0
              end
            end)
            |> Enum.sum()

          acc + txi_count
        end)

      total_count_acc + transfer_count
    end)
  end

  defp tx_type_and_contract_match?({type, tx}, account_contract_pk) do
    type == :contract_call_tx and
      :aect_call_tx.contract_pubkey(tx) == account_contract_pk
  end

  defp contract_call_tx_logs(tx, contract_pk, block_index) do
    block_hash = block_index |> Util.read_block!() |> Model.block(:hash)

    {_fun_arg_res, call_rec} =
      Contract.call_tx_info(tx, contract_pk, block_hash, &Contract.to_map/1)

    if contract_pk != :aect_call.contract_pubkey(call_rec),
      do: IO.inspect("CONTRACT DIFF")

    :aect_call.log(call_rec)
  end

  #
  # Since there are no records for id_int_contract_call,
  # taking for example `contract_pk = Validate.id!("ct_2bwK4mxEe3y9SazQRPXE8NdXikSTqF2T9FhNrawRzFA21yacTo")`,
  # to find the call_txi by contract pk
  #
  defp find_txis_by_event_log(raw_logs, create_txi) do
    transfer_evt_hash = AeMdw.Node.aex9_transfer_event_hash()

    raw_logs
    |> Enum.find(fn {_addr, [evt_hash | _args], data} ->
      if evt_hash == transfer_evt_hash do

        evt_spec =
          Ex2ms.fun do
            {:data_contract_log, {^data, call_txi, ^create_txi, ^evt_hash, :_}, :_} -> call_txi
          end

        case Util.select(Model.DataContractLog, evt_spec) do
          [] -> nil
          txi_list -> txi_list
        end
      end
    end)
  end

  defp fetch_aex9_contracts() do
    aex9_spec =
      Ex2ms.fun do
        {:aex9_contract, :_, :_} = record -> record
      end

    Model.Aex9Contract
    |> Util.select(aex9_spec)
    |> Enum.map(fn Model.aex9_contract(index: {_name, _symbol, txi, _decimals}) ->
      block_tx =
        txi
        |> Validate.nonneg_int!()
        |> Util.read_tx!()
        |> Format.to_map()

      contract_pk = block_tx |> get_in(["tx", "contract_id"]) |> Validate.id!([:contract_pubkey])
      {contract_pk, block_tx["block_height"]}
    end)
  end

  defp accounts_without_balance(contract_list, drop, take) do
    last_txi = Util.last_txi()

    contract_list
    |> Enum.drop(drop)
    |> Enum.take(take)
    |> Enum.map(fn {contract_pk, since_height} ->
      {amounts, _last_block_tuple} = DBN.aex9_balances(contract_pk)

      %{
        amounts: normalized_amounts(amounts),
        contract_pk: contract_pk,
        since_height: since_height
      }
    end)
    |> Enum.flat_map(fn %{amounts: amounts, contract_pk: contract_pk, since_height: since_height} ->
      amounts
      |> Map.keys()
      |> Enum.filter(fn account_id -> Map.get(amounts, account_id) > 0 end)
      |> Enum.map(fn account_id ->
        %Account2Fix{
          account_pk: AeMdw.Validate.id!(account_id, [:account_pubkey]),
          contract_pk: contract_pk,
          since_height: since_height
        }
      end)
    end)
    |> Enum.filter(fn %Account2Fix{account_pk: account_pk} ->
      account_balances_is_empty?(account_pk, last_txi)
    end)
  end

  defp normalized_amounts(amounts), do: Aex9Helper.normalize_balances(amounts)

  defp account_balances_is_empty?(account_pk, last_txi) do
    contracts =
      AeMdw.Db.Contract.aex9_search_contract(account_pk, last_txi)
      |> Map.to_list()
      |> Enum.sort_by(&elem(&1, 1), &<=/2)

    height_hash = DBN.top_height_hash(false)

    balance =
      contracts
      |> Enum.map(fn {contract_pk, txi} ->
        {amount, _} = DBN.aex9_balance(contract_pk, account_pk, height_hash)
        {amount, txi, contract_pk}
      end)
      |> Enum.map(&Aex9ControllerView.balance_to_map/1)

    Enum.empty?(balance)
  end
end
