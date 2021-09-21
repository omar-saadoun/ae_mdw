defmodule AeMdwWeb.Helpers.Aex9Helper do
  @moduledoc """
  Used to format aex9 related info
  """
  def normalize_balances(bals) do
    for {{:address, pk}, amt} <- bals, reduce: %{} do
      acc ->
        Map.put(acc, enc_id(pk), amt)
    end
  end

  def enc_block(:key, hash), do: :aeser_api_encoder.encode(:key_block_hash, hash)
  def enc_block(:micro, hash), do: :aeser_api_encoder.encode(:micro_block_hash, hash)

  def enc_ct(pk), do: :aeser_api_encoder.encode(:contract_pubkey, pk)
  def enc_id(pk), do: :aeser_api_encoder.encode(:account_pubkey, pk)

  def enc(type, pk), do: :aeser_api_encoder.encode(type, pk)
end
