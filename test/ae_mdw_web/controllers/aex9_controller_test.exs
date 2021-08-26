defmodule AeMdwWeb.Aex9ControllerTest do
  use AeMdwWeb.ConnCase, async: false

  alias AeMdwWeb.Samples

  import Mock

  describe "balance_for_hash" do
    test "gets balance for hash", %{conn: conn} do
      mb_hash = "mh_2NkfQ9p29EQtqL6YQAuLpneTRPxEKspNYLKXeexZ664ZJo7fcw"
      contract_id = "ct_RDRJC5EySx4TcLtGRWYrXfNgyWzEDzssThJYPd9kdLeS5ECaA"
      account_id = "ak_Yc8Lr64xGiBJfm2Jo8RQpR1gwTY8KMqqXk8oWiVC9esG8ce48"

      with_mocks [
        {:aec_chain, [:passthrough],
         [
           get_block: fn _hash -> {:ok, Samples.micro_block()} end,
           get_contract: fn _hash -> {:ok, Samples.contract()} end
         ]},
        {AeMdw.Node.Db, [],
         [
           aex9_balance: fn _contract_pk, _account_pk, {type, height, hash} ->
             {1_234, {type, height, hash}}
           end
         ]}
      ] do
        conn = get(conn, "/aex9/balance/hash/#{mb_hash}/#{contract_id}/#{account_id}")

        assert %{
                 "account_id" => "ak_Yc8Lr64xGiBJfm2Jo8RQpR1gwTY8KMqqXk8oWiVC9esG8ce48",
                 "amount" => 1_234,
                 "block_hash" => ^mb_hash,
                 "contract_id" => ^contract_id,
                 "height" => 350_622
               } = json_response(conn, 200)
      end
    end
  end

  describe "balances_for_hash" do
    test "gets balances for hash", %{conn: conn} do
      mb_hash = "mh_2NkfQ9p29EQtqL6YQAuLpneTRPxEKspNYLKXeexZ664ZJo7fcw"
      contract_id = "ct_RDRJC5EySx4TcLtGRWYrXfNgyWzEDzssThJYPd9kdLeS5ECaA"

      amounts = [
        {{:address,
          <<177, 191, 60, 62, 170, 226, 235, 50, 0, 188, 192, 70, 225, 195, 109, 9, 109, 86, 44,
            212, 245, 42, 113, 172, 181, 142, 36, 109, 214, 99, 30, 188>>}, 4_050_000_000_000},
        {{:address,
          <<201, 218, 178, 202, 42, 163, 173, 199, 75, 135, 70, 236, 182, 248, 91, 5, 103, 254,
            75, 221, 230, 170, 8, 217, 169, 45, 203, 209, 116, 71, 244, 50>>}, 8_100_000_000_000},
        {{:address,
          <<25, 210, 178, 96, 161, 13, 112, 123, 62, 19, 154, 103, 13, 4, 10, 83, 72, 244, 191,
            239, 182, 227, 199, 24, 145, 91, 19, 25, 222, 221, 238, 152>>}, 81_000_000_000_000},
        {{:address,
          <<71, 195, 179, 19, 220, 26, 43, 176, 31, 41, 88, 252, 104, 135, 97, 181, 140, 9, 200,
            113, 149, 46, 35, 130, 238, 130, 165, 201, 28, 0, 205, 200>>},
         49_999_999_999_906_850_000_000_000}
      ]

      with_mocks [
        {:aec_chain, [:passthrough],
         [
           get_block: fn _hash -> {:ok, Samples.micro_block()} end,
           get_contract: fn _hash -> {:ok, Samples.contract()} end
         ]},
        {AeMdw.Node.Db, [],
         [
           aex9_balances: fn _contract_pk, {type, height, hash} ->
             {amounts, {type, height, hash}}
           end
         ]}
      ] do
        conn = get(conn, "/aex9/balances/hash/#{mb_hash}/#{contract_id}")

        assert %{
                 "amounts" => %{
                   "ak_2MHJv6JcdcfpNvu4wRDZXWzq8QSxGbhUfhMLR7vUPzRFYsDFw6" => 4_050_000_000_000,
                   "ak_2Xu6d6W4UJBWyvBVJQRHASbQHQ1vjBA7d1XUeY8SwwgzssZVHK" => 8_100_000_000_000,
                   "ak_CNcf2oywqbgmVg3FfKdbHQJfB959wrVwqfzSpdWVKZnep7nj4" => 81_000_000_000_000,
                   "ak_Yc8Lr64xGiBJfm2Jo8RQpR1gwTY8KMqqXk8oWiVC9esG8ce48" =>
                     49_999_999_999_906_850_000_000_000
                 },
                 "block_hash" => ^mb_hash,
                 "contract_id" => ^contract_id,
                 "height" => 350_622
               } = json_response(conn, 200)
      end
    end
  end
end
