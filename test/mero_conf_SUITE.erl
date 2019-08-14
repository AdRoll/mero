%% Copyright (c) 2014, AdRoll
%% All rights reserved.
%%
%% Redistribution and use in source and binary forms, with or without
%% modification, are permitted provided that the following conditions are met:
%%
%% * Redistributions of source code must retain the above copyright notice, this
%% list of conditions and the following disclaimer.
%%
%% * Redistributions in binary form must reproduce the above copyright notice,
%% this list of conditions and the following disclaimer in the documentation
%% and/or other materials provided with the distribution.
%%
%% * Neither the name of the {organization} nor the names of its
%% contributors may be used to endorse or promote products derived from
%% this software without specific prior written permission.
%%
%% THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
%% AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
%% IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
%% DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
%% FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
%% DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
%% SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
%% CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
%% OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
%% OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
%%
-module(mero_conf_SUITE).

-author('Miriam Pena <miriam.pena@adroll.com>').

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    all/0,
    init_per_testcase/2,
    end_per_testcase/2,
    helper_mfa_config_function/0,
    diff/1,
    process_server_specs_a_compatible/1,
    process_server_specs_a/1,
    process_server_specs_a_alternate/1,
    process_server_specs_a_b/1,
    process_server_specs_a_b_c/1,
    process_server_specs_mfa/1,
    per_pool_config/1
]).

all() -> [
    diff,
    process_server_specs_a_compatible,
    process_server_specs_a,
    process_server_specs_a_alternate,
    process_server_specs_a_b,
    process_server_specs_a_b_c,
    process_server_specs_mfa,
    per_pool_config
].

init_per_testcase(diff, Conf) ->
    Conf;
init_per_testcase(_, Conf) ->
    meck:new(mero_elasticache, [passthrough]),
    HostLinea = <<"a1.com|10.100.100.100|11211 ",
        "a2.com|10.101.101.00|11211 ",
        "a3.com|10.102.00.102|11211\n">>,

    HostLineb = <<"b1.com|10.100.100.100|11211 ",
        "b2.com|10.101.101.00|11211\n">>,

    HostLinec = <<"c1.com|10.100.100.100|11211 ",
        "c2.com|10.101.101.00|11211 ",
        "c3.com|10.102.00.102|11211 c4.com|10.102.00.102|11211\n">>,

    meck:expect(mero_elasticache, request_response,
        fun(Type, _, _, _) ->
            HostLines = case Type of
                            a -> HostLinea;
                            b -> HostLineb;
                            c -> HostLinec
                        end,
            {
                ok,
                [
                    {banner, <<"CONFIG cluster ...">>},
                    {version, <<"version1">>},
                    {hosts, HostLines},
                    {crlf, <<"\r\n">>},
                    {eom, <<"END\r\n">>}
                ]
            }
        end),
    Conf.

end_per_testcase(diff, _Conf) ->
    ok;
end_per_testcase(_, _Conf) ->
    dbg:stop_clear(),
    meck:unload([mero_elasticache]),
    ok.

helper_mfa_config_function() ->
    {ok, [{"mfa1.com", 11211}, {"mfa2.com", 11211}]}.

%%%=============================================================================
%%% Tests
%%%=============================================================================

diff(_Conf) ->
    Now = {1393, 871004, 818836},
    Diff = 5000,
    Then = mero_conf:add_now(Diff, Now),
    ?assertMatch(Now, mero_conf:add_now(0, Now)),
    ct:log("Then ~p Now  ~p Diff~p", [Then, Now, timer:now_diff(Then, Now)]),
    ?assertMatch(Diff, mero_conf:millis_to(Then, Now)).

process_server_specs_a(_Conf) ->
    Spec = [{default,
        [{servers, {elasticache, [{a, 11211, 2}]}},
            {sharding_algorithm, {mero, shard_crc32}},
            {workers_per_shard, 1},
            {pool_worker_module, mero_wrk_tcp_binary}]}],

    [{default, ServerSpecs}] = mero_conf:process_server_specs(Spec),
    ?assertEqual([
        {"a1.com", 11211}, {"a2.com", 11211}, {"a3.com", 11211},
        {"a1.com", 11211}, {"a2.com", 11211}, {"a3.com", 11211}],
        proplists:get_value(servers, ServerSpecs)),
    ?assertEqual(mero_wrk_tcp_binary, proplists:get_value(pool_worker_module, ServerSpecs)),
    ?assertEqual(1, proplists:get_value(workers_per_shard, ServerSpecs)),
    ?assertEqual({mero, shard_crc32}, proplists:get_value(sharding_algorithm, ServerSpecs)),
    ok.

process_server_specs_a_alternate(_Conf) ->
    Spec = [{default,
        [{servers, {elasticache, [{a, 11211}]}},
            {sharding_algorithm, {mero, shard_crc32}},
            {workers_per_shard, 1},
            {pool_worker_module, mero_wrk_tcp_binary}]}],

    [{default, ServerSpecs}] = mero_conf:process_server_specs(Spec),
    ?assertEqual([
        {"a1.com", 11211}, {"a2.com", 11211}, {"a3.com", 11211}],
        proplists:get_value(servers, ServerSpecs)),
    ?assertEqual(mero_wrk_tcp_binary, proplists:get_value(pool_worker_module, ServerSpecs)),
    ?assertEqual(1, proplists:get_value(workers_per_shard, ServerSpecs)),
    ?assertEqual({mero, shard_crc32}, proplists:get_value(sharding_algorithm, ServerSpecs)),
    ok.

process_server_specs_a_compatible(_Conf) ->
    Spec = [{default,
        [{servers, {elasticache, a, 11211}},
            {sharding_algorithm, {mero, shard_crc32}},
            {workers_per_shard, 1},
            {pool_worker_module, mero_wrk_tcp_binary}]}],

    [{default, ServerSpecs}] = mero_conf:process_server_specs(Spec),
    ?assertEqual([{"a1.com", 11211}, {"a2.com", 11211}, {"a3.com", 11211}],
        proplists:get_value(servers, ServerSpecs)),
    ?assertEqual(mero_wrk_tcp_binary, proplists:get_value(pool_worker_module, ServerSpecs)),
    ?assertEqual(1, proplists:get_value(workers_per_shard, ServerSpecs)),
    ?assertEqual({mero, shard_crc32}, proplists:get_value(sharding_algorithm, ServerSpecs)),
    ok.

process_server_specs_a_b(_Conf) ->
    Spec = [{default,
        [{servers, {elasticache, [{a, 11211, 1}, {b, 11211, 2}]}},
            {sharding_algorithm, {mero, shard_crc32}},
            {workers_per_shard, 2},
            {pool_worker_module, mero_wrk_tcp_txt}]}],

    [{default, ServerSpecs}] = mero_conf:process_server_specs(Spec),
    ?assertEqual([
        {"a1.com", 11211}, {"a2.com", 11211}, {"a3.com", 11211},
        {"b1.com", 11211}, {"b2.com", 11211},
        {"b1.com", 11211}, {"b2.com", 11211}],
        proplists:get_value(servers, ServerSpecs)),
    ?assertEqual(mero_wrk_tcp_txt, proplists:get_value(pool_worker_module, ServerSpecs)),
    ?assertEqual(2, proplists:get_value(workers_per_shard, ServerSpecs)),
    ?assertEqual({mero, shard_crc32}, proplists:get_value(sharding_algorithm, ServerSpecs)),
    ok.

process_server_specs_a_b_c(_Conf) ->
    Spec = [{default,
        [{servers, {elasticache, [{a, 11211, 1}, {b, 11211, 2}, {c, 11211, 4}]}},
            {sharding_algorithm, {mero, shard_crc32}},
            {workers_per_shard, 20},
            {pool_worker_module, mero_wrk_tcp_txt}]}],

    [{default, ServerSpecs}] = mero_conf:process_server_specs(Spec),
    ?assertEqual([
        {"a1.com", 11211}, {"a2.com", 11211}, {"a3.com", 11211},
        {"b1.com", 11211}, {"b2.com", 11211},
        {"b1.com", 11211}, {"b2.com", 11211},
        {"c1.com", 11211}, {"c2.com", 11211}, {"c3.com", 11211}, {"c4.com", 11211},
        {"c1.com", 11211}, {"c2.com", 11211}, {"c3.com", 11211}, {"c4.com", 11211},
        {"c1.com", 11211}, {"c2.com", 11211}, {"c3.com", 11211}, {"c4.com", 11211},
        {"c1.com", 11211}, {"c2.com", 11211}, {"c3.com", 11211}, {"c4.com", 11211}
        ],
        proplists:get_value(servers, ServerSpecs)),
    ?assertEqual(mero_wrk_tcp_txt, proplists:get_value(pool_worker_module, ServerSpecs)),
    ?assertEqual(20, proplists:get_value(workers_per_shard, ServerSpecs)),
    ?assertEqual({mero, shard_crc32}, proplists:get_value(sharding_algorithm, ServerSpecs)),
    ok.

process_server_specs_mfa(_Conf) ->
    Spec = [{default,
        [{servers, {mfa, {?MODULE, helper_mfa_config_function, []}}},
            {sharding_algorithm, {mero, shard_crc32}},
            {workers_per_shard, 20},
            {pool_worker_module, mero_wrk_tcp_txt}]}],
    [{default, ServerSpecs}] = mero_conf:process_server_specs(Spec),
    ?assertEqual([{"mfa1.com", 11211}, {"mfa2.com", 11211}],
        proplists:get_value(servers, ServerSpecs)),
    ?assertEqual(mero_wrk_tcp_txt, proplists:get_value(pool_worker_module, ServerSpecs)),
    ?assertEqual(20, proplists:get_value(workers_per_shard, ServerSpecs)),
    ?assertEqual({mero, shard_crc32}, proplists:get_value(sharding_algorithm, ServerSpecs)),
    ok.

per_pool_config(_Conf) ->
    mero_conf:initial_connections_per_pool({by_pool, 20, #{pool_1 => 30, pool_2 => 50}}),
    ?assertEqual(20, mero_conf:pool_initial_connections(pool_3)),
    ?assertEqual(30, mero_conf:pool_initial_connections(pool_1)),
    ?assertEqual(50, mero_conf:pool_initial_connections(pool_2)),
    ok.
