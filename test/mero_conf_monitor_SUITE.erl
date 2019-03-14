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
-module(mero_conf_monitor_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    all/0,
    init_per_testcase/2,
    end_per_testcase/2
]).
-export([
    conf_is_periodically_fetched/1,
    cluster_is_restarted_when_new_nodes/1
]).

all() -> [
    conf_is_periodically_fetched,
    cluster_is_restarted_when_new_nodes
].

init_per_testcase(_, Conf) ->
    meck:new([mero_elasticache, mero_wrk_tcp_binary], [passthrough]),
    HostLinea = <<"a1.com|10.100.100.100|11211 ",
        "a2.com|10.101.101.00|11211 ",
        "a3.com|10.102.00.102|11211\n">>,

    HostLineb = <<"b1.com|10.100.100.100|22122 ",
        "b2.com|10.101.101.00|22122\n">>,

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
            [{banner, <<"CONFIG cluster ...">>},
                {version, <<"version1">>},
                {hosts, HostLines},
                {crlf, <<"\r\n">>},
                {eom, <<"END\r\n">>}]
        end),

    meck:expect(mero_wrk_tcp_binary, connect,
        fun(_Host, Port, CallbackInfo) ->
            meck:passthrough(["localhost", Port, CallbackInfo])
        end),

    application:load(mero),
    Conf.

end_per_testcase(_, _Conf) ->
    application:stop(mero),
    meck:unload([mero_elasticache, mero_wrk_tcp_binary]).


conf_is_periodically_fetched(_) ->
    mero_conf:monitor_heartbeat_delay(10, 11),
    start_server(),
    timer:sleep(20),
    ?assert(meck:called(mero_elasticache, get_cluster_config, 2)),
    meck:reset(mero_elasticache),
    timer:sleep(20),
    ?assert(meck:called(mero_elasticache, get_cluster_config, 2)).

cluster_is_restarted_when_new_nodes(_) ->
    mero_conf:monitor_heartbeat_delay(10000, 10001),
    start_server(),
    % ct:pal("Cluster 1: ~p", [supervisor:which_children(mero_cluster:sup_by_cluster_name(cluster1))]),
    % ct:pal("Cluster 2: ~p", [supervisor:which_children(mero_cluster:sup_by_cluster_name(cluster2))]),
    % ?assert(3, supervisor:count_children(mero_cluster:sup_by_cluster_name(cluster1))).
    ok.

start_server() ->
    mero_test_util:start_server(cluster_config(), 5, 30, 1000, 5000).

cluster_config() ->
    [
        {cluster1, [
            {servers,               {elasticache, [{a, 11211, 1}]}},
            {sharding_algorithm,    {mero, shard_crc32}},
            {workers_per_shard,     1},
            {pool_worker_module,    mero_wrk_tcp_binary}
        ]},
        {cluster2, [
            {servers,               {elasticache, [{b, 22122, 1}]}},
            {sharding_algorithm,    {mero, shard_crc32}},
            {workers_per_shard,     1},
            {pool_worker_module,    mero_wrk_tcp_binary}
        ]}
    ].
