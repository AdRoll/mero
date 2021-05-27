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

-behaviour(ct_suite).

-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([conf_is_periodically_fetched/1, cluster_is_restarted_when_new_nodes/1,
         cluster_is_restarted_when_lost_nodes/1, cluster_is_not_restarted_when_other_changes/1,
         cluster_is_not_restarted_with_bad_info/1, cluster_is_not_restarted_on_socket_error/1,
         non_heartbeat_messages_are_ignored/1]).

all() ->
    [conf_is_periodically_fetched,
     cluster_is_restarted_when_new_nodes,
     cluster_is_restarted_when_lost_nodes,
     cluster_is_not_restarted_when_other_changes,
     cluster_is_not_restarted_with_bad_info,
     cluster_is_not_restarted_on_socket_error,
     non_heartbeat_messages_are_ignored].

init_per_testcase(_, Conf) ->
    meck:new([mero_elasticache, mero_wrk_tcp_binary], [passthrough, no_link]),
    HostLinea =
        <<"a1.com|10.100.100.100|11112 ",
          "a2.com|10.101.101.00|11112 ",
          "a3.com|10.102.00.102|11112\n">>,
    HostLineb = <<"b1.com|10.100.100.100|11212 ", "b2.com|10.101.101.00|11212\n">>,
    HostLinec =
        <<"c1.com|10.100.100.100|11112 ",
          "c2.com|10.101.101.00|11112 ",
          "c3.com|10.102.00.102|11112 c4.com|10.102.00.102|11112\n">>,
    Lines =
        #{a => HostLinea,
          b => HostLineb,
          c => HostLinec},
    mock_elasticache(Lines),

    meck:expect(mero_wrk_tcp_binary,
                connect,
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

    Cluster1Children =
        supervisor:which_children(
            mero_cluster:sup_by_cluster_name(cluster1)),
    Cluster2Children =
        supervisor:which_children(
            mero_cluster:sup_by_cluster_name(cluster2)),
    ?assertEqual(3, length(Cluster1Children)),
    ?assertEqual(2, length(Cluster2Children)),

    %% Nothing Changed...
    send_heartbeat(),
    ?assertEqual(Cluster1Children,
                 supervisor:which_children(
                     mero_cluster:sup_by_cluster_name(cluster1))),
    ?assertEqual(Cluster2Children,
                 supervisor:which_children(
                     mero_cluster:sup_by_cluster_name(cluster2))),
    %% Cluster1 stays, Cluster2 adds a node
    Lines =
        #{a =>
              <<"a1.com|10.100.100.100|11112 ",
                "a2.com|10.101.101.00|11112 ",
                "a3.com|10.102.00.102|11112\n">>,
          b =>
              <<"b1.com|10.100.100.100|11212 ",
                "b2.com|10.101.101.00|11212 b3.com|10.102.00.102|11212\n">>},
    mock_elasticache(Lines),

    %% Cluster1 remains the same, Cluster2 is rebuilt
    send_heartbeat(),
    ?assertEqual(Cluster1Children,
                 supervisor:which_children(
                     mero_cluster:sup_by_cluster_name(cluster1))),
    NewCluster2Children =
        supervisor:which_children(
            mero_cluster:sup_by_cluster_name(cluster2)),
    ?assertEqual(3, length(NewCluster2Children)),
    lists:foreach(fun(Child) -> ?assertNot(lists:member(Child, Cluster2Children)) end,
                  NewCluster2Children),
    ok.

cluster_is_restarted_when_lost_nodes(_) ->
    mero_conf:monitor_heartbeat_delay(10000, 10001),
    start_server(),

    Cluster1Children =
        supervisor:which_children(
            mero_cluster:sup_by_cluster_name(cluster1)),
    Cluster2Children =
        supervisor:which_children(
            mero_cluster:sup_by_cluster_name(cluster2)),
    ?assertEqual(3, length(Cluster1Children)),
    ?assertEqual(2, length(Cluster2Children)),

    %% Nothing Changed...
    send_heartbeat(),
    ?assertEqual(Cluster1Children,
                 supervisor:which_children(
                     mero_cluster:sup_by_cluster_name(cluster1))),
    ?assertEqual(Cluster2Children,
                 supervisor:which_children(
                     mero_cluster:sup_by_cluster_name(cluster2))),
    %% Cluster2 stays, Cluster1 lost a node
    Lines =
        #{a => <<"a1.com|10.100.100.100|11112 ", "a2.com|10.101.101.00|11112\n">>,
          b => <<"b1.com|10.100.100.100|11212 ", "b2.com|10.101.101.00|11212\n">>},
    mock_elasticache(Lines),

    %% Cluster1 remains the same, Cluster2 is rebuilt
    send_heartbeat(),
    NewCluster1Children =
        supervisor:which_children(
            mero_cluster:sup_by_cluster_name(cluster1)),
    ?assertEqual(2, length(NewCluster1Children)),
    lists:foreach(fun(Child) -> ?assertNot(lists:member(Child, Cluster1Children)) end,
                  NewCluster1Children),
    ?assertEqual(Cluster2Children,
                 supervisor:which_children(
                     mero_cluster:sup_by_cluster_name(cluster2))),
    ok.

cluster_is_not_restarted_when_other_changes(_) ->
    mero_conf:monitor_heartbeat_delay(10000, 10001),
    start_server(),

    Cluster1Children =
        supervisor:which_children(
            mero_cluster:sup_by_cluster_name(cluster1)),
    Cluster2Children =
        supervisor:which_children(
            mero_cluster:sup_by_cluster_name(cluster2)),
    ?assertEqual(3, length(Cluster1Children)),
    ?assertEqual(2, length(Cluster2Children)),

    %% Nothing Changed...
    send_heartbeat(),
    ?assertEqual(Cluster1Children,
                 supervisor:which_children(
                     mero_cluster:sup_by_cluster_name(cluster1))),
    ?assertEqual(Cluster2Children,
                 supervisor:which_children(
                     mero_cluster:sup_by_cluster_name(cluster2))),
    %% servers are reordered in both clusters, but that's irrelevant for us
    Lines =
        #{a =>
              <<"a2.com|10.101.101.00|11112 ",
                "a1.com|10.100.100.100|11112 ",
                "a3.com|10.102.00.102|11112\n">>,
          b => <<"b2.com|10.101.101.00|11212 ", "b1.com|10.100.100.100|11212\n">>},
    mock_elasticache(Lines),

    %% Nothing Changed...
    send_heartbeat(),
    ?assertEqual(Cluster1Children,
                 supervisor:which_children(
                     mero_cluster:sup_by_cluster_name(cluster1))),
    ?assertEqual(Cluster2Children,
                 supervisor:which_children(
                     mero_cluster:sup_by_cluster_name(cluster2))),
    ok.

cluster_is_not_restarted_with_bad_info(_) ->
    mero_conf:monitor_heartbeat_delay(10000, 10001),
    start_server(),

    Cluster1Children =
        supervisor:which_children(
            mero_cluster:sup_by_cluster_name(cluster1)),
    Cluster2Children =
        supervisor:which_children(
            mero_cluster:sup_by_cluster_name(cluster2)),
    ?assertEqual(3, length(Cluster1Children)),
    ?assertEqual(2, length(Cluster2Children)),

    %% Nothing Changed...
    send_heartbeat(),
    ?assertEqual(Cluster1Children,
                 supervisor:which_children(
                     mero_cluster:sup_by_cluster_name(cluster1))),
    ?assertEqual(Cluster2Children,
                 supervisor:which_children(
                     mero_cluster:sup_by_cluster_name(cluster2))),
    %% bad info is received
    Lines = #{a => <<"this is wrong\n">>},
    mock_elasticache(Lines),

    %% Nothing Changed...
    send_heartbeat(),
    ?assertEqual(Cluster1Children,
                 supervisor:which_children(
                     mero_cluster:sup_by_cluster_name(cluster1))),
    ?assertEqual(Cluster2Children,
                 supervisor:which_children(
                     mero_cluster:sup_by_cluster_name(cluster2))),
    ?assertNotEqual(undefined, whereis(mero_conf_monitor)),
    ok.

cluster_is_not_restarted_on_socket_error(_) ->
    mero_conf:monitor_heartbeat_delay(10000, 10001),
    start_server(),

    Cluster1Children =
        supervisor:which_children(
            mero_cluster:sup_by_cluster_name(cluster1)),
    Cluster2Children =
        supervisor:which_children(
            mero_cluster:sup_by_cluster_name(cluster2)),
    ?assertEqual(3, length(Cluster1Children)),
    ?assertEqual(2, length(Cluster2Children)),

    %% Nothing Changed...
    send_heartbeat(),
    ?assertEqual(Cluster1Children,
                 supervisor:which_children(
                     mero_cluster:sup_by_cluster_name(cluster1))),
    ?assertEqual(Cluster2Children,
                 supervisor:which_children(
                     mero_cluster:sup_by_cluster_name(cluster2))),
    %% socket times out when connecting to elasticache
    mock_elasticache_timeout(),

    %% Nothing Changed...
    send_heartbeat(),
    ?assertEqual(Cluster1Children,
                 supervisor:which_children(
                     mero_cluster:sup_by_cluster_name(cluster1))),
    ?assertEqual(Cluster2Children,
                 supervisor:which_children(
                     mero_cluster:sup_by_cluster_name(cluster2))),
    ?assertNotEqual(undefined, whereis(mero_conf_monitor)),
    ok.

non_heartbeat_messages_are_ignored(_) ->
    start_server(),

    MeroConfMonitor = whereis(mero_conf_monitor),

    ok = gen_server:cast(mero_conf_monitor, something),
    mero_conf_monitor ! something_else,
    send_heartbeat(),

    ?assertEqual(MeroConfMonitor, whereis(mero_conf_monitor)),
    ok.

start_server() ->
    mero_test_util:start_server(cluster_config(), 5, 30, 1000, 5000).

cluster_config() ->
    [{cluster1,
      [{servers, {elasticache, [{a, 11112, 1}]}},
       {sharding_algorithm, {mero, shard_crc32}},
       {workers_per_shard, 1},
       {pool_worker_module, mero_wrk_tcp_binary}]},
     {cluster2,
      [{servers, {elasticache, [{b, 11112, 1}]}},
       {sharding_algorithm, {mero, shard_crc32}},
       {workers_per_shard, 1},
       {pool_worker_module, mero_wrk_tcp_binary}]}].

send_heartbeat() ->
    mero_conf_monitor ! heartbeat,
    {unknown_call, sync} = gen_server:call(mero_conf_monitor, sync).

mock_elasticache(Lines) ->
    meck:expect(mero_elasticache,
                request_response,
                fun(Type, _, _, _) ->
                   HostLines = maps:get(Type, Lines),
                   {ok,
                    [{banner, <<"CONFIG cluster ...">>},
                     {version, <<"version1">>},
                     {hosts, HostLines},
                     {crlf, <<"\r\n">>},
                     {eom, <<"END\r\n">>}]}
                end).

mock_elasticache_timeout() ->
    meck:expect(mero_elasticache, request_response, 4, {error, etimedout}).
