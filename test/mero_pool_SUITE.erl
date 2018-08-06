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
-module(mero_pool_SUITE).

-author('Miriam Pena <miriam.pena@adroll.com>').

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(POOL, 'mero_cluster_localhost_0_0').
-define(PORT, 11999).
-define(TIMELIMIT(Timeout), mero_conf:add_now(Timeout)).
-define(CLUSTER_CONFIG,
    [{cluster,
        [{servers, [{"localhost", ?PORT}]},
         {sharding_algorithm, {mero, shard_phash2}},
         {workers_per_shard, 1},
         {pool_worker_module, mero_wrk_tcp_txt}]}
    ]).

all() -> [
             start_stop,
             start_stop_many,
             checkout_checkin,
             checkout_checkin_limits,
             checkout_checkin_closed,
             conn_failed_checkout_error,
             checkout_and_die,
             expire_connections,
             checkout_timeout
].


suite() -> [{timetrap, {seconds, 5}}].


init_per_testcase(_, Conf) ->
  application:load(mero),
  Conf.


end_per_testcase(_, _Conf) ->
  application:stop(mero).

%%%=============================================================================
%%% Tests
%%%=============================================================================

%% Just tests if the application can be started and when it does that
%% the mero_cluster module is generated correctly.
start_stop(_Conf) ->
  mero_test_util:start_server(?CLUSTER_CONFIG,  5, 30, 1000, 5000),
  ct:log("~p~n", [mero_cluster:child_definitions()]),
  ?assertMatch(
      [{cluster,
        [
         {links,6},
         {monitors,0},
         {free,5},
         {connected,5},
         {connecting,0},
         {failed,0},
         {message_queue_len,0}
        ]}
      ],
      mero:state()),

  ok = application:stop(mero),
  ok = application:unload(mero).


start_stop_many(_Conf) ->
  MinConn = 10,
  MaxConn = 400,

  ok = mero_conf:cluster_config(?CLUSTER_CONFIG),
  ok = mero_conf:min_free_connections_per_pool(MinConn),
  ok = mero_conf:initial_connections_per_pool(MinConn),
  ok = mero_conf:max_connections_per_pool(MaxConn),
  ok = mero_conf:expiration_interval(3000),
  ok = mero_conf:connection_unused_max_time(2000),
  ok = mero_conf:max_connection_delay_time(100),

  ok = application:start(mero),

  PoolModule = mero_cluster:server(cluster, <<"222">>),

  ct:log("all memcached servers are down.. ~p" , [{PoolModule}]),

  mero_test_util:wait_for_min_connections_failed(PoolModule,
    0, 0, MinConn),

  ct:log("starting server on ?PORT ~p", [?PORT]),
  {ok, _ServerPid} = mero_dummy_server:start_link(?PORT),

  ct:log("checking that the connections are performed"),
  mero_test_util:wait_for_pool_state(PoolModule, MinConn, MinConn, 0, 0),

  ok = application:stop(mero),
  ok = application:unload(mero).


%% Test expiration of the connections and re-generation when the server is
%% available after expiration
expire_connections(_) ->
  ct:log("Creating a server configured to renew unused sockets."),
  {ok, _Pid} = mero_test_util:start_server(?CLUSTER_CONFIG, 2, 4, 300, 900),
  mero_test_util:wait_for_pool_state(?POOL, 2, 2, 0, 0),

  ct:log("Let's take two of the connections, no new ones will be created"),
  P1 = proc:new(),
  P2 = proc:new(),
  P3 = proc:new(),
  {ok, Conn1} = proc:exec(P1, {mero_pool, checkout, [?POOL, ?TIMELIMIT(1000)]}),
  {ok, Conn2} = proc:exec(P2, {mero_pool, checkout, [?POOL, ?TIMELIMIT(1000)]}),
  mero_test_util:wait_for_pool_state(?POOL, 0, 2, 0, 0),

  ct:log("Only on reject are new connections minted."),
  {error, reject} = proc:exec(P3, {mero_pool, checkout, [?POOL, ?TIMELIMIT(1000)]}),
  mero_test_util:wait_for_pool_state(?POOL, 2, 4, 0, 0),

  ct:log("Now we can take a new connection."),
  {ok, Conn3} = proc:exec(P3, {mero_pool, checkout, [?POOL, ?TIMELIMIT(1000)]}),
  mero_test_util:wait_for_pool_state(?POOL, 1, 4, 0, 0),

  timer:sleep(500),
  ct:log("kill the server so no connection can be stablished from now on! ;)"),
  mero_dummy_server:stop(?PORT),

  mero_test_util:wait_for_pool_state(?POOL, 1, 4, 0, 0),
  ct:log("checkin the sockets. We want these sockets to not expire"),
  ok = proc:exec(P1, {mero_pool, checkin, [Conn1]}),
  ok = proc:exec(P2, {mero_pool, checkin, [Conn2]}),
  ok = proc:exec(P3, {mero_pool, checkin, [Conn3]}),
  mero_test_util:wait_for_pool_state(?POOL, 4, 4, 0, 0),

  ct:log("After second check we will end up with only 2 connections"),
  timer:sleep(300),
  mero_test_util:wait_for_pool_state(?POOL, 3, 3, 0, 0),

  ct:log("We take one of the connections"),
  {ok, _Conn4} = proc:exec(P1, {mero_pool, checkout, [?POOL, ?TIMELIMIT(1000)]}),
  mero_test_util:wait_for_pool_state(?POOL, 2, 3, 0, 0),

  ct:log("And wait for the rest to expire..."),
  timer:sleep(601),
    mero_test_util:wait_for_min_connections_failed(?POOL, 0, 1, 1),

  ct:log("Finally start server again and expect recover"),
  {ok, _ServerPid} = mero_dummy_server:start_link(?PORT),
  mero_test_util:wait_for_pool_state(?POOL, 2, 3, 0, 0).



%% @doc Basic test for checkout and checkin to pool.
%% Test that connections are re-used.
checkout_checkin(_) ->
  mero_test_util:start_server(?CLUSTER_CONFIG, 1, 1, 1000, 1000),

  ct:log("A process is allowed to checkout a new connection"),
  {ok, Conn1} = mero_pool:checkout(?POOL, ?TIMELIMIT(1000)),

  ct:log("You are rejected simply because the limit has been reached"),
  ?assertMatch({error, reject},
    proc:exec(proc:new(), {mero_pool, checkout, [?POOL, ?TIMELIMIT(1000)]})),
  ?assertMatch({error, reject},
    proc:exec(proc:new(), {mero_pool, checkout, [?POOL, ?TIMELIMIT(1000)]})),
  ?assertMatch({error, reject},
    proc:exec(proc:new(), {mero_pool, checkout, [?POOL, ?TIMELIMIT(1000)]})),

  ct:log("checkin connection"),
  ok = mero_pool:checkin(Conn1),
  mero_test_util:wait_for_pool_state(?POOL, 1, 1, 0, 0),

  ct:log("Another process is allowed to checkout a new connection"),
  ?assertMatch({ok, _Conn2},
    proc:exec(proc:new(), {mero_pool, checkout, [?POOL, ?TIMELIMIT(1000)]})).


%% A little more complex than the previous. Tests that new connections are created
%% as the ones we have are in use
checkout_checkin_limits(_) ->
  mero_test_util:start_server(?CLUSTER_CONFIG, 2, 4, 1000, 1000),

  mero_test_util:wait_for_pool_state(?POOL, 2, 2, 0, 0),

  ct:log("A process is allowed to checkout a new connection"),
  {ok, Conn1} = mero_pool:checkout(?POOL, ?TIMELIMIT(1000)),
  mero_test_util:wait_for_pool_state(?POOL, 2, 3, 0, 0),

  ct:log("A 2nd process is allowed to checkout a new connection"),
  ?assertMatch({ok, _},
    proc:exec(proc:new(), {mero_pool, checkout, [?POOL, ?TIMELIMIT(1000)]})),
  mero_test_util:wait_for_pool_state(?POOL, 2, 4, 0, 0),

  ct:log("A 3rd process is allowed to checkout a new connection"),
  ?assertMatch({ok, _},
    proc:exec(proc:new(), {mero_pool, checkout, [?POOL, ?TIMELIMIT(1000)]})),
  mero_test_util:wait_for_pool_state(?POOL, 1, 4, 0, 0),

  ct:log("The first one is on use so the second one should be established soon"),
  ok = mero_pool:checkin(Conn1),
  mero_test_util:wait_for_pool_state(?POOL, 2, 4, 0, 0),

  ct:log("A 3rd process is allowed to checkout a new connection"),
  ?assertMatch({ok, _},
    proc:exec(proc:new(), {mero_pool, checkout, [?POOL, ?TIMELIMIT(1000)]})),
  mero_test_util:wait_for_pool_state(?POOL, 1, 4, 0, 0).


%% Tests that if a socket is checkined closed a new one will be created
checkout_checkin_closed(_) ->
  mero_test_util:start_server(?CLUSTER_CONFIG, 2, 2, 1000, 1000),
  mero_test_util:wait_for_pool_state(?POOL, 2, 2, 0, 0),

  ct:log("A process is allowed to checkout a new connection"),
  {ok, Conn1} = mero_pool:checkout(?POOL, ?TIMELIMIT(1000)),
  mero_test_util:wait_for_pool_state(?POOL, 1, 2, 0, 0),

  ct:log("A 2nd process is allowed to checkout a new connection"),
  {ok, _Conn2} =
     proc:exec(proc:new(), {mero_pool, checkout, [?POOL, ?TIMELIMIT(1000)]}),
  mero_test_util:wait_for_pool_state(?POOL, 0, 2, 0, 0),

  ct:log("First connection is checkined closed. It will open a new one"),
  ok = mero_pool:checkin_closed(Conn1),
  mero_test_util:wait_for_pool_state(?POOL, 1, 2, 0, 0).



%% @doc Test that connection failure results in a error
%% checkoutting a connection from the pool.
conn_failed_checkout_error(_) ->
  ok = mero_conf:cluster_config(?CLUSTER_CONFIG),
  ok = application:start(mero),

  mero_test_util:wait_for_min_connections_failed(?POOL, 0, 0, 2),
  ?assertMatch({error, reject}, mero_pool:checkout(?POOL, ?TIMELIMIT(1000))).


%% @doc Test that the pool recovers a connection when
%% a using process dies without checkin the connection.
checkout_and_die(_) ->
  mero_test_util:start_server(?CLUSTER_CONFIG, 1, 1, 1000, 1000),

  ct:log("A process is allowed to checkout a new connection"),
  Parent = self(),

  spawn_link(fun() ->
        mero_pool:checkout(?POOL, ?TIMELIMIT(1000)),
        mero_test_util:wait_for_pool_state(?POOL, 0, 1, 0, 0),
        Parent ! done
      end),

  receive
    done ->
      mero_test_util:wait_for_pool_state(?POOL, 1, 1, 0, 0),
      ?assertMatch({ok, _}, mero_pool:checkout(?POOL, ?TIMELIMIT(1000))),
      mero_test_util:wait_for_pool_state(?POOL, 0, 1, 0, 0)
  end.

%% @doc Test that checkout the conn timeout, and the process dies.
checkout_timeout(_) ->
    mero_test_util:start_server(?CLUSTER_CONFIG, 2, 2, 1000, 1000),
    P1 = proc:new(),
    ct:log("A process timesout"),
    {error, pool_timeout} = proc:exec(P1, {mero_pool, checkout, [?POOL, ?TIMELIMIT(0)]}),
    mero_test_util:wait_for_pool_state(?POOL, 2, 2, 0, 0),
    ?assertMatch({ok, _Conn}, mero_pool:checkout(?POOL, ?TIMELIMIT(3))),
    mero_test_util:wait_for_pool_state(?POOL, 1, 2, 0, 0),
    ?assertMatch({ok, _Conn}, mero_pool:checkout(?POOL, ?TIMELIMIT(1000))),
    mero_test_util:wait_for_pool_state(?POOL, 0, 2, 0, 0).


%%%=============================================================================
%%% Helper functions
%%%=============================================================================
