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
-module(mero_test_util).

-author('Miriam Pena <miriam.pena@adroll.com>').

-export([start_server/5,
         stop_servers/1,
         wait_for_pool_state/5,
         wait_for_min_connections_failed/4
  ]).


wait_for_pool_state(Pool, Free, Connected, Connecting, NumFailedConnecting) ->
  case mero_pool:state(Pool) of
    [ _QueueInfo,
      {free, Free},
      {num_connected, Connected},
      {num_connecting, Connecting},
      {num_failed_connecting, NumFailedConnecting}] = State ->
      io:format("Pool State is ~p ~p... GOT IT! ~n",[now(), State]),
      ok;
    State ->
      io:format("Pool State is ~p ~p... retry ~n",[now(), State]),
      timer:sleep(30),
      wait_for_pool_state(Pool, Free, Connected, Connecting, NumFailedConnecting)
  end.


wait_for_min_connections_failed(Pool, Free, Connected, MinFailed) ->
  case mero_pool:state(Pool) of
    [ _QueueInfo,
      {free, Free},
      {num_connected, Connected},
      {num_connecting, _},
      {num_failed_connecting, NumFailed}] = State when MinFailed =<NumFailed ->
      io:format("Pool State is ~p ~p... GOT IT! ~n",[now(), State]),
      ok;
    State ->
      io:format("Pool State is ~p ~p... retry ~n",[now(), State]),
      timer:sleep(30),
      wait_for_min_connections_failed(Pool, Free, Connected, MinFailed)
  end.


start_server(ClusterConfig, MinConn, MaxConn, Expiration, MaxTime) ->
  ok = mero_conf:cluster_config(ClusterConfig),
  ok = mero_conf:initial_connections_per_pool(MinConn),
  ok = mero_conf:min_free_connections_per_pool(MinConn),
  ok = mero_conf:max_connections_per_pool(MaxConn),
  ok = mero_conf:expiration_interval(Expiration),
  ok = mero_conf:connection_unused_max_time(MaxTime),
  ok = mero_conf:max_connection_delay_time(100),
  ok = mero_conf:write_retries(3),
  ok = mero_conf:timeout_read(100),
  ok = mero_conf:timeout_write(1000),

  ServerPids = lists:foldr(
      fun({_, Config}, Acc) ->
        HostPortList = proplists:get_value(servers, Config),
        lists:foldr(fun({_Host, Port}, Acc2) ->
            ct:log("Starting server on Port ~p", [Port]),
            {ok, ServerPid} = mero_dummy_server:start_link(Port),
            [ServerPid | Acc2]
            end, Acc, HostPortList)
      end, [], ClusterConfig),

  ok = application:start(mero),

  %% Wait for the connections
  [ wait_for_pool_state(PoolName, MinConn, MinConn, 0, 0)
    || {_Name, _Host, _Port, PoolName, _WorkerModule} <- mero_cluster:child_definitions() ],

  {ok, ServerPids}.

stop_servers(Pids) ->
    [mero_dummy_server:stop(Pid) || Pid <- Pids].
