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
%% @doc: Lists all modules
-module(mero_conf).

-author('Miriam Pena <miriam.pena@adroll.com>').

-export([cluster_config/0,
         cluster_config/1,
         timeout_read/0,
         timeout_read/1,
         timeout_write/0,
         timeout_write/1,
         key_expiration_time/0,
         key_expiration_time/1,
         write_retries/0,
         write_retries/1,
         max_connections_per_pool/0,
         max_connections_per_pool/1,
         initial_connections_per_pool/0,
         initial_connections_per_pool/1,
         expiration_interval/0,
         expiration_interval/1,
         min_free_connections_per_pool/0,
         min_free_connections_per_pool/1,
         connection_unused_max_time/0,
         connection_unused_max_time/1,
         max_connection_delay_time/0,
         max_connection_delay_time/1,
         stat_callback/0,
         stat_callback/1,
         add_now/1,
         add_now/2,
         millis_to/1,
         millis_to/2,
         process_server_specs/1]).

-include_lib("mero/include/mero.hrl").

%%%=============================================================================
%%% External functions
%%%=============================================================================

%% @doc: Returns the cluster configuration
-spec cluster_config() -> list({Name :: atom(), Config :: list()}).
cluster_config() ->
    get_env(cluster_config).

-spec cluster_config(ClusterConfig :: term()) -> ok.
cluster_config(ClusterConfig) ->
    application:set_env(mero, cluster_config, ClusterConfig).


%% @doc: Number of sockets that each pool will open on startup
-spec initial_connections_per_pool() -> integer().
initial_connections_per_pool() ->
  get_env(initial_connections_per_pool).

-spec initial_connections_per_pool(Initial :: integer()) -> ok.
initial_connections_per_pool(Initial) ->
  application:set_env(mero, initial_connections_per_pool, Initial).


%% @doc: If the number of free sockets is smaller than this
%% the pool will asyncronously create new ones to ensure we
%% dont run out of them.
-spec min_free_connections_per_pool() -> integer().
min_free_connections_per_pool() ->
  get_env(min_free_connections_per_pool).

-spec min_free_connections_per_pool(MinFree :: integer()) -> ok.
min_free_connections_per_pool(MinFree) ->
  application:set_env(mero, min_free_connections_per_pool, MinFree).


%% Maximun number of connections that each pool will open.
-spec max_connections_per_pool() -> integer().
max_connections_per_pool() ->
  get_env(max_connections_per_pool).

-spec max_connections_per_pool(Max :: integer()) -> ok.
max_connections_per_pool(Max) ->
  application:set_env(mero, max_connections_per_pool, Max).


%% @doc: Read timeout in milliseconds
-spec timeout_read() -> integer().
timeout_read() ->
    get_env(timeout_read).

-spec timeout_read(Timeout :: integer()) -> ok.
timeout_read(Timeout) ->
    application:set_env(mero, timeout_read, Timeout).


%% @doc: Write timeout in milliseconds
-spec timeout_write() -> integer().
timeout_write() ->
    get_env(timeout_write).

-spec timeout_write(Timeout :: integer()) -> ok.
timeout_write(Timeout) ->
    application:set_env(mero, timeout_write, Timeout).


%% @doc: Number of retries for write operations
-spec write_retries() -> integer().
write_retries() ->
  get_env(write_retries).

-spec write_retries(Timeout :: integer()) -> ok.
write_retries(Timeout) ->
  application:set_env(mero, write_retries, Timeout).


%% @doc: Gets the default value for a key expiration time
-spec key_expiration_time() -> integer().
key_expiration_time() ->
    get_env(expiration_time).

-spec key_expiration_time(Time :: integer()) -> ok.
key_expiration_time(Time) ->
    application:set_env(mero, expiration_time, Time).


%% @doc: Checks for unused sockets every XX (millis) and closes them.
-spec expiration_interval() -> integer().
expiration_interval() ->
    get_env(expiration_interval).

-spec expiration_interval(integer()) -> ok.
expiration_interval(Val) ->
    application:set_env(mero, expiration_interval, Val).

%% @doc: Maximum time that a connection can be inactive before closing it (millis)
-spec connection_unused_max_time() -> integer().
connection_unused_max_time() ->
    get_env(connection_unused_max_time).

-spec connection_unused_max_time(integer()) -> ok.
connection_unused_max_time(Val) ->
    application:set_env(mero, connection_unused_max_time, Val).


%% @doc: maximum delay establishing initial connections
-spec max_connection_delay_time() -> integer().
max_connection_delay_time() ->
  get_env(max_connection_delay_time).

-spec max_connection_delay_time(integer()) -> ok.
max_connection_delay_time(Val) ->
  application:set_env(mero, max_connection_delay_time, Val).


%% @doc: maximum delay establishing initial connections (ms)
-spec stat_callback() -> {Module::module(), Function :: atom()}.
stat_callback() ->
  get_env(stat_event_callback).

-spec stat_callback({Module::module(), Function :: atom()}) -> ok.
stat_callback(Val) ->
  application:set_env(mero, stat_event_callback, Val).


add_now(Timeout) ->
  add_now(Timeout, os:timestamp()).


add_now(Timeout, Then) ->
  {M, S, MS} = Then,
  {M, S, MS + (Timeout * 1000)}.


millis_to(TimeLimit) ->
  millis_to(TimeLimit, os:timestamp()).

millis_to(TimeLimit, Then) ->
  case (timer:now_diff(TimeLimit, Then) div 1000) of
    N when N > 0 -> N;
    _ -> 0
  end.

process_server_specs(L) ->
    lists:foldl(fun ({ClusterName, AttrPlist}, Acc) ->
        [{ClusterName, [process_value(Attr)
            || Attr <- AttrPlist]} | Acc]
                end, [], L).

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

get_env(Key) ->
    case application:get_env(mero, Key) of
        {ok, Value} ->
            Value;
        undefined ->
            exit({undefined_configuration, Key})
    end.


process_value({servers, {elasticache, ConfigEndpoint, ConfigPort}}) ->
    process_value({servers, {elasticache, [{ConfigEndpoint, ConfigPort, 1}]}});
process_value({servers, {elasticache, ConfigList}}) when is_list(ConfigList) ->
    HostsPorts = lists:foldr(
        fun(HostConfig, Acc) ->
            {Host, Port, ClusterSpeedFactor} =
                case HostConfig of
                    {HostIn, PortIn, ClusterSpeedFactorIn} when is_integer(ClusterSpeedFactorIn) ->
                        {HostIn, PortIn, ClusterSpeedFactorIn};
                    {HostIn, PortIn} ->
                        {HostIn, PortIn, 1}

                end,
            lists:duplicate(ClusterSpeedFactor, get_elasticache_cluster_config(Host, Port)) ++ Acc
        end,
        [],
        ConfigList),
    {servers, lists:concat(HostsPorts)};
process_value(V) ->
    V.

get_elasticache_cluster_config(Host, Port) ->
    case mero_elasticache:get_cluster_config(Host, Port) of
        {ok, Entries} ->
            Entries;
        {error, Reason} ->
            throw(Reason)
    end.
