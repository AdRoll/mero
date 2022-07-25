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

%% It's dynamically invoked using rpc:pmap/3
-ignore_xref({?MODULE, get_elasticache_cluster_configs, 1}).

-export([cluster_config/0, cluster_config/1, pool_timeout_read/1, timeout_read/1,
         pool_timeout_write/1, timeout_write/1, pool_key_expiration_time/1, key_expiration_time/1,
         pool_write_retries/1, write_retries/1, pool_max_connections/1, max_connections_per_pool/1,
         pool_initial_connections/1, initial_connections_per_pool/1, pool_expiration_interval/1,
         expiration_interval/1, pool_min_free_connections/1, min_free_connections_per_pool/1,
         pool_connection_unused_max_time/1, connection_unused_max_time/1,
         pool_max_connection_delay_time/1, pool_min_connection_interval/1,
         max_connection_delay_time/1, stat_callback/0, stat_callback/1, add_now/1, add_now/2,
         millis_to/1, millis_to/2, process_server_specs/1, elasticache_load_config_delay/0,
         elasticache_load_config_delay/1, monitor_heartbeat_delay/0, monitor_heartbeat_delay/2]).

-include_lib("mero/include/mero.hrl").

-type per_pool_config_value(Type) ::
    {by_pool, Default :: Type, [{Pool :: atom(), Value :: Type}]}.
-type mero_conf_value(Type) :: Type | per_pool_config_value(Type).

%%%=============================================================================
%%% External functions
%%%=============================================================================

%% @doc Returns a _randomized_ time to wait between config checks, in milliseconds
monitor_heartbeat_delay() ->
    Min = get_env(conf_monitor_min_sleep),
    Max = get_env(conf_monitor_max_sleep),
    Min + rand:uniform(Max - Min).

%% @doc Sets the boundaries of the time to wait between config checks, in milliseconds
monitor_heartbeat_delay(Min, Max) ->
    application:set_env(mero, conf_monitor_min_sleep, Min),
    application:set_env(mero, conf_monitor_max_sleep, Max).

%% @doc Returns the amount of milliseconds to wait before reading elasticache config
-spec elasticache_load_config_delay() -> non_neg_integer().
elasticache_load_config_delay() ->
    get_env(elasticache_load_config_delay).

%% @doc Sets the amount of milliseconds to wait before reading elasticache config
-spec elasticache_load_config_delay(non_neg_integer()) -> ok.
elasticache_load_config_delay(Millis) ->
    application:set_env(mero, elasticache_load_config_delay, Millis).

%% @doc: Returns the cluster configuration
-spec cluster_config() -> mero:cluster_config().
cluster_config() ->
    get_env(cluster_config).

-spec cluster_config(ClusterConfig :: term()) -> ok.
cluster_config(ClusterConfig) ->
    application:set_env(mero, cluster_config, ClusterConfig).

%% @doc: Number of sockets that each pool will open on startup
-spec pool_initial_connections(Pool :: atom()) -> integer().
pool_initial_connections(Pool) ->
    get_env_per_pool(initial_connections_per_pool, Pool).

-spec initial_connections_per_pool(Initial :: mero_conf_value(integer())) -> ok.
initial_connections_per_pool(Initial) ->
    application:set_env(mero, initial_connections_per_pool, Initial).

%% @doc: If the number of free sockets is smaller than this
%% the pool will asynchronously create new ones to ensure we
%% dont run out of them.
-spec pool_min_free_connections(Pool :: atom()) -> integer().
pool_min_free_connections(Pool) ->
    get_env_per_pool(min_free_connections_per_pool, Pool).

-spec min_free_connections_per_pool(MinFree :: mero_conf_value(integer())) -> ok.
min_free_connections_per_pool(MinFree) ->
    application:set_env(mero, min_free_connections_per_pool, MinFree).

%% Maximum number of connections that each pool will open.
-spec pool_max_connections(Pool :: atom()) -> integer().
pool_max_connections(Pool) ->
    get_env_per_pool(max_connections_per_pool, Pool).

-spec max_connections_per_pool(Max :: mero_conf_value(integer())) -> ok.
max_connections_per_pool(Max) ->
    application:set_env(mero, max_connections_per_pool, Max).

%% @doc: Read timeout in milliseconds
-spec pool_timeout_read(Pool :: atom()) -> integer().
pool_timeout_read(Pool) ->
    get_env_per_pool(timeout_read, Pool).

-spec timeout_read(Timeout :: mero_conf_value(integer())) -> ok.
timeout_read(Timeout) ->
    application:set_env(mero, timeout_read, Timeout).

%% @doc: Write timeout in milliseconds
-spec pool_timeout_write(Pool :: atom()) -> integer().
pool_timeout_write(Pool) ->
    get_env_per_pool(timeout_write, Pool).

-spec timeout_write(Timeout :: mero_conf_value(integer())) -> ok.
timeout_write(Timeout) ->
    application:set_env(mero, timeout_write, Timeout).

%% @doc: Number of retries for write operations
-spec pool_write_retries(Pool :: atom()) -> integer().
pool_write_retries(Pool) ->
    get_env_per_pool(write_retries, Pool).

-spec write_retries(Timeout :: mero_conf_value(integer())) -> ok.
write_retries(Timeout) ->
    application:set_env(mero, write_retries, Timeout).

%% @doc: Gets the default value for a key expiration time
-spec pool_key_expiration_time(Pool :: atom()) -> integer().
pool_key_expiration_time(Pool) ->
    get_env_per_pool(expiration_time, Pool).

-spec key_expiration_time(Time :: mero_conf_value(integer())) -> ok.
key_expiration_time(Time) ->
    application:set_env(mero, expiration_time, Time).

%% @doc: Checks for unused sockets every XX (millis) and closes them.
-spec pool_expiration_interval(Pool :: atom()) -> integer().
pool_expiration_interval(Pool) ->
    get_env_per_pool(expiration_interval, Pool).

-spec expiration_interval(mero_conf_value(integer())) -> ok.
expiration_interval(Val) ->
    application:set_env(mero, expiration_interval, Val).

%% @doc: Maximum time that a connection can be inactive before closing it (millis)
-spec pool_connection_unused_max_time(Pool :: atom()) -> timeout().
pool_connection_unused_max_time(Pool) ->
    get_env_per_pool(connection_unused_max_time, Pool).

-spec connection_unused_max_time(mero_conf_value(integer())) -> ok.
connection_unused_max_time(Val) ->
    application:set_env(mero, connection_unused_max_time, Val).

%% @doc: maximum delay establishing initial connections
-spec pool_max_connection_delay_time(Pool :: atom()) -> integer().
pool_max_connection_delay_time(Pool) ->
    get_env_per_pool(max_connection_delay_time, Pool).

%% @doc: min delay between connection attempts
-spec pool_min_connection_interval(Pool :: atom()) -> integer().
pool_min_connection_interval(Pool) ->
    try
        get_env_per_pool(min_connection_interval, Pool)
    catch
        _:_ ->
            %% Don't want to make this mandatory, but the rest are mandatory already.
            undefined
    end.

-spec max_connection_delay_time(mero_conf_value(integer())) -> ok.
max_connection_delay_time(Val) ->
    application:set_env(mero, max_connection_delay_time, Val).

%% @doc: maximum delay establishing initial connections (ms)
-spec stat_callback() -> {Module :: module(), Function :: atom()}.
stat_callback() ->
    get_env(stat_event_callback).

-spec stat_callback({Module :: module(), Function :: atom()}) -> ok.
stat_callback(Val) ->
    application:set_env(mero, stat_event_callback, Val).

add_now(Timeout) ->
    add_now(Timeout, os:timestamp()).

add_now(Timeout, Then) ->
    {M, S, MS} = Then,
    {M, S, MS + Timeout * 1000}.

millis_to(TimeLimit) ->
    millis_to(TimeLimit, os:timestamp()).

millis_to(TimeLimit, Then) ->
    case timer:now_diff(TimeLimit, Then) div 1000 of
        N when N > 0 ->
            N;
        _ ->
            0
    end.

-spec process_server_specs(mero:cluster_config()) -> mero:cluster_config().
process_server_specs([]) ->
    [];
process_server_specs([Cluster | Clusters]) ->
    case process_server_spec(Cluster) of
        {error, _} ->
            process_server_specs(Clusters);
        Config ->
            [Config | process_server_specs(Clusters)]
    end.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

process_server_spec({ClusterName, Attrs}) ->
    try
        {ClusterName, [process_value(Attr) || Attr <- Attrs]}
    catch
        Kind:Desc:Stack ->
            error_logger:error_report([{error, mero_config_failed},
                                       {kind, Kind},
                                       {desc, Desc},
                                       {stack, Stack}]),
            {error, Desc}
    end.

get_env(Key) ->
    case application:get_env(mero, Key) of
        {ok, Value} ->
            Value;
        undefined ->
            exit({undefined_configuration, Key})
    end.

get_env_per_pool(Key, Pool) ->
    case get_env(Key) of
        {by_pool, Default, ByPool} ->
            maps:get(Pool, ByPool, Default);
        Value ->
            Value
    end.

process_value({servers, {elasticache, ConfigEndpoint, ConfigPort}}) ->
    process_value({servers, {elasticache, [{ConfigEndpoint, ConfigPort, 1}]}});
process_value({servers, {elasticache, ConfigList}}) when is_list(ConfigList) ->
    HostsPorts =
        try
            rpc:pmap({?MODULE, get_elasticache_cluster_configs}, [], ConfigList)
        catch
            _:badrpc ->
                % Fallback to sequential execution, mostly to get proper error descriptions
                lists:map(fun get_elasticache_cluster_configs/1, ConfigList)
        end,
    {servers, lists:flatten(HostsPorts)};
process_value({servers, {mfa, {Module, Function, Args}}}) ->
    try erlang:apply(Module, Function, Args) of
        {ok, HostsPorts} when is_list(HostsPorts) ->
            {servers, HostsPorts}
    catch
        Type:Reason ->
            error({invalid_call, {Module, Function, Args}, {Type, Reason}})
    end;
process_value(V) ->
    V.

get_elasticache_cluster_configs({Host, Port, ClusterSpeedFactor}) ->
    lists:duplicate(ClusterSpeedFactor, get_elasticache_cluster_config(Host, Port));
get_elasticache_cluster_configs({Host, Port}) ->
    [get_elasticache_cluster_config(Host, Port)].

get_elasticache_cluster_config(Host, Port) ->
    case mero_elasticache:get_cluster_config(Host, Port) of
        {ok, Entries} ->
            Entries;
        {error, Reason} ->
            error({Reason, Host, Port})
    end.
