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
-module(mero).

-author('Miriam Pena <miriam.pena@adroll.com>').

-behaviour(application).

-export([start/0,
         start/2,
         stop/1]).

-export([increment_counter/2,
         increment_counter/7,
         mincrement_counter/2,
         mincrement_counter/7,
         get/2,
         get/3,
         gets/2,
         gets/3,
         delete/3,
         mdelete/3,
         mget/2,
         mget/3,
         mgets/2,
         mgets/3,
         set/5, mset/3,
         cas/6, mcas/3,
         add/5, madd/3,
         flush_all/1,
         shard_phash2/2,
         shard_crc32/2,
         clustering_key/1,
         storage_key/1
        ]).

-export([state/0,
         state/1,
         deep_state/0,
         deep_state/1]).

-include_lib("mero/include/mero.hrl").

-type cas_token() :: undefined | integer().
-type result() :: {Key :: binary(), Value :: undefined | binary()}.
-type extended_result() :: {Key :: binary(), Value :: undefined | binary(), CAS :: cas_token()}.

%% {ClusteringKey, Key}. ClusteringKey is used to select the memcached node
%% where to store the data, and Key is used to store the data in that
%% node.  If a single binary Key is used,  then ClusteringKey = Key.
-type mero_key() :: binary() | {ClusteringKey :: binary(), Key :: binary()}.

-export_type([mero_key/0,
              cas_token/0,
              result/0,
              extended_result/0]).

%%%=============================================================================
%%% Application behaviour
%%%=============================================================================

start() ->
    application:start(mero).

%% @doc: Starts the application
start(_StartType, _StartArgs) ->
    ClusterConfig = mero_conf:cluster_config(),
    mero_sup:start_link(ClusterConfig).

stop(_State) ->
    ok.

%%%=============================================================================
%%% External functions
%%%=============================================================================

-spec get(ClusterName :: atom(), Key :: mero_key(), Timeout :: integer()) ->
                 result() | {error, Reason :: term()}.
get(ClusterName, Key, Timeout) ->
    case gets(ClusterName, Key, Timeout) of
        {error, Reason} ->
            {error, Reason};
        {Key, Value, _CAS} ->
            {Key, Value}
    end.
get(ClusterName, Key) ->
    get(ClusterName, Key, mero_conf:pool_timeout_read(ClusterName)).


-spec mget(ClusterName :: atom(), Keys :: [mero_key()], Timeout :: integer()) ->
    [result()] | {error, [Reason :: term()], ProcessedKeyValues :: [result()]}.
mget(ClusterName, Keys, Timeout) when is_list(Keys), is_atom(ClusterName) ->
    Extract = fun (Items) ->
                      [{Key, Value}
                       || {Key, Value, _} <- Items]
              end,
    case mgets(ClusterName, Keys, Timeout) of
        {error, Reason, ProcessedKeyValues} ->
            {error, Reason, Extract(ProcessedKeyValues)};
        KeyValues ->
            Extract(KeyValues)
    end.
mget(ClusterName, Keys) ->
    mget(ClusterName, Keys, mero_conf:pool_timeout_read(ClusterName)).


-spec gets(ClusterName :: atom(), Key :: mero_key(), Timeout :: integer()) ->
                  extended_result() | {error, Reason :: term()}.
gets(ClusterName, Key, Timeout) ->
    case mgets(ClusterName, [Key], Timeout) of
        {error, [Reason], []} ->
            {error, Reason};
        {error, _Reason, [Processed]} ->
            Processed;
        [Result] ->
            Result;
        [] ->
            {Key, undefined, undefined}
    end.
gets(ClusterName, Key) ->
    gets(ClusterName, Key, mero_conf:pool_timeout_read(ClusterName)).


-spec mgets(ClusterName :: atom(), Keys :: [mero_key()], Timeout :: integer()) ->
    [extended_result()]
    | {error, [Reason :: term()], ProcessedKeyValues :: [extended_result()]}.
mgets(ClusterName, Keys, Timeout) when is_list(Keys), is_atom(ClusterName) ->
    Extract = fun (Items) ->
                      [{Key, Value, CAS}
                       || #mero_item{key = Key, value = Value, cas = CAS} <- Items]
              end,
    case mero_conn:get(ClusterName, Keys, Timeout) of
        {error, Reason, ProcessedKeyValues} ->
            {error, Reason, Extract(ProcessedKeyValues)};
        KeyValues ->
            Extract(KeyValues)
    end.
mgets(ClusterName, Keys) ->
    mgets(ClusterName, Keys, mero_conf:pool_timeout_read(ClusterName)).


-spec add(ClusterName :: atom(), Key :: mero_key(), Value :: binary(), ExpTime :: integer(),
    Timeout :: integer()) ->
    ok | {error, Reason :: term()}.
add(ClusterName, Key, Value, ExpTime, Timeout)
    when is_atom(ClusterName), is_binary(Value), is_integer(ExpTime) ->
    BExpTime = list_to_binary(integer_to_list(ExpTime)),
    mero_conn:add(ClusterName, Key, Value, BExpTime, Timeout).


-spec madd(ClusterName :: atom(),
           [{Key :: mero_key(),
             Value :: binary(),
             ExpTime :: integer()}],
           Timeout :: integer()) ->
    [ok | {error, Reason :: term()}].
madd(ClusterName, KVEs, Timeout)
    when is_atom(ClusterName) ->
    L = [{Key, Value, list_to_binary(integer_to_list(ExpTime))}
         || {Key, Value, ExpTime} <- KVEs,
            is_binary(Key),
            is_binary(Value),
            is_integer(ExpTime)],
    mero_conn:madd(ClusterName, L, Timeout).


-spec set(ClusterName :: atom(),
          Key :: mero_key(),
          Value :: binary(),
          ExpTime :: integer(), % value is in seconds
          Timeout :: integer()) ->
    ok | {error, Reason :: term()}.
set(ClusterName, Key, Value, ExpTime, Timeout) ->
    cas(ClusterName, Key, Value, ExpTime, Timeout, undefined).


-spec mset(ClusterName :: atom(),
           [{Key :: mero_key(),
             Value :: binary(),
             ExpTime :: integer()}], % value is in seconds
           Timeout :: integer()) ->
    [ok | {error, Reason :: term()}].
mset(ClusterName, KVEs, Timeout) ->
    L = [{Key, Value, ExpTime, undefined}
         || {Key, Value, ExpTime} <- KVEs],
    mcas(ClusterName, L, Timeout).


-spec cas(ClusterName :: atom(),
          Key :: mero_key(),
          Value :: binary(),
          ExpTime :: integer(), % value is in seconds
          Timeout :: integer(),
          CAS :: cas_token()) ->
    ok | {error, Reason :: term()}.
cas(ClusterName, Key, Value, ExpTime, Timeout, CAS)
    when is_atom(ClusterName), is_binary(Value), is_integer(ExpTime) ->
    BExpTime = list_to_binary(integer_to_list(ExpTime)),
    %% note: if CAS is undefined, this will be an unconditional set:
    mero_conn:set(ClusterName, Key, Value, BExpTime, Timeout, CAS).


-spec mcas(ClusterName :: atom(),
           [{Key :: mero_key(),
             Value :: binary(),
             ExpTime :: integer(), % value is in seconds
             CAS :: cas_token()}],
           Timeout :: integer()) ->
    [ok | {error, Reason :: term()}].
mcas(ClusterName, KVECs, Timeout)
     when is_atom(ClusterName) ->
    %% note: if CAS is undefined, the corresponding set will be unconditional.
    L = [{Key, Value, list_to_binary(integer_to_list(ExpTime)), CAS}
         || {Key, Value, ExpTime, CAS} <- KVECs,
            is_binary(Key),
            is_binary(Value),
            is_integer(ExpTime)],
    mero_conn:mset(ClusterName, L, Timeout).


%% @doc: Increments a counter: initial value is 1, steps of 1, timeout defaults to 24 hours.
%%    3 retries.
-spec increment_counter(ClusterName :: atom(), Key :: mero_key()) ->
    ok | {error, Reason :: term()}.
increment_counter(ClusterName, Key) when is_atom(ClusterName) ->
    increment_counter(ClusterName, Key, 1, 1,
                      mero_conf:pool_key_expiration_time(ClusterName),
                      mero_conf:pool_write_retries(ClusterName),
                      mero_conf:pool_timeout_write(ClusterName)).


-spec increment_counter(ClusterName :: atom(), Key :: mero_key(), Value :: integer(),
    Initial :: integer(), ExpTime :: integer(),
    Retries :: integer(), Timeout :: integer()) ->
        ok | {error, Reason :: term()}.
increment_counter(ClusterName, Key, Value, Initial, ExpTime, Retries, Timeout)
  when is_integer(Value), is_integer(ExpTime), is_atom(ClusterName),
    (Initial >= 0), (Value >=0) ->
    BValue = list_to_binary(integer_to_list(Value)),
    BInitial = list_to_binary(integer_to_list(Initial)),
    BExpTime = list_to_binary(integer_to_list(ExpTime)),
    mero_conn:increment_counter(ClusterName, Key, BValue, BInitial, BExpTime, Retries, Timeout).

-spec mincrement_counter(ClusterName :: atom(), Key :: [mero_key()]) ->
    ok | {error, Reason :: term()}.
mincrement_counter(ClusterName, Keys) when is_atom(ClusterName), is_list(Keys) ->
    mincrement_counter(ClusterName, Keys, 1, 1,
                       mero_conf:pool_key_expiration_time(ClusterName),
                       mero_conf:pool_write_retries(ClusterName),
                       mero_conf:pool_timeout_write(ClusterName)).

-spec mincrement_counter(ClusterName :: atom(), Keys :: [mero_key()], Value :: integer(),
                         Initial :: integer(), ExpTime :: integer(),
                         Retries :: integer(), Timeout :: integer()) ->
                                ok | {error, Reason :: term()}.
mincrement_counter(ClusterName, Keys, Value, Initial, ExpTime, Retries, Timeout)
  when is_list(Keys), is_integer(Value), is_integer(ExpTime), is_atom(ClusterName),
    (Initial >= 0), (Value >=0) ->
    BValue = list_to_binary(integer_to_list(Value)),
    BInitial = list_to_binary(integer_to_list(Initial)),
    BExpTime = list_to_binary(integer_to_list(ExpTime)),
    mero_conn:mincrement_counter(ClusterName, Keys, BValue, BInitial, BExpTime, Retries, Timeout).


-spec delete(ClusterName :: atom(), Key :: mero_key(), Timeout :: integer()) ->
    ok | {error, Reason :: term()}.
delete(ClusterName, Key, Timeout) when is_atom(ClusterName) ->
    mero_conn:delete(ClusterName, Key, Timeout).

-spec mdelete(ClusterName :: atom(), Keys :: [mero_key()], Timeout :: integer()) -> ok.
mdelete(ClusterName, Keys, Timeout) when is_list(Keys), is_atom(ClusterName) ->
    mero_conn:mdelete(ClusterName, Keys, Timeout).

%% The response is a list of all the individual requests, one per shard
-spec flush_all(ClusterName :: atom()) ->
    [ ok | {error, Response :: term()}].
flush_all(ClusterName) ->
    mero_conn:flush_all(ClusterName, ?DEFAULT_TIMEOUT).


%%%=============================================================================
%%% Sharding algorithms
%%%=============================================================================

-spec clustering_key(Key :: mero_key()) -> binary().
clustering_key({ClusteringKey, _}) ->
    ClusteringKey;
clustering_key(Key) when is_binary(Key) ->
    Key.

-spec storage_key(Key :: mero_key()) -> binary().
storage_key({_ClusteringKey, Key}) ->
    Key;
storage_key(Key) when is_binary(Key) ->
    Key.

-spec shard_phash2(Key :: binary(), ShardSize :: pos_integer()) -> pos_integer().
shard_phash2(Key, ShardSize) ->
    erlang:phash2(Key, ShardSize).


-spec shard_crc32(Key :: binary(), ShardSize :: pos_integer()) -> pos_integer().
shard_crc32(Key, ShardSize) ->
    ((erlang:crc32(Key) bsr 16) band 16#7fff) rem ShardSize.

%%%=============================================================================
%%% Introspection functions
%%%=============================================================================

%% @doc: Returns the state of the sockets of a Cluster
state(ClusterName) ->
    ZeroState = [
        {links, 0},
        {monitors, 0},
        {free, 0},
        {connected, 0},
        {connecting, 0},
        {failed, 0},
        {message_queue_len, 0}
    ],
    lists:foldr(
        fun({_, _, Pool, _}, Acc) ->
            inc_state(mero_pool:state(Pool), Acc)
        end, ZeroState, mero_cluster:child_definitions(ClusterName)).

%% @doc: Returns the state of the sockets for all clusters
state() ->
    [{Cluster, state(Cluster)} || Cluster <- mero_cluster:clusters()].

inc_state({error, _}, Acc) ->
    Acc;
inc_state(St, Acc) ->
    lists:map(
        fun ({connected, AccV}) ->
                {connected, AccV + proplists:get_value(num_connected, St)};
            ({connecting, AccV}) ->
                {connecting, AccV + proplists:get_value(num_connecting, St)};
            ({failed, AccV}) ->
                {failed, AccV + proplists:get_value(num_failed_connecting, St)};
            ({K, AccV}) ->
                {K, AccV + proplists:get_value(K, St)}
        end, Acc).


deep_state(ClusterName) ->
    F = fun({_, _, Pool, _}, Acc) ->
            St = mero_pool:state(Pool),
            [[{pool, Pool} | St] | Acc]
        end,
    lists:foldr(F, [], mero_cluster:child_definitions(ClusterName)).


%% @doc: Returns the state of the sockets for all clusters
deep_state() ->
    [{Cluster, deep_state(Cluster)} || Cluster <- mero_cluster:clusters()].
