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
-module(mero_conn).

-author('Miriam Pena <miriam.pena@adroll.com>').

-export([increment_counter/7, mincrement_counter/6, get/3, set/6, mset/3, delete/3,
         mdelete/3, add/5, madd/3, flush_all/2]).

-include_lib("mero/include/mero.hrl").

-record(async_op,
        {op :: atom(), % name of worker op which sends this request
         op_error :: atom(), % name of error for errors occuring when sending request
         response :: atom(), % name of worker op which reads response
         response_error :: atom()}). % name of error for errors occuring when reading response

%%%=============================================================================
%%% External functions
%%%=============================================================================

increment_counter(Name, Key, Value, Initial, ExpTime, Retries, Timeout) ->
    TimeLimit = mero_conf:add_now(Timeout),
    case mero_cluster:server(Name, Key) of
        {ok, PoolName} ->
            increment_counter_timelimit(PoolName,
                                        mero:storage_key(Key),
                                        Value,
                                        Initial,
                                        ExpTime,
                                        Retries,
                                        TimeLimit);
        Error ->
            Error
    end.

mincrement_counter(Name, Keys, Value, Initial, ExpTime, Timeout) ->
    TimeLimit = mero_conf:add_now(Timeout),
    KeysGroupedByShards = mero_cluster:group_by_shards(Name, Keys),
    Payload =
        [{Shard, [{mero:storage_key(K), Value, Initial, ExpTime} || K <- Ks]}
         || {Shard, Ks} <- KeysGroupedByShards],
    case async_by_shard(Name,
                        Payload,
                        TimeLimit,
                        #async_op{op = async_increment,
                                  op_error = async_increment_error,
                                  response = async_blank_response,
                                  response_error = async_increment_response_error})
    of
        {error, [not_supportable], _} ->
            {error, not_supportable};
        _Other ->
            ok
    end.

set(Name, Key, Value, ExpTime, Timeout, CAS) ->
    TimeLimit = mero_conf:add_now(Timeout),
    case mero_cluster:server(Name, Key) of
        {ok, PoolName} ->
            pool_execute(PoolName,
                         set,
                         [mero:storage_key(Key), Value, ExpTime, TimeLimit, CAS],
                         TimeLimit);
        Error ->
            Error
    end.

mset(Name, KVECs, Timeout) ->
    mset_(Name, KVECs, Timeout, async_mset).

madd(Name, KVEs, Timeout) ->
    mset_(Name,
          [{Key, Value, ExpTime, undefined} || {Key, Value, ExpTime} <- KVEs],
          Timeout,
          async_madd).

get(Name, [Key], Timeout) ->
    TimeLimit = mero_conf:add_now(Timeout),
    case mero_cluster:server(Name, Key) of
        {ok, PoolName} ->
            case pool_execute(PoolName, get, [mero:storage_key(Key), TimeLimit], TimeLimit) of
                {error, Reason} ->
                    {error, [Reason], []};
                Value ->
                    [Value]
            end;
        Error ->
            Error
    end;
get(Name, Keys, Timeout) ->
    TimeLimit = mero_conf:add_now(Timeout),
    KeysGroupedByShards = mero_cluster:group_by_shards(Name, Keys),
    async_by_shard(Name,
                   KeysGroupedByShards,
                   TimeLimit,
                   #async_op{op = async_mget,
                             op_error = async_mget_error,
                             response = async_mget_response,
                             response_error = async_mget_response_error}).

delete(Name, Key, Timeout) ->
    TimeLimit = mero_conf:add_now(Timeout),
    case mero_cluster:server(Name, Key) of
        {ok, PoolName} ->
            pool_execute(PoolName, delete, [mero:storage_key(Key), TimeLimit], TimeLimit);
        Error ->
            Error
    end.

mdelete(Name, Keys, Timeout) ->
    TimeLimit = mero_conf:add_now(Timeout),
    KeysGroupedByShards = mero_cluster:group_by_shards(Name, Keys),
    async_by_shard(Name,
                   KeysGroupedByShards,
                   TimeLimit,
                   #async_op{op = async_delete,
                             op_error = async_delete_error,
                             response = async_blank_response,
                             response_error = async_delete_response_error}),
    ok.

add(Name, Key, Value, ExpTime, Timeout) ->
    TimeLimit = mero_conf:add_now(Timeout),
    case mero_cluster:server(Name, Key) of
        {ok, PoolName} ->
            pool_execute(PoolName,
                         add,
                         [mero:storage_key(Key), Value, ExpTime, TimeLimit],
                         TimeLimit);
        Error ->
            Error
    end.

flush_all(Name, Timeout) ->
    TimeLimit = mero_conf:add_now(Timeout),
    [{Name, pool_execute(PoolName, flush_all, [TimeLimit], TimeLimit)}
     || PoolName <- mero_cluster:one_pool_of_each_shard_of_cluster(Name)].

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

mset_(Name, KVECs, Timeout, Op) ->
    TimeLimit = mero_conf:add_now(Timeout),
    Requests =
        lists:zip(
            lists:seq(1, length(KVECs)), KVECs),
    %% we number each request according to its original position so we can return results
    %% in the same order:
    NKVECs = [{N, Key, Value, ExpTime, CAS} || {N, {Key, Value, ExpTime, CAS}} <- Requests],
    ItemsGroupedByShards = mero_cluster:group_by_shards(Name, NKVECs, 2),
    Processed =
        case async_by_shard(Name,
                            ItemsGroupedByShards,
                            TimeLimit,
                            #async_op{op = Op,
                                      op_error = async_mset_error,
                                      response = async_mset_response,
                                      response_error = async_mset_response_error})
        of
            {error, _ErrorsOut, ProcessedOut} ->
                ProcessedOut;
            ProcessedOut ->
                ProcessedOut
        end,
    tuple_to_list(lists:foldl(fun({N, Result}, Acc) -> setelement(N, Acc, Result) end,
                              list_to_tuple(lists:duplicate(length(KVECs), {error, failed})),
                              Processed)).

increment_counter_timelimit(Name, Key, Value, Initial, ExpTime, Retries, TimeLimit) ->
    case pool_execute(Name,
                      increment_counter,
                      [Key, Value, Initial, ExpTime, TimeLimit],
                      TimeLimit)
    of
        {ok, ActualValue} ->
            {ok, ActualValue};
        {error, _Reason} when Retries >= 1 ->
            increment_counter_timelimit(Name, Key, Value, Initial, ExpTime, Retries - 1, TimeLimit);
        {error, Reason} ->
            {error, Reason}
    end.

async_by_shard(Name,
               ItemsGroupedByShards,
               TimeLimit,
               #async_op{op = AsyncOp,
                         op_error = AsyncOpError,
                         response = AsyncOpResponse,
                         response_error = AsyncOpResponseError}) ->
    {Processed, Errors} =
        lists:foldl(fun({ShardIdentifier, Items}, {Processed, Errors}) ->
                       begin
                           PoolName = mero_cluster:random_pool_of_shard(Name, ShardIdentifier),
                           case mero_pool:checkout(PoolName, TimeLimit) of
                               {ok, Conn} ->
                                   case mero_pool:transaction(Conn, AsyncOp, [Items]) of
                                       {error, Reason} ->
                                           mero_pool:close(Conn, AsyncOpError),
                                           mero_pool:checkin_closed(Conn),
                                           {Processed, [Reason | Errors]};
                                       {NConn, {error, Reason}} ->
                                           mero_pool:checkin(NConn),
                                           {Processed, [Reason | Errors]};
                                       {NConn, ok} ->
                                           {[{NConn, Items} | Processed], Errors}
                                   end;
                               {error, Reason} ->
                                   {Processed, [Reason | Errors]}
                           end
                       end
                    end,
                    {[], []},
                    ItemsGroupedByShards),
    {ProcessedOut, ErrorsOut} =
        lists:foldl(fun({Conn, Items}, {ProcessedIn, ErrorsIn}) ->
                       case mero_pool:transaction(Conn, AsyncOpResponse, [Items, TimeLimit]) of
                           {error, Reason} ->
                               mero_pool:close(Conn, AsyncOpResponseError),
                               mero_pool:checkin_closed(Conn),
                               {ProcessedIn, [Reason | ErrorsIn]};
                           {Client, {error, Reason}} ->
                               mero_pool:checkin(Client),
                               {ProcessedIn, [Reason | ErrorsIn]};
                           {Client, Responses} when is_list(Responses) ->
                               mero_pool:checkin(Client),
                               {Responses ++ ProcessedIn, ErrorsIn}
                       end
                    end,
                    {[], Errors},
                    Processed),
    case ErrorsOut of
        [] ->
            ProcessedOut;
        ErrorsOut ->
            {error, ErrorsOut, ProcessedOut}
    end.

%% @doc: Request a socket form the pool, uses it and returns it once finished.
pool_execute(PoolName, Op, Args, TimeLimit) when is_tuple(TimeLimit) ->
    case mero_pool:checkout(PoolName, TimeLimit) of
        {ok, Conn} ->
            case mero_pool:transaction(Conn, Op, Args) of
                {error, Reason} ->
                    mero_pool:close(Conn, sync_transaction_error),
                    mero_pool:checkin_closed(Conn),
                    {error, Reason};
                {NConn, Return} ->
                    mero_pool:checkin(NConn),
                    Return
            end;
        {error, reject} ->
            {error, reject};
        {error, Reason} ->
            {error, Reason}
    end.
