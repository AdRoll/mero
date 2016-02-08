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

-export([increment_counter/7,
         get/3,
         set/5,
         delete/3,
         mdelete/3,
         add/5,
         flush_all/2
        ]).

-include_lib("mero/include/mero.hrl").


%%%=============================================================================
%%% External functions
%%%=============================================================================

increment_counter(Name, Key, Value, Initial, ExpTime, Retries, Timeout) ->
    TimeLimit = mero_conf:add_now(Timeout),
    PoolName = mero_cluster:server(Name, Key),
    increment_counter_timelimit(PoolName, Key, Value, Initial, ExpTime, Retries, TimeLimit).


set(Name, Key, Value, ExpTime, Timeout) ->
    TimeLimit = mero_conf:add_now(Timeout),
    PoolName = mero_cluster:server(Name, Key),
    pool_execute(PoolName, set, [Key, Value, ExpTime, TimeLimit], TimeLimit).


get(Name, Keys, Timeout) ->
    TimeLimit = mero_conf:add_now(Timeout),
    KeysGroupedByShards = mero_cluster:group_by_shards(Name, Keys),
    async_by_shard_mget(Name, KeysGroupedByShards, TimeLimit).


delete(Name, Key, Timeout) ->
    TimeLimit = mero_conf:add_now(Timeout),
    PoolName = mero_cluster:server(Name, Key),
    pool_execute(PoolName, delete, [Key, TimeLimit], TimeLimit).


mdelete(Name, Keys, Timeout) ->
    TimeLimit = mero_conf:add_now(Timeout),
    PoolName = mero_cluster:server(Name, Keys),
    pool_execute(PoolName, mdelete, [Keys, TimeLimit], TimeLimit).


add(Name, Key, Value, ExpTime, Timeout) ->
    TimeLimit = mero_conf:add_now(Timeout),
    PoolName = mero_cluster:server(Name, Key),
    pool_execute(PoolName, add, [Key, Value, ExpTime, TimeLimit], TimeLimit).


flush_all(Name, Timeout) ->
    TimeLimit = mero_conf:add_now(Timeout),
    [{Name, pool_execute(PoolName, flush_all, [TimeLimit], TimeLimit)} || PoolName <-
        mero_cluster:one_pool_of_each_shard_of_cluster(Name)].


%%%=============================================================================
%%% Internal functions
%%%=============================================================================

increment_counter_timelimit(Name, Key, Value, Initial, ExpTime, Retries, TimeLimit) ->
    case pool_execute(Name, increment_counter, [Key, Value, Initial, ExpTime, TimeLimit], TimeLimit) of
        {ok, ActualValue} ->
            {ok, ActualValue};
        {error, _Reason} when Retries >= 1 ->
            increment_counter_timelimit(Name, Key, Value, Initial, ExpTime, Retries - 1, TimeLimit);
        {error, Reason} ->
            {error, Reason}
    end.


async_by_shard_mget(Name, KeysGroupedByShards, TimeLimit) ->
    {Processed, Errors} =
        lists:foldr(
            fun({ShardIdentifier, Keys}, {Processed, Errors}) ->
                begin
                    PoolName = mero_cluster:random_pool_of_shard(Name, ShardIdentifier),
                    case mero_pool:checkout(PoolName, TimeLimit) of
                        {ok, Conn} ->
                            case mero_pool:transaction(Conn, async_mget, [Keys]) of
                                {error, Reason} ->
                                    mero_pool:close(Conn, async_mget_error),
                                    mero_pool:checkin_closed(Conn),
                                    {Processed, [Reason | Errors]};
                                {NConn, {error, Reason}} ->
                                    mero_pool:checkin(NConn),
                                    {Processed, [Reason | Errors]};
                                {NConn, ok} ->
                                    {[{NConn, Keys} | Processed], Errors}
                            end;
                        {error, Reason} ->
                            {Processed, [Reason | Errors]}
                    end
                end
            end,
            {[], []},
            KeysGroupedByShards),
    {ProcessedOut, ErrorsOut} =
        lists:foldr(
        fun({Conn, Keys}, {ProcessedIn, ErrorsIn}) ->
            case mero_pool:transaction(Conn, async_mget_response, [Keys, TimeLimit]) of
                {error, Reason} ->
                    mero_pool:close(Conn, async_mget_response_error),
                    mero_pool:checkin_closed(Conn),
                    {ProcessedIn, [Reason | ErrorsIn]};
                {Client, {error, Reason}} ->
                    mero_pool:checkin_closed(Client),
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
