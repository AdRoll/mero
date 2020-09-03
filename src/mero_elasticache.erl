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
%% @doc: This module implements cluster Auto Discovery for memcached
%% http://docs.aws.amazon.com/AmazonElastiCache/latest/UserGuide/AutoDiscovery.html
%%
-module(mero_elasticache).

-author('Miriam Pena <miriam.pena@adroll.com>').

%% API
-export([get_cluster_config/2]).
%% Just for testing purposes
-export([request_response/4]).

-define(GET_CLUSTER, <<"config get cluster\n">>).

-type cluster_entry() :: {Host :: string(),
                          Addr :: inet:ip_address(),
                          Port :: pos_integer()}.

-export_type([cluster_entry/0]).

%%%=============================================================================
%%% External functions
%%%=============================================================================
%% Given an elasticache config endpoint:port, returns parsed list of {host, port} nodes in cluster
-spec get_cluster_config(string(), integer()) ->
                            {ok, [{string(), integer()}]} | {error, Reason :: atom()}.
get_cluster_config(ConfigHost, ConfigPort) ->
    %% We wait for a bit before loading elasticache configuration to prevent runaway elasticache
    %% spam during error loops (which used to occur on occasion).
    timer:sleep(mero_conf:elasticache_load_config_delay()),
    LineDefinitions = [banner, version, hosts, crlf, eom],
    case mero_elasticache:request_response(ConfigHost,
                                           ConfigPort,
                                           ?GET_CLUSTER,
                                           LineDefinitions)
        of
        {ok, Result} ->
            case parse_cluster_config(proplists:get_value(hosts, Result)) of
                {error, Reason} ->
                    {error, Reason};
                {ok, Config} ->
                    {ok, [{Host, Port} || {Host, _IPAddr, Port} <- Config]}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================
request_response(Host, Port, Command, Names) ->
    Opts = [binary, {packet, line}, {active, false}, {recbuf, 5000}],
    %% @see https://github.com/erlang/otp/pull/2191
    %%      Even with Timeout == infinity, connect attempts may result in {error, etimedout}
    case gen_tcp:connect(Host, Port, Opts) of
        {ok, Socket} ->
            ok = gen_tcp:send(Socket, Command),
            Lines = receive_lines(Names, Socket),
            ok = gen_tcp:close(Socket),
            Lines;
        Error ->
            Error
    end.

-spec receive_lines([atom()], gen_tcp:socket()) ->
                       {ok, [{atom(), binary()}]} | {error, atom()}.
receive_lines(Names, Socket) ->
    receive_lines(Names, Socket, []).

receive_lines([], _Socket, Lines) ->
    {ok, lists:reverse(Lines)};
receive_lines([Name | Names], Socket, Acc) ->
    case gen_tcp:recv(Socket, 0, 10000) of
        {ok, Line} ->
            receive_lines(Names, Socket, [{Name, Line} | Acc]);
        {error, Error} ->
            {error, Error}
    end.

%% Parse host and version lines to return version and list of {host, port} cluster nodes
-spec parse_cluster_config(binary()) ->
                              {ok, Config :: [cluster_entry()]} | {error, Reason :: atom()}.
parse_cluster_config(HostLine) ->
    %% Strip any newlines
    Entries = binary:replace(HostLine, <<"\n">>, <<>>),

    %% Break entries on spaces and convert to charlists
    Entries1 = re:split(Entries, <<" ">>, [{return, list}]),

    %% Parse & validate individual entries
    parse_cluster_entries(Entries1, []).

parse_cluster_entries([], Accum) ->
    {ok, lists:reverse(Accum)};
parse_cluster_entries([H | T], Accum) ->
    case string:tokens(H, "|") of
        [Host, IP, Port] ->
            case inet:parse_ipv4_address(IP) of
                {ok, IPAddr} ->
                    case catch erlang:list_to_integer(Port) of
                        {'EXIT', _} ->
                            {error, bad_port};
                        P when P < 1 orelse P > 65535 ->
                            {error, bad_port};
                        P ->
                            parse_cluster_entries(T, [{Host, IPAddr, P} | Accum])
                    end;
                {error, _} ->
                    {error, bad_ip}
            end;
        _BadClusterEntry ->
            {error, bad_cluster_entry}
    end.

%%%===================================================================
%%% Unit tests
%%%===================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

get_cluster_config_test() ->
    HostLine =
        <<"server1.cache.amazonaws.com|10.100.100.100|11211 server2.cache.amazo"
          "naws.com|10.101.101.0|11211 server3.cache.amazonaws.com|10.102.00.10"
          "2|11211\n">>,
    ExpectedParse =
        [{"server1.cache.amazonaws.com", {10, 100, 100, 100}, 11211},
         {"server2.cache.amazonaws.com", {10, 101, 101, 0}, 11211},
         {"server3.cache.amazonaws.com", {10, 102, 0, 102}, 11211}],
    ?assertEqual({ok, ExpectedParse}, parse_cluster_config(HostLine)).

get_bad_ip_addr_config_test() ->
    HostLine =
        <<"server1.cache.amazonaws.com|10.100.100.100|11211 server2.cache.amazo"
          "naws.com|10.101.101.|11211 server3.cache.amazonaws.com|10.102.00.102"
          "|11211\n">>,
    ?assertEqual({error, bad_ip}, parse_cluster_config(HostLine)).

get_non_integer_port_config_test() ->
    HostLine =
        <<"server1.cache.amazonaws.com|10.100.100.100|11211 server2.cache.amazo"
          "naws.com|10.101.101.0|11211 server3.cache.amazonaws.com|10.102.00.10"
          "2|1l211\n">>,
    ?assertEqual({error, bad_port}, parse_cluster_config(HostLine)).

get_bad_low_port_config_test() ->
    HostLine =
        <<"server1.cache.amazonaws.com|10.100.100.100|0 server2.cache.amazonaws"
          ".com|10.101.101.0|11211 server3.cache.amazonaws.com|10.102.00.102|11"
          "211\n">>,
    ?assertEqual({error, bad_port}, parse_cluster_config(HostLine)).

get_bad_high_port_config_test() ->
    HostLine =
        <<"server1.cache.amazonaws.com|10.100.100.100|72000 server2.cache.amazo"
          "naws.com|10.101.101.0|11211 server3.cache.amazonaws.com|10.102.00.10"
          "2|11211\n">>,
    ?assertEqual({error, bad_port}, parse_cluster_config(HostLine)).

get_bad_entry_config_test() ->
    HostLine =
        <<"server1.cache.amazonaws.com|10.100.100.100|11211 server2.cache.amazo"
          "naws.com|10.101.101.0| server3.cache.amazonaws.com|10.102.00.102|112"
          "11\n">>,
    ?assertEqual({error, bad_cluster_entry}, parse_cluster_config(HostLine)).

-endif.
