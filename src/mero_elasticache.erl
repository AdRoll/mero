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

%%%=============================================================================
%%% External functions
%%%=============================================================================
%% Given an elasticache config Endpoint and port, return parsed list of {host, port} nodes in cluster
-spec get_cluster_config(string(), integer()) -> list({string(), integer()}).
get_cluster_config(ConfigHost, ConfigPort) ->
    LineDefinitions = [banner, version, hosts, crlf, eom],
    Result = mero_elasticache:request_response(ConfigHost, ConfigPort, ?GET_CLUSTER, LineDefinitions),
    parse_cluster_config(proplists:get_value(hosts, Result)).

%%%=============================================================================
%%% Internal functions
%%%=============================================================================
request_response(Host, Port, Command, Names) ->
    Opts = [binary, {packet, line}, {active, false}],
    {ok, Socket} = gen_tcp:connect(Host, Port, Opts),
    ok = gen_tcp:send(Socket, Command),
    Lines = [{Name, begin
                        {ok, Line} = gen_tcp:recv(Socket, 0, 1000),
                        Line
                    end}
        || Name <- Names],
    ok = gen_tcp:close(Socket),
    Lines.

%% Parse host and version lines to return version and list of {host, port} cluster nodes
-spec parse_cluster_config(binary()) -> [{string(), integer()}].
parse_cluster_config(HostLine) ->
    HostSpecs = re:split(butlast(HostLine), <<" ">>),
    [begin
         [Host, _IP, Port] = re:split(HIP, "\\|"),
         {binary_to_list(Host), binary_to_integer(Port)}
     end
        || HIP <- HostSpecs].

butlast(<<>>) -> <<>>;
butlast(Bin) -> binary:part(Bin, {0, size(Bin) - 1}).

%%%===================================================================
%%% Unit tests
%%%===================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

get_cluster_config_test() ->
    VersionLine = <<"1\n">>,
    HostLine = <<"server1.cache.amazonaws.com|10.100.100.100|11211 server2.cache.amazonaws.com|10.101.101.00|11211 server3.cache.amazonaws.com|10.102.00.102|11211\n">>,
    ExpectedParse = {1, [{"server1.cache.amazonaws.com", 11211}, {"server2.cache.amazonaws.com", 11211}, {"server3.cache.amazonaws.com", 11211}]},
    ?assertEqual(ExpectedParse, parse_cluster_config(HostLine, VersionLine)).

-endif.
