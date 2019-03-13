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
-module(mero_sup).

-author('Miriam Pena <miriam.pena@adroll.com>').

-export([start_link/1,
         init/1]).

-behaviour(supervisor).

-ignore_xref([{mero_cluster, size,0},
              {mero_cluster, cluster_size, 0},
              {mero_cluster, pools, 0},
              {mero_cluster, server, 1}]).

%%%===================================================================
%%% API functions
%%%===================================================================

%% @doc: Starts a list of workers with the configuration generated on
%% mero_cluster
-spec start_link(ClusterConfig :: [{ClusterName :: atom(), Config :: proplists:proplist()}]) ->
    {ok, Pid :: pid()} | {error, Reason :: term()}.
start_link(Config) ->
    ClusterConfig = mero_conf:process_server_specs(Config),
    ok = mero_cluster:load_clusters(ClusterConfig),
    ClusterDefs = [{
        Cluster,
        mero_cluster:sup_by_cluster_name(Cluster),
        mero_cluster:child_definitions(Cluster)
    } || {Cluster, _} <- ClusterConfig],
    supervisor:start_link({local, ?MODULE}, ?MODULE, ClusterDefs).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init(ClusterDefs) ->
    Children = [child(Cluster, SupName, PoolDefs) || {Cluster, SupName, PoolDefs} <- ClusterDefs],
    {ok, {{one_for_one, 10, 10}, Children}}.

child(ClusterName, SupName, PoolDefs) ->
    {
        ClusterName,
        {mero_cluster_sup, start_link, [ClusterName, SupName, PoolDefs]},
        permanent,
        5000,
        supervisor,
        [mero_cluster_sup]
    }.
