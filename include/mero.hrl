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
-ifndef(MEMCACHERL_HRL).
-define(MEMCACHERL_HRL, true).

-define(MEMCACHE_INCREMENT, 16#05).
-define(MEMCACHE_INCREMENTQ, 16#15).
-define(MEMCACHE_GET, 16#00).
-define(MEMCACHE_GETQ, 16#09).
-define(MEMCACHE_GETK, 16#0C).
-define(MEMCACHE_GETKQ, 16#0D).
-define(MEMCACHE_SET, 16#01).
-define(MEMCACHE_ADD, 16#02).
-define(MEMCACHE_DELETE, 16#04).
-define(MEMCACHE_DELETEQ, 16#14).
-define(MEMCACHE_FLUSH_ALL, 16#08).

%%% If a connection attempt fails, or a connection is broken
-define(RECONNECT_WAIT_TIME, 200).

%%% Default timeout for instrospection functions
-define(DEFAULT_TIMEOUT, 5000).

-define(LOG_EVENT(MF, KeyAndTags), begin
                                  {StatModule, StatFunction, GlobalTags} = MF,
                                  apply(StatModule, StatFunction, [KeyAndTags ++ GlobalTags])
                                end).

-define(CALLBACK_CONTEXT(StatModule, StatFunction, ClusterName, Host, Port),
    {StatModule, StatFunction,
        [{cluster_name, ClusterName},
         {host, Host},
         {port, Port}]}).

-record(mero_item, {key,
                    value,
                    cas}).

-endif.
