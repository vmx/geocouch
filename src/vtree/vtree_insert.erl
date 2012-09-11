% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

% This module implements the insertion into the vtree. It follows the normal
% R-tree rules and is implementation independent. It just calls out to
% modules for the choosing the correct subtree and splitting the nodes.

-module(vtree_insert).

-include("vtree.hrl").
-include("couch_db.hrl").

-export([insert/2]).

-ifdef(makecheck).
-compile(export_all).
-endif.


% Insert into a new empty tree
%-spec insert(VT::#vtree{}, NewNode::new_node()) ->
%                    {Pointer::integer(), Size::integer()}.
%insert(#vtree{root=nil}=Vt, Mbb, {_DocId, _Geom, _Body}=Value) ->
insert(#vtree{root=nil}=Vt, Nodes) ->
T1 = now(),
    % If we would do single inserts, the first node that was inserted would
    % have set the original Mbb `MbbO`
    MbbO = get_key(hd(Nodes)),
    %{PointerNode, TreeSize} = write_nodes(Vt, ?KV_NODE, Nodes, MbbO),
    %case write_nodes(Vt, ?KV_NODE, Nodes, MbbO) of
    %    %{ok, [{PointerNode, TreeSize, Reduce}] ->
    %    % XXX vmx 2012-09-03: The reduce value might need to be converted to
    %    %    binary
    %    {ok, [{_Mbb, Root}]} ->
    %    %{ok, [{PointerNode, TreeSize, Reduce}] ->
    %        % XXX No idea where the reduce value will come from, yet
    %        %Reduce = nil,
    %        %BinReduce = erlang:?term_to_bin(Reduce),
    %        %_SizeReduce = erlang:iolist_size(BinReduce),
    %        %Root = {PointerNode, TreeSize, Reduce},
    %        % XXX TODO write header to disk with root information
    %        Vt#vtree{root=Root};
    %    {ok, KpNodes} ->
    %        write_new_root(Vt, KpNodes)
    %end.
    KpNodes = write_nodes(Vt, Nodes, MbbO),
    Root = write_new_root(Vt, KpNodes, MbbO),
io:format("Insertion took: ~ps~n", [timer:now_diff(now(), T1)/1000000]),
    Vt#vtree{root=Root};
%insert(#vtree{root=_Root}=Vt, Mbb, {_DocId, _Geom, _Body}=Value) ->
insert(Vt, Nodes) ->
    MbbO = vtree_util:nodes_mbb(Nodes, Vt#vtree.less),
    write_nodes(Vt, Nodes, MbbO).


% Write a new root node for the given nodes. In case there are more than
% `fill_max` nodes, write a new root recursively. Stop when the root is a
% single node.
-spec write_new_root(Vt :: #vtree{}, Nodes :: [#kp_node{} | #kv_node{}],
                     MbbO :: mbb()) -> #kp_node{}.
write_new_root(_Vt, [Root], _MbbO) ->
    Root;
% The `write_nodes/3` call will handle the splitting if needed. It could
% happen that the number of nodes returned by `write_nodes/3` is bigger
% than `fill_max`, hence the recursive call.
write_new_root(Vt, Nodes, MbbO) ->
    WrittenNodes = write_nodes(Vt, Nodes, MbbO),
    write_new_root(Vt, WrittenNodes, MbbO).


% Add a list of nodes one by one into a list of nodes (think of the latter
% list as list containing child nodes). The node the new nodes get inserted
% to, will automatically be split.
% The result will again be a list of multiple nodes, with a maximum of
% `fill_max` nodes (given by #vtree{}). The total number of elements in the
% resulting list can be bigger than `fill_max`, hence you might need to call
% it recursively.
-spec insert_into_nodes(Vt :: #vtree{},
                        NodeParitions :: [[#kv_node{} | #kp_node{}]],
                        MbbO :: mbb(), ToInsert :: [#kv_node{} | #kp_node{}])
                       -> [#kv_node{} | #kp_node{}].
insert_into_nodes(_Vt, NodePartitions, _MbbO, []) ->
    NodePartitions;
insert_into_nodes(Vt, NodePartitions, MbbO, [ToInsert|Rest]) ->
    #vtree{
            fill_max = FillMax,
            less = Less
          } = Vt,
    Mbb = get_key(ToInsert),

    % Every node partition contains a list of nodes, the maximum number is
    % `fill_max`
    % Start with calculating the MBBs of the partitions
    PartitionMbbs = [vtree_util:nodes_mbb(Nodes, Less) ||
                        Nodes <- NodePartitions],

    % Choose the partition the new node should be inserted to.
    % vtree_choode:choose_subtree/3 expects a list of 2-tuples with the MBB
    % and any value you like. We use the index in the list as second element
    % in the tuple, so we can insert the new nodes there easily.
    NodesNumbered = lists:zip(PartitionMbbs,
                              lists:seq(0, length(PartitionMbbs)-1)),
    {_, NodeIndex} = vtree_choose:choose_subtree(NodesNumbered, Mbb, Less),
    {A, [Nth|B]} = lists:split(NodeIndex, NodePartitions),
    NewNodes = case length(Nth) =:= FillMax of
                   % Maximum number of nodes reached, hence split it
                   true ->
                       {C, D} = split_node(Vt, [ToInsert|Nth], MbbO),
                       A ++ [C, D] ++ B;
                   % No need to split the node, just insert the new one
                   false ->
                       C = [ToInsert|Nth],
                       A ++ [C] ++ B
               end,
    insert_into_nodes(Vt, NewNodes, MbbO, Rest).


-spec get_key(Node :: #kv_node{} | #kp_node{}) -> mbb().
get_key(#kv_node{}=Node) ->
    Node#kv_node.key;
get_key(#kp_node{}=Node) ->
    Node#kp_node.key.


%The commit to be http://review.couchbase.org/#patch,sidebyside,13557
% http://review.couchbase.org/#/c/17918/
% https://github.com/couchbase/couchdb/commit/9cb569466b7442d6285feaa2fd03c134b4978b92
% `MbbO` is the original MBB when it was create the first time
% It will return a list of KP-nodes. It might return more than `fill_max`
% nodes (but the number of nodes per node will be limited to `fill_max`).
-spec write_nodes(Vt :: #vtree{}, Nodes :: [#kv_node{} | #kp_node{}],
                  MbbO :: mbb()) -> [#kp_node{}].
%                        {ok, [{mbb(), kp_value()|kv_value()}]}.
%                         {ok, [{mbb(), kp_value()}]}.
%                        {ok, [{mbb(), integer(), integer(), any()}]}.
write_nodes(#vtree{fill_max = FillMax} = Vt, Nodes, MbbO) when
      length(Nodes) =:= FillMax+1 ->
    {NodesA, NodesB} = split_node(Vt, Nodes, MbbO),
    write_multiple_nodes(Vt, [NodesA, NodesB]);
% Too many nodes for a single split, hence do something smart to get all
% nodes stored in a good way. First take the first FillMax nodes and
% split then into two nodes. Then insert all other nodes one by one into
% one of the two newly created nodes. The decision which node to choose is
% done by vtree_choose:choose_subtree/3.
write_nodes(#vtree{fill_max = FillMax} = Vt, Nodes, MbbO) when
      length(Nodes) > FillMax+1 ->
    {FirstNodes, Rest} = lists:split(FillMax, Nodes),
    %{NodesA, NodesB} = split_node(Vt, NodeType, FirstNodes, MbbO),
    NewNodes = insert_into_nodes(Vt, [FirstNodes], MbbO, Rest),
    % XXX vmx 2012-08-09: The case where length(`NewNodes`) > fill_max is missing
    write_multiple_nodes(Vt, NewNodes);
write_nodes(Vt, Nodes, _MbbO) ->
    write_multiple_nodes(Vt, [Nodes]).


% Write multiple nodes at once
-spec write_multiple_nodes(Vt :: #vtree{},
                           NodeList :: [[#kp_node{} | #kv_node{}]]) ->
                                  [#kp_node{}].
write_multiple_nodes(Vt, NodeList) ->
    #vtree{
            fd = Fd,
            less = Less
          } = Vt,
    lists:map(
      fun(Nodes) ->
              {ok, WrittenNodes} = vtree_io:write_node(Fd, Nodes, Less),
              WrittenNodes
      end, NodeList).


% Splits a KV- or KP-Node. Needs to be called with `fill_max+1` Nodes. It
% operates with #kv_node and #kp_node records and also returns those.
-spec split_node(Vt :: #vtree{}, Nodes :: [#kv_node{} | #kp_node{}],
                 MbbO :: mbb()) -> {[#kv_node{}], [#kv_node{}]} |
                                   {[#kp_node{}], [#kp_node{}]}.
split_node(Vt, [#kv_node{}|_]=Nodes, MbbO) ->
    #vtree{
            fill_min = FillMin,
            fill_max = FillMax,
            less = Less
          } = Vt,
    SplitNodes = [{Node#kv_node.key, Node} || Node <- Nodes],
    {SplitNodesA, SplitNodesB} = vtree_split:split_leaf(
                                   SplitNodes, MbbO, FillMin, FillMax, Less),
    {_, NodesA} = lists:unzip(SplitNodesA),
    {_, NodesB} = lists:unzip(SplitNodesB),
    {NodesA, NodesB};
split_node(Vt, [#kp_node{}|_]=Nodes, MbbO) ->
    #vtree{
            fill_min = FillMin,
            fill_max = FillMax,
            less = Less
          } = Vt,
    SplitNodes = [{Node#kp_node.key, Node} || Node <- Nodes],
    {SplitNodesA, SplitNodesB} = vtree_split:split_inner(
                                   SplitNodes, MbbO, FillMin, FillMax, Less),
    {_, NodesA} = lists:unzip(SplitNodesA),
    {_, NodesB} = lists:unzip(SplitNodesB),
    {NodesA, NodesB}.
