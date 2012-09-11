#!/usr/bin/env escript
%% -*- erlang -*-
%%! -na me insert@127.0.0.1

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

-include_lib("../include/vtree.hrl").

-define(MOD, vtree_insert).
-define(FILENAME, "/tmp/vtree_insert_vtree.bin").

main(_) ->
    io:format("vmx: node: ~p~n", [node()]),
    io:format("vmx: cookie: ~p~n", [erlang:get_cookie()]),
    %spawn(etop:start()),
    % Set the random seed once, for the whole test suite
    random:seed(1, 11, 91),

    code:add_pathz(filename:dirname(escript:script_name())),
    etap:plan(1),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            % Somehow etap:diag/1 and etap:bail/1 don't work properly
            %etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            %etap:bail(Other),
            io:format(standard_error, "Test died abnormally:~n~p~n", [Other])
     end.


test() ->
    couch_file_write_guard:sup_start_link(),
    test_insert(),
    %test_write_new_root(),
    %test_insert_into_nodes(),
    %test_get_key(),
    %test_write_nodes(),
    %test_write_multiple_nodes(),
    %test_split_node(),
    ok.


test_insert() ->
    Fd = vtree_test_util:create_file(?FILENAME),
    Nodes1 = vtree_test_util:generate_kvnodes(6),

    Vtree1 = #vtree{
      fd = Fd,
      fill_min = 4,
      fill_max = 8,
      less = fun(A, B) -> A < B end
     },

    NewVtree1 = ?MOD:insert(Vtree1, [hd(Nodes1)]),
    Root1 = NewVtree1#vtree.root,
    io:format("root1: ~p~n", [Root1]),
    [InsertedNodes1] = vtree_io:read_node(Fd, Root1#kp_node.childpointer),
    etap:is(InsertedNodes1, hd(Nodes1), "Single node got inserted"),

    NewVtree2 = ?MOD:insert(Vtree1, Nodes1),
    Root2 = NewVtree2#vtree.root,
    InsertedNodes2 = vtree_io:read_node(Fd, Root2#kp_node.childpointer),
    etap:is(InsertedNodes2, Nodes1, "All 6 nodes got inserted"),

    Nodes2 = vtree_test_util:generate_kvnodes(1000),
    io:format("generated 1000 kvnodes~n", []),
    NewVtree3 = ?MOD:insert(Vtree1, Nodes2),
    io:format("inserted 1000 kvnodes~n", []),
    io:format("NewVtree3:~n~p~n", [NewVtree3]),
    %Root3 = NewVtree3#vtree.root,
    %InsertedNodes2 = vtree_io:read_node(Fd, Root2#kp_node.childpointer),
    %etap:is(InsertedNodes2, Nodes1, "All 6 nodes got inserted"),

    couch_file:close(Fd).


test_write_new_root() ->
    Less = fun(A, B) -> A < B end,
    Fd = vtree_test_util:create_file(?FILENAME),

    Vtree1 = #vtree{
      fill_min = 2,
      fill_max = 4,
      less = Less,
      fd = Fd
     },

    NodesKp = vtree_test_util:generate_kpnodes(5),
    MbbOKp = (hd(NodesKp))#kp_node.key,

    Root1 = ?MOD:write_new_root(Vtree1, [hd(NodesKp)], MbbOKp),
    etap:is(Root1, hd(NodesKp), "Single node is new root"),

    Root2 = ?MOD:write_new_root(Vtree1, tl(NodesKp), MbbOKp),
    Root2Children = vtree_io:read_node(Fd, Root2#kp_node.childpointer),
    etap:is(Root2Children, tl(NodesKp),
            "A new root node was written (one new level)"),

    Root3 = ?MOD:write_new_root(Vtree1, NodesKp, MbbOKp),
    Root3Children = vtree_io:read_node(Fd, Root3#kp_node.childpointer),
    ChildPointer = [C#kp_node.childpointer || C <- Root3Children],
    Root3ChildrenChildren = lists:append(
                              [vtree_io:read_node(Fd, C#kp_node.childpointer)
                               || C <- Root3Children]),
    etap:is(lists:sort(Root3ChildrenChildren), lists:sort(NodesKp),
            "A new root node was written (two new levels)"),

    NodesKv = vtree_test_util:generate_kvnodes(4),
    MbbOKv = (hd(NodesKv))#kv_node.key,

    Root4 = ?MOD:write_new_root(Vtree1, NodesKv, MbbOKv),
    Root4Children = vtree_io:read_node(Fd, Root4#kp_node.childpointer),
    etap:is(Root4Children, NodesKv,
            "A new root node (for KV-nodes) was written (one new level)"),

    couch_file:close(Fd).


test_insert_into_nodes() ->
    Tests = [
             {2, 4},
             {3, 6},
             {10, 30}
            ],
    lists:foreach(fun({FillMin, FillMax}) ->
                          insert_into_nodes(FillMin, FillMax)
                  end, Tests).

insert_into_nodes(FillMin, FillMax) ->
    Less = fun(A, B) -> A < B end,

    Vtree = #vtree{
      fill_min = FillMin,
      fill_max = FillMax,
      less = Less
     },

    Nodes1 = vtree_test_util:generate_kpnodes(4),
    Nodes2 = vtree_test_util:generate_kpnodes(20),
    MbbO = (hd(Nodes1))#kp_node.key,

    Inserted = ?MOD:insert_into_nodes(Vtree, [Nodes1], MbbO, Nodes2),
    etap:is(length(lists:append(Inserted)), 24,
            "All nodes got inserted"),
    NodesFilledOk = lists:all(
                      fun(N) ->
                              length(N) >= Vtree#vtree.fill_min andalso
                                  length(N) =< Vtree#vtree.fill_max
                      end, Inserted),
    etap:ok(NodesFilledOk,
            "All child nodes have the right number of nodes"),
    etap:ok(length(Inserted) >= 24/FillMax andalso
            length(Inserted) =< 24/FillMin,
            "Right number of child nodes").


test_get_key() ->
    [KvNode] = vtree_test_util:generate_kvnodes(1),
    [KpNode] = vtree_test_util:generate_kpnodes(1),

    etap:is(?MOD:get_key(KvNode), KvNode#kv_node.key,
            "Returns the key of a KV-node"),
    etap:is(?MOD:get_key(KpNode), KpNode#kp_node.key,
            "Returns the key of a KP-node").


test_write_nodes() ->
    Tests = [
             {4, 1, "Less then the maximum number of nodes were written"},
             {5, 1, "The maximum number of nodes were written"},
             {6, 2, "One more than maximum number of nodes were written"},
             {8, 2, "A bit more than maximum number of nodes were written"},
             % The expected 9 nodes depend on the choose_subtree algorithm
             {30, 9, "Way more than maximum number of nodes were written"}
            ],
    lists:foreach(fun({Insert, Expected, Message}) ->
                          write_nodes(Insert, Expected, Message)
                  end, Tests).

write_nodes(Insert, Expected, Message) ->
    Less = fun(A, B) -> A < B end,
    Fd = vtree_test_util:create_file(?FILENAME),

    Vtree = #vtree{
      fill_min = 2,
      fill_max = 5,
      less = Less,
      fd = Fd
     },

    Nodes = vtree_test_util:generate_kpnodes(Insert),
    MbbO = (hd(Nodes))#kp_node.key,
    WrittenNodes = ?MOD:write_nodes(Vtree, Nodes, MbbO),
    etap:is(length(WrittenNodes), Expected, Message),
    etap:ok(lists:all(fun(#kp_node{}) -> true; (_) -> false end, WrittenNodes),
            "The return values are KP-nodes"),
    couch_file:close(Fd).


test_write_multiple_nodes() ->
    Less = fun(A, B) -> A < B end,
    Fd1a = vtree_test_util:create_file(?FILENAME),

    Vtree1 = #vtree{
      fill_min = 2,
      fill_max = 4,
      less = Less,
      fd = Fd1a
     },

    Nodes1a = vtree_test_util:generate_kvnodes(5),
    Nodes1b = vtree_test_util:generate_kvnodes(8),
    WrittenNodes1 = ?MOD:write_multiple_nodes(Vtree1, [Nodes1a, Nodes1b]),
    couch_file:close(Fd1a),
    Fd1b = vtree_test_util:create_file(?FILENAME),
    {ok, WrittenNodes1a} = vtree_io:write_node(Fd1b, Nodes1a, Less),
    {ok, WrittenNodes1b} = vtree_io:write_node(Fd1b, Nodes1b, Less),
    etap:is(WrittenNodes1, [WrittenNodes1a, WrittenNodes1b],
            "Multiple KV-nodes were correctly written"),
    couch_file:close(Fd1b),

    Fd2a = vtree_test_util:create_file(?FILENAME),
    Vtree2 = Vtree1#vtree{fd = Fd2a},
    Nodes2a = vtree_test_util:generate_kpnodes(5),
    Nodes2b = vtree_test_util:generate_kpnodes(8),
    WrittenNodes2 = ?MOD:write_multiple_nodes(Vtree2, [Nodes2a, Nodes2b]),
    couch_file:close(Fd2a),
    Fd2b = vtree_test_util:create_file(?FILENAME),
    {ok, WrittenNodes2a} = vtree_io:write_node(Fd2b, Nodes2a, Less),
    {ok, WrittenNodes2b} = vtree_io:write_node(Fd2b, Nodes2b, Less),
    etap:is(WrittenNodes2, [WrittenNodes2a, WrittenNodes2b],
            "Multiple KP-nodes were correctly written"),
    couch_file:close(Fd2b).


test_split_node() ->
    Vtree = #vtree{
      fill_min = 3,
      fill_max = 6
     },
    KvNodes = vtree_test_util:generate_kvnodes(7),
    KpNodes = vtree_test_util:generate_kpnodes(7),
    KvMbbO = (hd(KvNodes))#kv_node.key,
    KpMbbO = (hd(KpNodes))#kp_node.key,
    NodesFilledOkFun = fun(N) ->
                               length(N) >= Vtree#vtree.fill_min andalso
                                   length(N) =< Vtree#vtree.fill_max
                       end,

    {KvNodesA, KvNodesB} = ?MOD:split_node(Vtree, KvNodes, KvMbbO),
    KvNodesFilledOk = lists:all(NodesFilledOkFun, [KvNodesA, KvNodesB]),
    etap:ok(KvNodesFilledOk,
            "Both partitions contain the right number of nodes"),
    etap:is(lists:sort(lists:append([KvNodesA, KvNodesB])),
            lists:sort(KvNodes),
            "All nodes are still there after the split"),

    {KpNodesA, KpNodesB} = ?MOD:split_node(Vtree, KpNodes, KpMbbO),
    KpNodesFilledOk = lists:all(NodesFilledOkFun, [KpNodesA, KpNodesB]),
    etap:ok(KpNodesFilledOk,
            "Both partitions contain the right number of nodes"),
    etap:is(lists:sort(lists:append([KpNodesA, KpNodesB])),
            lists:sort(KpNodes),
            "All nodes are still there after the split").
