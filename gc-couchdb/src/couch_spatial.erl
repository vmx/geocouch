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

-module(couch_spatial).

-export([query_view/6, query_view_count/4]).
-export([get_info/2]).
-export([compact/2, compact/3, cancel_compaction/2]).
-export([cleanup/1]).

-include("couch_db.hrl").
-include("couch_spatial.hrl").
-include_lib("vtree/include/vtree.hrl").

-record(acc, {
    meta_sent = false,
    offset,
    limit,
    skip,
    callback,
    user_acc,
    last_go = ok,
    update_seq = 0
}).


query_view(Db, DDoc, ViewName, Args, Callback, Acc0) ->
    {ok, View, Sig, Args2} = couch_spatial_util:get_view(
        Db, DDoc, ViewName, Args),
    {ok, Acc} = case Args#spatial_args.preflight_fun of
        PFFun when is_function(PFFun, 2) -> PFFun(Sig, Acc0);
        _ -> {ok, Acc0}
    end,
    spatial_fold(View, Args2, Callback, Acc).


query_view_count(Db, DDoc, ViewName, Args) ->
    {ok, View, _, Args2} = couch_spatial_util:get_view(
        Db, DDoc, ViewName, Args),

    #spatial_args{
        range = Mbb,
        geometry = QueryGeom0
    } = Args2,
    Vt = View#spatial.vtree,

    case QueryGeom0 of
    nil ->
        case Mbb of
        nil ->
            vtree_search:count_all(Vt);
        Mbb ->
            vtree_search:count_search(Vt, [Mbb])
        end;
    QueryGeom0 ->
        QueryGeom = erlgeom:to_geom(QueryGeom0),

        FoldFun = fun(#kv_node{geometry=Geom}, Acc) ->
            case condition_not_disjoint(QueryGeom, Geom) of
                true -> {ok, Acc + 1};
                false -> {ok, Acc}
            end
        end,
        vtree_search:search(Vt, [Mbb], FoldFun, 0)
    end.


get_info(Db, DDoc) ->
    {ok, Pid} = couch_index_server:get_index(couch_spatial_index, Db, DDoc),
    couch_index:get_info(Pid).


compact(Db, DDoc) ->
    compact(Db, DDoc, []).


compact(Db, DDoc, Opts) ->
    {ok, Pid} = couch_index_server:get_index(couch_spatial_index, Db, DDoc),
    couch_index:compact(Pid, Opts).


cancel_compaction(Db, DDoc) ->
    {ok, IPid} = couch_index_server:get_index(couch_spatial_index, Db, DDoc),
    {ok, CPid} = couch_index:get_compactor_pid(IPid),
    ok = couch_index_compactor:cancel(CPid),

    % Cleanup the compaction file if it exists
    {ok, State} = couch_index:get_state(IPid, 0),
    #spatial_state{
        sig = Sig,
        db_name = DbName
    } = State,
    couch_spatial_util:delete_compaction_file(DbName, Sig),
    ok.


cleanup(Db) ->
    couch_spatial_cleanup:run(Db).


spatial_fold(View, Args, Callback, UserAcc) ->
    #spatial_args{
        limit = Limit,
        skip = Skip
    } = Args,
    Acc = #acc{
        limit = Limit,
        skip = Skip,
        callback = Callback,
        user_acc = UserAcc,
        update_seq = View#spatial.update_seq
    },
    Acc2 = fold(View, fun do_fold/2, Acc, Args),
    finish_fold(Acc2, []).


fold(Index, FoldFun, InitAcc, Args) ->
    #spatial_args{
        bounds = Bounds,
        range = Mbb,
        geometry = QueryGeom0
    } = Args,

    % XXX vmx 2012-12-12: The whole tree folding should be reworked,
    %    there are way too many nested fold functions.
    WrapperFun = case QueryGeom0 of
    nil ->
        fun(Node, Acc) ->
            % NOTE vmx 2012-11-28: in Apache CouchDB the body is stored as
            %     Erlang terms
            Value = binary_to_term(Node#kv_node.body),
            Expanded = couch_spatial_util:expand_dups(
                [Node#kv_node{body=Value}], []),
            fold_fun(FoldFun, Expanded, Acc)
        end;
    QueryGeom0 ->
        QueryGeom = erlgeom:to_geom(QueryGeom0),
        fun(Node, Acc) ->
            % NOTE vmx 2012-11-28: in Apache CouchDB the body is stored as
            %     Erlang terms
            Value = binary_to_term(Node#kv_node.body),
            Expanded = couch_spatial_util:expand_dups(
                [Node#kv_node{body=Value}], []),
            Filtered = lists:filter(fun(N) ->
                case N#kv_node.geometry of
                nil ->
                    true;
                Geom ->
                    condition_not_disjoint(QueryGeom, Geom)
                end
            end, Expanded),
            fold_fun(FoldFun, Filtered, Acc)
        end
    end,

    case Mbb of
    nil ->
        vtree_search:all(Index#spatial.vtree, WrapperFun, InitAcc);
    Mbb ->
        Mbbs = case Bounds of
        nil ->
            [Mbb];
        _ ->
            couch_spatial_util:split_bbox_if_flipped(Mbb, Bounds)
        end,
        vtree_search:search(Index#spatial.vtree, Mbbs, WrapperFun, InitAcc)
    end.


% This is like a normal fold that can be interrupted in the middle
fold_fun(_Fun, [], Acc) ->
    {ok, Acc};
fold_fun(Fun, [KV|Rest], Acc) ->
    case Fun(KV, Acc) of
    {ok, Acc2} ->
        fold_fun(Fun, Rest, Acc2);
    {stop, Acc2} ->
        {stop, Acc2}
    end.

do_fold(_Kv, #acc{skip=N}=Acc) when N > 0 ->
    {ok, Acc#acc{skip=N-1, last_go=ok}};
do_fold(Kv, #acc{meta_sent=false}=Acc) ->
    #acc{
        callback = Callback,
        user_acc = UserAcc,
        update_seq = UpdateSeq
    } = Acc,
    Meta = make_meta(UpdateSeq, []),
    {Go, UserAcc2} = Callback(Meta, UserAcc),
    Acc2 = Acc#acc{meta_sent=true, user_acc=UserAcc2, last_go=Go},
    case Go of
        ok -> do_fold(Kv, Acc2);
        stop -> {stop, Acc2}
    end;
do_fold(_Kv, #acc{limit=0}=Acc) ->
    {stop, Acc};
do_fold(#kv_node{}=Node, Acc) ->
    #kv_node{
        key = Mbb,
        docid = DocId,
        geometry = Geom,
        body = Value
    } = Node,
    #acc{
        limit = Limit,
        callback = Callback,
        user_acc = UserAcc
    } = Acc,
    Row = {Mbb, DocId, Geom, Value},
    {Go, UserAcc2} = Callback({row, Row}, UserAcc),
    {Go, Acc#acc{
        limit = Limit-1,
        user_acc = UserAcc2,
        last_go = Go
    }}.


finish_fold(#acc{last_go=ok}=Acc, ExtraMeta) ->
    #acc{
        callback = Callback,
        user_acc = UserAcc,
        update_seq = UpdateSeq,
        meta_sent = MetaSent
    }=Acc,
    % Possible send meta info
    Meta = make_meta(UpdateSeq, ExtraMeta),
    {Go, UserAcc1} = case MetaSent of
        false -> Callback(Meta, UserAcc);
        _ -> {ok, UserAcc}
    end,
    % Notify callback that the fold is complete.
    {_, UserAcc2} = case Go of
        ok -> Callback(complete, UserAcc1);
        _ -> {ok, UserAcc1}
    end,
    {ok, UserAcc2};
finish_fold(Acc, _ExtraMeta) ->
    {ok, Acc#acc.user_acc}.


make_meta(UpdateSeq, Base) ->
    {meta, Base ++ [{update_seq, UpdateSeq}]}.


% Include the geometries (hence return true) if they are not disjoint
% The `QueryGeom` is already converted to an Erlgeom geometry. The `Geom` is
% still an Erlang term
condition_not_disjoint(QueryGeom, Geom) ->
    Geom2 = erlgeom:to_geom(Geom),
    not erlgeom:disjoint(QueryGeom, Geom2).
