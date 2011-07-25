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

-module(couch_spatial_merger).

%-export([query_spatial/2]).
-export([parse_http_params/4, make_funs/5, get_skip_and_limit/1,
    http_index_folder_req_details/3]).

-include("couch_db.hrl").
-include("couch_merger.hrl").
-include("couch_spatial.hrl").
%-include("couch_spatial_merger.hrl").

-define(LOCAL, <<"local">>).

% callback!
parse_http_params(Req, _DDoc, _IndexName, _Extra) ->
    couch_httpd_spatial:parse_spatial_params(Req).

% callback!
make_funs(_Req, _DDoc, _IndexName, _IndexArgs, _IndexMergeParams) ->
    {nil, fun spatial_folder/5, fun merge_spatial/1,
    fun(NumFolders, Callback, UserAcc) ->
        {ok, UserAcc2} = Callback(start, UserAcc),
        couch_merger:collect_rows(fun spatial_row_obj/1, Callback, UserAcc2)
    end, nil}.

% callback!
get_skip_and_limit(_SpatialArgs) ->
    % GeoCouch doesn't currently neither support skip, nor limit
    Defaults = #merge_params{},
    {Defaults#merge_params.skip, Defaults#merge_params.limit}.

% callback!
http_index_folder_req_details(#merged_view_spec{
        url = MergeUrl0, ejson_spec = EJson},
        #index_merge{conn_timeout = Timeout}, SpatialArgs) ->
    {ok, #httpdb{url = Url, ibrowse_options = Options} = Db} =
        couch_merger:open_db(MergeUrl0, nil, Timeout),
    MergeUrl = Url ++ spatial_qs(SpatialArgs),
    Headers = [{"Content-Type", "application/json"} | Db#httpdb.headers],
    put(from_url, Url),
    {MergeUrl, post, Headers, ?JSON_ENCODE(EJson), Options};

http_index_folder_req_details(#simple_view_spec{
        database = DbUrl, ddoc_id = DDocId, view_name = SpatialName},
        #index_merge{conn_timeout = Timeout}, SpatialArgs) ->
    {ok, #httpdb{url = Url, ibrowse_options = Options}} =
        couch_merger:open_db(DbUrl, nil, Timeout),
    SpatialUrl = Url ++ ?b2l(DDocId) ++ "/_spatial/" ++ ?b2l(SpatialName) ++
        spatial_qs(SpatialArgs),
    put(from_url, DbUrl),
    {SpatialUrl, get, [], [], Options}.

spatial_row_obj({{Key, error}, Value}) ->
    {[{key, Key}, {error, Value}]};

spatial_row_obj({{Bbox, DocId}, {Geom, Value}}) ->
    {[{id, DocId}, {bbox, tuple_to_list(Bbox)}, {geometry, {[Geom]}},
        {value, Value}]}.


% Counterpart to map_view_folder/6 in couch_view_merger
spatial_folder(#simple_view_spec{database = <<"http://", _/binary>>} =
        SpatialSpec, MergeParams, _UserCtx, SpatialArgs, Queue) ->
    EventFun = make_event_fun(Queue),
    couch_merger:http_index_folder(couch_spatial_merger, SpatialSpec,
        MergeParams, SpatialArgs, Queue, EventFun);

spatial_folder(#simple_view_spec{database = <<"https://", _/binary>>} =
        SpatialSpec, MergeParams, _UserCtx, SpatialArgs, Queue) ->
    EventFun = make_event_fun(Queue),
    couch_merger:http_index_folder(couch_spatial_merger, SpatialSpec,
        MergeParams, SpatialArgs, Queue, EventFun);

spatial_folder(#merged_view_spec{} = SpatialSpec,
                MergeParams, _UserCtx, SpatialArgs, Queue) ->
    EventFun = make_event_fun(Queue),
    couch_merger:http_index_folder(couch_spatial_merger, SpatialSpec,
        MergeParams, SpatialArgs, Queue, EventFun);

spatial_folder(SpatialSpec, _MergeParams, UserCtx, SpatialArgs, Queue) ->
    #simple_view_spec{
        database = DbName, ddoc_database = DDocDbName,
        ddoc_id = DDocId, view_name = SpatialName
    } = SpatialSpec,
    #spatial_query_args{
        bbox = Bbox,
        bounds = Bounds,
        stale = Stale
    } = SpatialArgs,
    case couch_db:open(DbName, [{user_ctx, UserCtx}]) of
    {ok, Db} ->
        try
            FoldlFun = make_spatial_fold_fun(Queue),
            {DDocDb, Index} = get_spatial_index(Db, DDocDbName, DDocId,
                SpatialName, Stale),
            {ok, RowCount} = couch_spatial:get_item_count(Index#spatial.fd,
                Index#spatial.treepos),
            ok = couch_view_merger_queue:queue(Queue, {row_count, RowCount}),
            couch_spatial:fold(Index, FoldlFun, {undefined, ""}, Bbox, Bounds),
            catch couch_db:close(DDocDb)
        catch
        {not_found, Reason} when Reason =:= missing; Reason =:= deleted ->
            ok = couch_view_merger_queue:queue(
                Queue, {error, ?LOCAL,
                    couch_view_merger:ddoc_not_found_msg(DbName, DDocId)});
        ddoc_db_not_found ->
            ok = couch_view_merger_queue:queue(
                Queue, {error, ?LOCAL,
                    couch_view_merger:ddoc_not_found_msg(DDocDbName, DDocId)});
        _Tag:Error ->
            couch_view_merger_queue:queue(Queue,
                {error, ?LOCAL, couch_util:to_binary(Error)})
        after
            ok = couch_view_merger_queue:done(Queue),
            couch_db:close(Db)
        end;
    {not_found, _} ->
        ok = couch_view_merger_queue:queue(
               Queue, {error, ?LOCAL,
                   couch_view_merger:db_not_found_msg(DbName)}),
        ok = couch_view_merger_queue:done(Queue)
    end.

get_spatial_index(Db, DDocDbName, DDocId, SpatialName, Stale) ->
    GroupId = case DDocDbName of
    nil ->
        DDocDb = nil,
        DDocId;
    _ when is_binary(DDocDbName) ->
        DDocDb = case couch_db:open_int(DDocDbName, []) of
        {ok, DDocDb1} ->
            DDocDb1;
        {not_found, _} ->
            throw(ddoc_db_not_found)
        end,
        {DDocDb, DDocId}
    end,
    {ok, Index, _Group} = couch_spatial:get_spatial_index(Db, GroupId,
        SpatialName, Stale),
    {DDocDb, Index}.


make_event_fun(Queue) ->
    fun(Ev) ->
        %couch_view_merger:http_view_fold(Ev, map, Queue)
        http_view_fold(Ev, Queue)
    end.

http_view_fold(object_start, Queue) ->
    ok = couch_view_merger_queue:queue(Queue, {row_count, 0}),
    fun(Ev) -> http_view_fold_rows_1(Ev, Queue) end.

http_view_fold_rows_1({key, <<"rows">>}, Queue) ->
    fun(array_start) -> fun(Ev) -> http_view_fold_rows_2(Ev, Queue) end end;
http_view_fold_rows_1(_Ev, Queue) ->
    fun(Ev) -> http_view_fold_rows_1(Ev, Queue) end.

http_view_fold_rows_2(array_end, Queue) ->
    fun(Ev) -> http_view_fold_errors_1(Ev, Queue) end;
http_view_fold_rows_2(object_start, Queue) ->
    fun(Ev) ->
        json_stream_parse:collect_object(
            Ev,
            fun(Row) ->
                http_view_fold_queue_row(Row, Queue),
                fun(Ev2) -> http_view_fold_rows_2(Ev2, Queue) end
            end)
    end.

http_view_fold_errors_1({key, <<"errors">>}, Queue) ->
    fun(array_start) -> fun(Ev) -> http_view_fold_errors_2(Ev, Queue) end end;
http_view_fold_errors_1(_Ev, _Queue) ->
    fun couch_view_merger:void_event/1.

http_view_fold_errors_2(array_end, _Queue) ->
    fun couch_view_merger:void_event/1;
http_view_fold_errors_2(object_start, Queue) ->
    fun(Ev) ->
        json_stream_parse:collect_object(
            Ev,
            fun(Error) ->
                couch_view_merger:http_view_fold_queue_error(Error, Queue),
                fun(Ev2) -> http_view_fold_errors_2(Ev2, Queue) end
            end)
    end.

http_view_fold_queue_row({Props}, Queue) ->
    Id = couch_util:get_value(<<"id">>, Props, nil),
    Bbox = couch_util:get_value(<<"bbox">>, Props, null),
    {Geom} = couch_util:get_value(<<"geometry">>, Props, null),
    Val = couch_util:get_value(<<"value">>, Props),
    Row = case couch_util:get_value(<<"error">>, Props, nil) of
    nil ->
        GeomType = couch_util:get_value(<<"type">>, Geom),
        Coords = couch_util:get_value(<<"coordinates">>, Geom),
        case couch_util:get_value(<<"doc">>, Props, nil) of
        nil ->
            {{list_to_tuple(Bbox), Id}, {{GeomType,Coords}, Val}};
        % NOTE vmx 20110818: GeoCouch doesn't support include_docs atm,
        %     but I'll just leave the code here
        Doc ->
            {{list_to_tuple(Bbox), Id}, {{GeomType,Coords}, Val}, {doc, Doc}}
        end;
    Error ->
        % error in a map row
        {{list_to_tuple(Bbox), error}, Error}
    end,
    ok = couch_view_merger_queue:queue(Queue, Row).



% Counterpart to  make_map_fold_fun/4 in couch_view_merger
make_spatial_fold_fun(Queue) ->
    fun({{_Bbox, _DocId}, {_Geom, _Value}}=Row, Acc) ->
        ok = couch_view_merger_queue:queue(Queue, Row),
        {ok, Acc}
    end.

% Counterpart to merge_map_views/6 in couch_view_merger
merge_spatial(#merge_params{limit = 0} = Params) ->
    couch_merger:merge_indexes_no_limit(Params);

merge_spatial(#merge_params{row_acc = []} = Params) ->
    case couch_merger:merge_indexes_no_acc(
            Params, fun merge_spatial_min_row/2) of
    {params, Params2} ->
        merge_spatial(Params2);
    Else ->
        Else
    end;

% ??? vmx 20110805: Does this case ever happen in the spatial index?
merge_spatial(Params) ->
    Params2 = couch_merger:handle_skip(Params),
    merge_spatial(Params2).

merge_spatial_min_row(Params, MinRow) ->
    ok = couch_view_merger_queue:flush(Params#merge_params.queue),
    couch_merger:handle_skip(Params#merge_params{row_acc=[MinRow]}).

% Counterpart to view_qs/1 in couch_view_merger
spatial_qs(SpatialArgs) ->
    DefSpatialArgs = #spatial_query_args{},
    #spatial_query_args{
        bbox = Bbox,
        stale = Stale,
        count = Count,
        bounds = Bounds
    } = SpatialArgs,
    QsList = case Bbox =:= DefSpatialArgs#spatial_query_args.bbox of
    true ->
        [];
    false ->
        ["bbox=" ++ ?b2l(iolist_to_binary(lists:nth(2, hd(io_lib:format("~p", [Bbox])))))]
    end ++
    case Stale =:= DefSpatialArgs#spatial_query_args.stale of
    true ->
        [];
    false ->
        ["stale=" ++ atom_to_list(Stale)]
    end ++
    case Count =:= DefSpatialArgs#spatial_query_args.count of
    true ->
        [];
    false ->
        ["count=" ++ atom_to_list(Count)]
    end ++
    case Bounds =:= DefSpatialArgs#spatial_query_args.bounds of
    true ->
        [];
    false ->
        ["bounds=" ++ ?b2l(iolist_to_binary(lists:nth(2, hd(io_lib:format("~p", [Bounds])))))]
    end,
    case QsList of
    [] ->
        [];
    _ ->
        "?" ++ string:join(QsList, "&")
    end.
