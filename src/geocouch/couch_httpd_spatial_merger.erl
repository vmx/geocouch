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

-module(couch_httpd_spatial_merger).

-export([handle_req/1, apply_http_config/3]).

-include("couch_db.hrl").
-include("couch_merger.hrl").
%-include("couch_spatial_merger.hrl").
-include("../ibrowse/ibrowse.hrl").

-import(couch_util, [
    get_value/2,
    get_value/3,
    to_binary/1
]).
-import(couch_httpd, [
    qs_json_value/3
]).

-record(sender_acc, {
    req = nil,
    resp = nil,
    on_error,
    acc = <<>>,
    error_acc = []
}).


setup_http_sender(MergeParams, Req) ->
    MergeParams#index_merge{
        user_acc = #sender_acc{
            req = Req, on_error = MergeParams#index_merge.on_error
        },
        callback = fun http_sender/2
    }.

handle_req(#httpd{method = 'GET'} = Req) ->
    Indexes = validate_spatial_param(qs_json_value(Req, "spatial", nil)),
    MergeParams0 = #index_merge{
        indexes = Indexes
    },
    MergeParams1 = apply_http_config(Req, [], MergeParams0),
    couch_merger:query_index(couch_spatial_merger, Req, MergeParams1);

handle_req(#httpd{method = 'POST'} = Req) ->
    couch_httpd:validate_ctype(Req, "application/json"),
    {Props} = couch_httpd:json_body_obj(Req),
    Indexes = validate_spatial_param(get_value(<<"spatial">>, Props)),
    MergeParams0 = #index_merge{
        indexes = Indexes
    },
    MergeParams1 = apply_http_config(Req, Props, MergeParams0),
    couch_merger:query_index(couch_spatial_merger, Req, MergeParams1);

handle_req(Req) ->
    couch_httpd:send_method_not_allowed(Req, "GET,POST").


apply_http_config(Req, Body, MergeParams) ->
    DefConnTimeout = MergeParams#index_merge.conn_timeout,
    ConnTimeout = case get_value(<<"connection_timeout">>, Body, nil) of
    nil ->
        qs_json_value(Req, "connection_timeout", DefConnTimeout);
    T when is_integer(T) ->
        T
    end,
    OnError = case get_value(<<"on_error">>, Body, nil) of
    nil ->
       qs_json_value(Req, "on_error", <<"continue">>);
    Policy when is_binary(Policy) ->
       Policy
    end,
    setup_http_sender(MergeParams#index_merge{
        conn_timeout = ConnTimeout,
        on_error = validate_on_error_param(OnError)
    }, Req).


http_sender(start, #sender_acc{req = Req, error_acc = ErrorAcc} = SAcc) ->
?LOG_DEBUG("http_sender: 1", []),
    {ok, Resp} = couch_httpd:start_json_response(Req, 200, []),
    couch_httpd:send_chunk(Resp, <<"{\"rows\":[">>),
    case ErrorAcc of
    [] ->
        Acc = <<"\r\n">>;
    _ ->
        lists:foreach(
            fun(Row) -> couch_httpd:send_chunk(Resp, [Row, <<",\r\n">>]) end,
            lists:reverse(ErrorAcc)),
        Acc = <<>>
    end,
    {ok, SAcc#sender_acc{resp = Resp, acc = Acc}};

http_sender({start, RowCount}, #sender_acc{req = Req, error_acc = ErrorAcc} = SAcc) ->
?LOG_DEBUG("http_sender: 2", []),
    Start = io_lib:format(
        "{\"total_rows\":~w,\"rows\":[", [RowCount]),
    {ok, Resp} = couch_httpd:start_json_response(Req, 200, []),
    couch_httpd:send_chunk(Resp, Start),
    case ErrorAcc of
    [] ->
        Acc = <<"\r\n">>;
    _ ->
        lists:foreach(
            fun(Row) -> couch_httpd:send_chunk(Resp, [Row, <<",\r\n">>]) end,
            lists:reverse(ErrorAcc)),
        Acc = <<>>
    end,
    {ok, SAcc#sender_acc{resp = Resp, acc = Acc, error_acc = []}};

http_sender({row, Row}, #sender_acc{resp = Resp, acc = Acc} = SAcc) ->
?LOG_DEBUG("http_sender: 3:~n~p", [Row]),
    couch_httpd:send_chunk(Resp, [Acc, ?JSON_ENCODE(Row)]),
    {ok, SAcc#sender_acc{acc = <<",\r\n">>}};

http_sender(stop, #sender_acc{resp = Resp}) ->
?LOG_DEBUG("http_sender: 4", []),
    couch_httpd:send_chunk(Resp, <<"\r\n]}">>),
    {ok, couch_httpd:end_json_response(Resp)};

http_sender({error, Url, Reason}, #sender_acc{on_error = continue} = SAcc) ->
?LOG_DEBUG("http_sender: 5", []),
    #sender_acc{resp = Resp, error_acc = ErrorAcc, acc = Acc} = SAcc,
    Row = {[
        {<<"error">>, true}, {<<"from">>, rem_passwd(Url)},
        {<<"reason">>, to_binary(Reason)}
    ]},
    case Resp of
    nil ->
        % we haven't started the response yet
        ErrorAcc2 = [?JSON_ENCODE(Row) | ErrorAcc],
        Acc2 = Acc;
    _ ->
        couch_httpd:send_chunk(Resp, [Acc, ?JSON_ENCODE(Row)]),
        ErrorAcc2 = ErrorAcc,
        Acc2 = <<",\r\n">>
    end,
    {ok, SAcc#sender_acc{error_acc = ErrorAcc2, acc = Acc2}};

http_sender({error, Url, Reason}, #sender_acc{on_error = stop} = SAcc) ->
?LOG_DEBUG("http_sender: 6", []),
    #sender_acc{req = Req, resp = Resp, acc = Acc} = SAcc,
    Row = {[
        {<<"error">>, true}, {<<"from">>, rem_passwd(Url)},
        {<<"reason">>, to_binary(Reason)}
    ]},
    case Resp of
    nil ->
        % we haven't started the response yet
        Start = io_lib:format("{\"total_rows\":~w,\"rows\":[\r\n", [0]),
        {ok, Resp2} = couch_httpd:start_json_response(Req, 200, []),
        couch_httpd:send_chunk(Resp2, Start),
        couch_httpd:send_chunk(Resp2, ?JSON_ENCODE(Row)),
        couch_httpd:send_chunk(Resp2, <<"\r\n]}">>);
    _ ->
       Resp2 = Resp,
       couch_httpd:send_chunk(Resp2, [Acc, ?JSON_ENCODE(Row)]),
       couch_httpd:send_chunk(Resp2, <<"\r\n]}">>)
    end,
    {stop, Resp2}.


%% Valid `spatial` example:
%%
%% {
%%   "spatial": {
%%     "localdb1": ["ddocname/viewname", ...],
%%     "http://server2/dbname": ["ddoc/view"],
%%     "http://server2/_view_merge": {
%%       "views": {
%%         "localdb3": "viewname", // local to server2
%%         "localdb4": "viewname"  // local to server2
%%       }
%%     }
%%   }
%% }

validate_spatial_param({[_ | _] = Indexes}) ->
    lists:flatten(lists:map(
        fun({DbName, SpatialName}) when is_binary(SpatialName) ->
            {DDocId, Vn} = parse_spatial_name(SpatialName),
            #simple_view_spec{
                database = DbName, ddoc_id = DDocId, view_name = Vn
            };
        ({DbName, SpatialNames}) when is_list(SpatialNames) ->
            lists:map(
                fun(SpatialName) ->
                    {DDocId, Vn} = parse_spatial_name(SpatialName),
                    #simple_view_spec{
                        database = DbName, ddoc_id = DDocId, view_name = Vn
                    }
                end, SpatialNames);
        ({MergeUrl, {[_ | _] = Props} = EJson}) ->
            case (catch ibrowse_lib:parse_url(?b2l(MergeUrl))) of
            #url{} ->
                ok;
            _ ->
                throw({bad_request, "Invalid spatial merge definition object."})
            end,
            case get_value(<<"spatial">>, Props) of
            {[_ | _]} = SubSpatial ->
                SubSpatialSpecs = validate_spatial_param(SubSpatial),
                case lists:any(
                    fun(#simple_view_spec{}) -> true; (_) -> false end,
                    SubSpatialSpecs) of
                true ->
                    ok;
                false ->
                    SubMergeError = io_lib:format("Could not find a"
                        " non-composed spatial spec in the spatial merge"
                        " targeted at `~s`",
                        [rem_passwd(MergeUrl)]),
                    throw({bad_request, SubMergeError})
                end,
                #merged_view_spec{url = MergeUrl, ejson_spec = EJson};
            _ ->
                SubMergeError = io_lib:format("Invalid spatial merge"
                    " definition for sub-merge done at `~s`.",
                    [rem_passwd(MergeUrl)]),
                throw({bad_request, SubMergeError})
            end;
        (_) ->
            throw({bad_request, "Invalid spatial merge definition object."})
        end, Indexes));

validate_spatial_param(_) ->
    throw({bad_request, <<"`spatial` parameter must be an object with at ",
                          "least 1 property.">>}).

parse_spatial_name(Name) ->
    case string:tokens(couch_util:trim(?b2l(Name)), "/") of
    [DDocName, ViewName0] ->
        {<<"_design/", (?l2b(DDocName))/binary>>, ?l2b(ViewName0)};
    ["_design", DDocName, ViewName0] ->
        {<<"_design/", (?l2b(DDocName))/binary>>, ?l2b(ViewName0)};
    _ ->
        throw({bad_request, "A `spatial` property must have the shape"
            " `ddoc_name/spatial_name`."})
    end.


validate_on_error_param(<<"continue">>) ->
    continue;
validate_on_error_param(<<"stop">>) ->
    stop;
validate_on_error_param(Value) ->
    Msg = io_lib:format("Invalid value (`~s`) for the parameter `on_error`."
        " It must be `continue` (default) or `stop`.", [to_binary(Value)]),
    throw({bad_request, Msg}).


rem_passwd(Url) ->
    ?l2b(couch_util:url_strip_password(Url)).
