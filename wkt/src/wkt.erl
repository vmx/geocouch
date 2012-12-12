% Copyright 2010-2012 Volker Mische (http://vmx.cx/)
%
% Licensed under the Apache License, Version 2.0 (the "License");
% you may not use this file except in compliance with the License.
% You may obtain a copy of the License at
%
%      http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS,
% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
% See the License for the specific language governing permissions and
% limitations under the License.

% This code is heavily based on the ideas of the MochiWeb's JSON2 decoder
% http://code.google.com/p/mochiweb/source/browse/trunk/src/mochijson2.erl
% (Copyright 2006 Mochi Media, Inc.)

-module(wkt).

-export([parse/1]).


parse(Wkt) ->
    case parse_char(Wkt) of
    {{parsed, Parsed}, _Wkt} ->
        Parsed;
    {space, Wkt2} ->
        parse(Wkt2);
    _ ->
        error
    end.

parse_char([H|T]=Wkt) ->
    case H of
    $( ->
        {start_list, T};
    $) ->
        {end_list, T};
    $, ->
        {comma, T};
    C when ((C >= $0) and (C =< $9)) orelse (C == $-) orelse (C == $+) ->
        parse_number(Wkt);
    C when ((C >= $a) and (C =< $z)) orelse ((C >= $A) and (C =< $Z)) ->
        parse_geometry(Wkt);
    $\s ->
        {space, T};
    _ ->
        parse_char(T)
    end.

parse_list(Wkt) ->
    parse_list(Wkt, []).

parse_list(Wkt, Acc) ->
    case parse_char(Wkt) of
    {start_list, Wkt2} ->
        {{parsed_list, List}, Wkt3} = parse_list(Wkt2),
        parse_list_inner(Wkt3, [List|Acc]);
    {{parsed, Parsed}, Wkt2} ->
        parse_list_inner(Wkt2, [Parsed|Acc]);
    {space, Wkt2} ->
        parse_list(Wkt2, Acc)
    end.


parse_list_inner(Wkt, Acc) ->
    case parse_char(Wkt) of
    {end_list, Wkt2} ->
        Acc2 = case tuple_them(Acc) of
        % Else points will end up as [[x, y]] instead if [x, y]
        [SingleItem] when is_number(hd(SingleItem)) ->
            lists:reverse(SingleItem);
        MultipleItems ->
            MultipleItems
        end,
        {{parsed_list, lists:reverse(Acc2)}, Wkt2};
    {space, Wkt2} ->
        parse_list_inner(Wkt2, Acc);
    {{parsed, Parsed}, Wkt2} ->
        parse_list_inner(Wkt2, [Parsed|Acc]);
    {comma, Wkt2} ->
        Acc2 = tuple_them(Acc),
        parse_list(Wkt2, Acc2)
    end.

% converts leading non-list elements to a list
% i.e. [a,b,[c,d],[e,f]] -> [[a,b],[c,d],[e,f]]
tuple_them(List) ->
    tuple_them(List, []).
% case when the comma is behind a parenthesis and not behind a number
tuple_them([], Acc) when is_list(hd(Acc)) ->
    lists:reverse(Acc);
tuple_them([], Acc) ->
    [Acc];
tuple_them([H|_T]=Rest, Acc) when is_tuple(H); is_list(H) ->
    case Acc of
        [] -> Rest;
        _ -> [Acc|Rest]
    end;
tuple_them([H|T], Acc) ->
    tuple_them(T, [H|Acc]).


parse_number([H|T]) ->
    {Num, Type, Wkt} = parse_number(T, int, [H]),
    case Type of
    int ->
        {{parsed, list_to_integer(Num)}, Wkt};
    float ->
        {{parsed, list_to_float(Num)}, Wkt}
    end.

parse_number([H|T], float, Acc) when H == $E orelse H == $- orelse H == $+ ->
    parse_number(T, float, [H|Acc]);
parse_number([H|T], Type, Acc) when (H >= $0) and (H =< $9) ->
    parse_number(T, Type, [H|Acc]);
parse_number([H|T], _Type, Acc) when H == $. ->
    parse_number(T, float, [H|Acc]);
parse_number(Wkt, Type, Acc) ->
    {lists:reverse(Acc), Type, Wkt}.


parse_geometry(Wkt) ->
    case parse_string(Wkt) of
    {{parsed_atom, Atom}, Wkt2} ->
        {{parsed_list, List}, Wkt3} = parse_geometry_inner(Wkt2),
        {{parsed, {Atom, List}}, Wkt3};
    {{parsed_empty, Atom}, Wkt2} ->
        {{parsed, {Atom, []}}, Wkt2}
    end.

parse_geometry_inner(Wkt) ->
    case parse_char(Wkt) of
    {space, Wkt2} ->
        parse_geometry_inner(Wkt2);
    {start_list, Wkt2} ->
        parse_list(Wkt2)
    end.


% all keywords (the geometry type) become Erlang atoms
parse_string([H|T]) ->
    %{String, Wkt} = parse_string(T, [H]),
    {{Type2, String}, Wkt2} = case parse_string(T, [H]) of
    {{type, Type}, Wkt} ->
        {{parsed_atom, list_to_atom(Type)}, Wkt};
    {{empty_type, Type}, Wkt} ->
        {{parsed_empty, list_to_atom(Type)}, Wkt}
    end,
    % The WKT specification doesn't say anything about the case of the
    % letters, therefore we transform them to camel case style, like in
    % GeoJSON specification
    String2 = case String of
        point -> 'Point';
        multipoint -> 'MultiPoint';
        linestring -> 'LineString';
        multilinestring -> 'MultiLineString';
        polygon -> 'Polygon';
        multipolygon -> 'MultiPolygon';
        geometrycollection -> 'GeometryCollection';
        _ -> String
    end,
    {{Type2, String2}, Wkt2}.


parse_string([H|T], Acc) when ((H >= $a) and (H =< $z)) orelse
                            ((H >= $A) and (H =< $Z)) orelse
                            H == $\s ->
    parse_string(T, [H|Acc]);
parse_string(Wkt, Acc) ->
    Stripped = string:strip(Acc, left),
    case Stripped of
    "YTPME " ++ Geometry ->
        Stripped2 = string:strip(Geometry, left),
        {{empty_type, lists:reverse(string:to_lower(Stripped2))}, Wkt};
    _ ->
        {{type, lists:reverse(string:to_lower(Stripped))}, Wkt}
    end.
