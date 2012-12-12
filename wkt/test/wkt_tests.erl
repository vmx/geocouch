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

-module(wkt_tests).

-include_lib("eunit/include/eunit.hrl").

parse_number_test() ->
    ?assertEqual({'Point', [12, 13]}, wkt:parse("POINT(12 13)")),
    ?assertEqual({'Point', [12.0, 13]}, wkt:parse("POINT(12.0 13)")),
    ?assertEqual({'Point', [12.458, 13]}, wkt:parse("POINT(12.458 13)")),
    ?assertEqual({'Point', [12, 13.4712]}, wkt:parse("POINT(12 13.4712)")),
    ?assertEqual({'Point', [1280.0, 13]}, wkt:parse("POINT(12.8E2 13)")),
    ?assertEqual({'Point', [0.128, 13]}, wkt:parse("POINT(12.8E-2 13)")),
    ?assertEqual({'Point', [12.8, 132500.0]}, wkt:parse("POINT(12.8 13.25E4)")),
    ?assertEqual({'Point', [12.8, 0.001325]}, wkt:parse("POINT(12.8 13.25E-4)")),

    ?assertEqual({'Point', [12, -13]}, wkt:parse("POINT(+12 -13)")),
    ?assertEqual({'Point', [-12.0, 13]}, wkt:parse("POINT(-12.0 +13)")),
    ?assertEqual({'Point', [-12.458, 13]}, wkt:parse("POINT(-12.458 13)")),
    ?assertEqual({'Point', [12, -13.4712]}, wkt:parse("POINT(  +12  -13.4712)")),
    ?assertEqual({'Point', [-1280.0, 0.001325]},
                 wkt:parse("POINT(-12.8E2 +13.25E-4)")),
    ?assertEqual({'Point', [0.128, 132500.0]},
                 wkt:parse("POINT(+12.8E-2 13.25E+4)")),
    ?assertEqual({'Point', [0.128, -132500.0]},
                 wkt:parse("POINT(+12.8E-2 -13.25E+4)")).


parse_whitespace_empty_test() ->
    Point = {'Point', []},
    GC = {'GeometryCollection', [Point]},
    ?assertEqual(Point, wkt:parse("POINT EMPTY")),
    ?assertEqual(Point, wkt:parse("POINT  EMPTY")),
    ?assertEqual(Point, wkt:parse("POINT   EMPTY")),
    ?assertEqual(GC, wkt:parse("GEOMETRYCOLLECTION(POINT EMPTY)")),
    ?assertEqual(GC, wkt:parse("GEOMETRYCOLLECTION(POINT EMPTY )")),
    ?assertEqual(GC, wkt:parse("GEOMETRYCOLLECTION(POINT EMPTY  )")),
    ?assertEqual(GC, wkt:parse("GEOMETRYCOLLECTION(POINT  EMPTY  )")),
    ?assertEqual(GC, wkt:parse("GEOMETRYCOLLECTION(POINT   EMPTY  )")).

parse_whitespace_single_test() ->
    Point = {'Point', [12, 13]},
    ?assertEqual(Point, wkt:parse("POINT(12 13)")),
    ?assertEqual(Point, wkt:parse("POINT (12 13)")),
    ?assertEqual(Point, wkt:parse("POINT  (12 13)")),
    ?assertEqual(Point, wkt:parse("POINT( 12 13)")),
    ?assertEqual(Point, wkt:parse("POINT(  12 13)")),
    ?assertEqual(Point, wkt:parse("POINT(12  13)")),
    ?assertEqual(Point, wkt:parse("POINT(12 13 )")),
    ?assertEqual(Point, wkt:parse("POINT(12 13  )")),
    ?assertEqual(Point, wkt:parse("POINT(12  13  )")).

parse_whitespace_multi_test() ->
    LS = {'LineString', [[12, 13], [14, 15], [16, 17]]},
    ?assertEqual(LS, wkt:parse("LINESTRING(12 13,14 15,16 17)")),
    ?assertEqual(LS, wkt:parse("LINESTRING(12 13, 14 15,16 17)")),
    ?assertEqual(LS, wkt:parse("LINESTRING(12 13,  14 15,16 17)")),
    ?assertEqual(LS, wkt:parse("LINESTRING(12 13,14 15, 16 17)")),
    ?assertEqual(LS, wkt:parse("LINESTRING(12 13,14 15,  16 17)")),
    ?assertEqual(LS, wkt:parse("LINESTRING(12 13, 14 15,  16 17)")),
    ?assertEqual(LS, wkt:parse("LINESTRING(12 13 ,14 15,16 17)")),
    ?assertEqual(LS, wkt:parse("LINESTRING(12 13  ,14 15,16 17)")),
    ?assertEqual(LS, wkt:parse("LINESTRING(12 13, 14 15 ,16 17)")),
    ?assertEqual(LS, wkt:parse("LINESTRING(12 13,  14 15  ,16 17)")),
    ?assertEqual(LS, wkt:parse("LINESTRING(12 13,  14 15  , 16 17)")),
    ?assertEqual(LS, wkt:parse("LINESTRING(12 13,  14 15  ,  16 17)")),
    ?assertEqual(LS, wkt:parse("LINESTRING(12 13 ,  14 15  ,16 17)")),
    ?assertEqual(LS, wkt:parse("LINESTRING(12 13  ,  14 15  ,16 17)")),
    ?assertEqual(LS, wkt:parse("LINESTRING(12 13,14 15,16 17 )")),
    ?assertEqual(LS, wkt:parse("LINESTRING(12 13,14 15,16 17  )")),
    ?assertEqual(LS, wkt:parse("LINESTRING(12 13,14 15, 16 17 )")),
    ?assertEqual(LS, wkt:parse("LINESTRING(12 13,14 15, 16 17  )")),
    ?assertEqual(LS, wkt:parse("LINESTRING(12 13,14 15,  16 17  )")).

parse_whitespace_geometrycollection_test() ->
    GC = {'GeometryCollection', [{'Point', [12, 13]}]},
    GC2 = {'GeometryCollection', [{'Point', [12, 13]}, {'Point', [14, 15]}]},
    ?assertEqual(GC, wkt:parse("GEOMETRYCOLLECTION( POINT(12 13))")),
    ?assertEqual(GC, wkt:parse("GEOMETRYCOLLECTION(  POINT(12 13))")),
    ?assertEqual(GC, wkt:parse("GEOMETRYCOLLECTION(POINT(12 13) )")),
    ?assertEqual(GC, wkt:parse("GEOMETRYCOLLECTION(POINT(12 13)  )")),
    ?assertEqual(GC2, wkt:parse("GEOMETRYCOLLECTION( POINT(12 13),POINT(14 15))")),
    ?assertEqual(GC2, wkt:parse("GEOMETRYCOLLECTION(  POINT(12 13),POINT(14 15))")),
    ?assertEqual(GC2, wkt:parse("GEOMETRYCOLLECTION(POINT(12 13),POINT(14 15))")),
    ?assertEqual(GC2, wkt:parse("GEOMETRYCOLLECTION(POINT(12 13), POINT(14 15))")),
    ?assertEqual(GC2, wkt:parse("GEOMETRYCOLLECTION(POINT(12 13),  POINT(14 15))")),
    ?assertEqual(GC2, wkt:parse("GEOMETRYCOLLECTION(POINT(12 13),POINT(14 15) )")),
    ?assertEqual(GC2, wkt:parse("GEOMETRYCOLLECTION(POINT(12 13),POINT(14 15)  )")),
    ?assertEqual(GC2, wkt:parse("GEOMETRYCOLLECTION(POINT(12 13) ,POINT(14 15))")),
    ?assertEqual(GC2, wkt:parse("GEOMETRYCOLLECTION(POINT(12 13)  ,POINT(14 15))")).


parse_geom_point_test() ->
    ?assertEqual({'Point', []}, wkt:parse("POINT EMPTY)")),
    ?assertEqual({'Point', [12, 13]}, wkt:parse("POINT (12 13)")).

parse_geom_linestring_test() ->
    ?assertEqual({'LineString', []},
                 wkt:parse("LINESTRING EMPTY")),
    ?assertEqual({'LineString', [[12, 13], [14, 15]]},
                 wkt:parse("LINESTRING (12 13, 14 15)")).

% the geometry of triangles are equal to ones of polygons
parse_geom_polygon_test() ->
    ?assertEqual({'Polygon', []},
                 wkt:parse("POLYGON EMPTY")),
    ?assertEqual({'Polygon', [[[12, 13], [24, 25], [36, 17], [12, 13]]]},
                 wkt:parse("POLYGON ((12 13, 24 25, 36 17, 12 13))")),
    ?assertEqual({'Polygon', [[[102, 103], [204, 205], [306, 107], [102, 103]],
                            [[12, 13], [24, 25], [36, 17], [12, 13]]]},
                 wkt:parse("POLYGON ((102 103, 204 205, 306 107, 102 103),"
                       "(12 13, 24 25, 36 17, 12 13))")),
    ?assertEqual({'Polygon', [[[102, 103], [204, 205], [306, 107], [102, 103]],
                            [[12, 13], [24, 25], [36, 17], [12, 13]],
                            [[62, 63], [74, 75], [86, 67], [62, 63]]]},
                 wkt:parse("POLYGON ((102 103, 204 205, 306 107, 102 103),"
                       "(12 13, 24 25, 36 17, 12 13),"
                       "(62 63, 74 75, 86 67, 62 63))")).

parse_geom_multipoint_test() ->
    ?assertEqual({'MultiPoint', []},
                 wkt:parse("MULTIPOINT EMPTY")),
    ?assertEqual({'MultiPoint', [[12, 13], [14, 15]]},
                 wkt:parse("MULTIPOINT (12 13, 14 15)")).

parse_geom_multilinestring_test() ->
    ?assertEqual({'MultiLineString', []},
                 wkt:parse("MULTILINESTRING EMPTY")),
    ?assertEqual({'MultiLineString', [[[12, 13], [14, 15]],
                                    [[16, 17], [18, 19]]]},
                 wkt:parse("MULTILINESTRING ((12 13, 14 15), (16 17, 18 19))")).

% the geometry of polyhedralsurfaces/tins are equal to ones of multipolygons
parse_geom_multipolygon_test() ->
    ?assertEqual({'MultiPolygon', []},
                 wkt:parse("MULTIPOLYGON EMPTY")),
    ?assertEqual({'MultiPolygon', [[[[12, 13], [24, 25], [36, 17], [12, 13]]],
                                 [[[2012, 2013], [2024, 2025],
                                   [2036, 2017], [2012, 2013]]]]},
                 wkt:parse("MULTIPOLYGON (((12 13, 24 25, 36 17, 12 13)),"
                       "((2012 2013, 2024 2025, 2036 2017, 2012 2013)))")),
    ?assertEqual({'MultiPolygon', [[[[102, 103], [204, 205], [306, 107], [102, 103]],
                                  [[12, 13], [24, 25], [36, 17], [12, 13]]],
                                 [[[2102, 2103], [2204, 2205],
                                   [2306, 2107], [2102, 2103]],
                                  [[2012, 2013], [2024, 2025],
                                   [2036, 2017], [2012, 2013]]]]},
                 wkt:parse("MULTIPOLYGON (((102 103, 204 205, 306 107, 102 103),"
                       "(12 13, 24 25, 36 17, 12 13)),"
                       "((2102 2103, 2204 2205, 2306 2107, 2102 2103),"
                       "(2012 2013, 2024 2025, 2036 2017, 2012 2013)))")).

parse_geom_geometrycollection_test() ->
    ?assertEqual({'GeometryCollection',[]},
                 wkt:parse("GEOMETRYCOLLECTION EMPTY")),
    ?assertEqual({'GeometryCollection',[{'Point', []}]},
                 wkt:parse("GEOMETRYCOLLECTION (POINT EMPTY)")),
    ?assertEqual({'GeometryCollection',[{'Point', []}, {'MultiPoint', []}]},
                 wkt:parse("GEOMETRYCOLLECTION (POINT EMPTY, MULTIPOINT EMPTY)")),
    ?assertEqual({'GeometryCollection',[{'Point', []},
                                      {'MultiPoint', [[12, 13], [14, 15]]}]},
                 wkt:parse("GEOMETRYCOLLECTION(POINT EMPTY,"
                       "MULTIPOINT (12 13, 14 15))")),

    ?assertEqual({'GeometryCollection',[{'Point', [12, 13]}]},
                 wkt:parse("GEOMETRYCOLLECTION(POINT (12 13))")),
    ?assertEqual({'GeometryCollection',[{'Point', [12, 13]},
                                      {'MultiPoint', [[12, 13], [14, 15]]}]},
                 wkt:parse("GEOMETRYCOLLECTION(POINT (12 13),"
                       "MULTIPOINT (12 13, 14 15))")),
    ?assertEqual({'GeometryCollection',
                  [{'Point', [12, 13]}, {'Point', [14, 15]},
                   {'Polygon', [[[102, 103], [204, 205], [306, 107], [102, 103]],
                              [[12, 13], [24, 25], [36, 17], [12, 13]],
                              [[62, 63], [74, 75], [86, 67], [62, 63]]]}]},
                 wkt:parse("GEOMETRYCOLLECTION("
                       "POINT (12 13),POINT (14 15),"
                       "POLYGON ((102 103, 204 205, 306 107, 102 103),"
                       "(12 13, 24 25, 36 17, 12 13),"
                       "(62 63, 74 75, 86 67, 62 63)))")).

% It's the same for all other geometries
parse_geom_with_space_test() ->
    ?assertEqual({'point z',[]}, wkt:parse("POINT Z EMPTY")),
    ?assertEqual({'point z',[]}, wkt:parse("POINT Z  EMPTY")),
    ?assertEqual({'point z',[12, 13, 14]},
                 wkt:parse("POINT Z (12 13 14)")),
    ?assertEqual({'point z',[12, 13, 14]},
                 wkt:parse("POINT Z  (12 13 14)")),
    ?assertEqual({'point zm',[]}, wkt:parse("POINT ZM EMPTY")),
    ?assertEqual({'point zm',[]}, wkt:parse("POINT ZM  EMPTY")),
    ?assertEqual({'point zm',[12, 13, 14, 15]},
                 wkt:parse("POINT ZM (12 13 14 15)")),
    ?assertEqual({'point zm',[12, 13, 14, 15]},
                 wkt:parse("POINT ZM  (12 13 14 15)")).
