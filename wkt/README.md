WKT
===

Overview
--------

This is a non-validating WKT (Well-know text) parser for Erlang. It is meant
for fast parsing and parses only valid WKT and all other strings that seem
to be valid, but are not. The grammar is simplified to (in [EBNF as used for
the XML spec](http://www.w3.org/TR/2004/REC-xml-20040204/#sec-notation)):

    wkt ::= item | string  '(' space* item (comma item)* ')'
    item ::= string (geom | list | nested_list | item | 'EMPTY')
    nested_list ::= space* '(' list (comma list)* ')' | '(' nested_list+ ')'
    list ::= '(' geom (comma geom)* ')'
    geom ::= space* '(' coord (comma coord)* ')'
    coord ::= space* number (space+ number)*
    number ::= integer | float
    integer ::=  ('-' | '+')? [0-9]+
    float ::= ('-' | '+')? [0-9]+ '.' [0-9]+ exponent?
    exponent = 'E' ('-' | '+')? [0-9]+
    string ::= [a-zA-Z]+ (space* [a-zA-Z])*
    space :== #x20
    comma :== ',' space*

This means also strings like `this(is(10 20), a test EMPTY)` would be
parsed to:

    {this,[{is,[{10,20}]},{'a test',[]}]}

A validating parser would be much slower as it would also need to
perform checks on the geometry, e.g. for polygons whether interiors
are really within the exterior ring or not.

The general rule is, a list of coordinates is transformed to a list,
a list of coordinates to a list. The geometry name will be an
atom. Here's an example for a polygon:

    wkt:parse("POLYGON ((102 103, 204 205, 306 107, 102 103), (12 13, 24 25, 36 17, 12 13), (62 63, 74 75, 86 67, 62 63))").
    {Polygon,[[{102,103},{204,205},{306,107},{102,103}],
              [{12,13},{24,25},{36,17},{12,13}],
              [{62,63},{74,75},{86,67},{62,63}]]}

The atoms will be all lowercase for arbitrary strings, but camel cased
for the geometries that are also known from the GeoJSON specification
(where the case matters). Those are:

 - Point
 - MultiPoint
 - LineString
 - MultiLineString
 - Polygon
 - MultiPolygon
 - GeometryCollection


Compilation
-----------

    ./rebar compile


Running the tests
-----------------

Running the EUnit tests is simple as:

    ./rebar eunit


License
-------

The code is released under the Apache License 2.0.
Copyright 2010-2012, Volker Mische (http://vmx.cx/)
