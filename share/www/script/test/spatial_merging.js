// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

couchTests.spatial_merging = function(debug) {
  if (debug) debugger;

  function newDb(name) {
    var db = new CouchDB(name, {"X-Couch-Full-Commit": "false"});
    db.deleteDb();
    db.createDb();

    return db;
  }

  function dbUri(db) {
    return CouchDB.protocol + CouchDB.host + '/' + db.name;
  }

  function populateAlternated(dbs, docs) {
    var docIdx = 0;

    while (docIdx < docs.length) {
      for (var i = 0; (i < dbs.length) && (docIdx < docs.length); i++) {
        var db = dbs[i];
        var doc = docs[docIdx];

        TEquals(true, db.save(doc).ok);
        docIdx += 1;
      }
    }
  }

  function populateSequenced(dbs, listOfDocLists) {
    for (var i = 0, j = 0; (i < dbs.length) && (j < listOfDocLists.length); i++, j++) {
      var db = dbs[i];
      var docList = listOfDocLists[j];

      for (var k = 0; k < docList.length; k++) {
        var doc = docList[k];
        TEquals(true, db.save(doc).ok);
      }
    }
  }

  function addDoc(dbs, doc) {
    for (var i = 0; i < dbs.length; i++) {
      TEquals(true, dbs[i].save(doc).ok);
      delete doc._rev;
    }
  }

  function mergedQuery(dbs, spatialName, options) {
    var body = {
      "spatial": {}
    };

    options = options || {};

    for (var i = 0; i < dbs.length; i++) {
      if (typeof dbs[i] === "string") {
        body.spatial[dbs[i]] = spatialName;
      } else {
        body.spatial[dbs[i].name] = spatialName;
      }
    }

    var qs = "";

    for (var q in options) {
      if (q === "connection_timeout") {
        body["connection_timeout"] = options[q];
        continue;
      }
      if (q === "on_error") {
        body["on_error"] = options[q];
        continue;
      }
      if (qs !== "") {
        qs = qs + "&";
      }
      qs = qs + String(q) + "=" + String(options[q]);
    }

    qs = "?bbox=-180,-90,180,90&" + qs;

    var xhr = CouchDB.request("POST", "/_spatial_merge" + qs, {
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(body)
    });
    TEquals(200, xhr.status);

    return JSON.parse(xhr.responseText);
  }

  function wait(ms) {
    var t0 = new Date(), t1;
    do {
      CouchDB.request("GET", "/");
      t1 = new Date();
    } while ((t1 - t0) <= ms);
  }

  function compareSpatialResults(resultA, resultB) {
    TEquals(resultA.rows.length, resultB.rows.length, "same # of rows");

    for (var i = 0; i < resultA.rows.length; i++) {
      var a = resultA.rows[i];
      var b = resultB.rows[i];
      var docA = a.doc || null;
      var docB = b.doc || null;

      TEquals(JSON.stringify(a.key), JSON.stringify(b.key), "keys are equal");
      TEquals(JSON.stringify(a.value), JSON.stringify(b.value),
        "values are equal");
      TEquals(JSON.stringify(docA), JSON.stringify(docB), "docs are equal");
    }
  }

  function makeSpatialDocs(start, end, templateDoc) {
    var docs = makeDocs(start, end, templateDoc);
    for (var i=0; i<docs.length; i++) {
        docs[i].loc = [i-20+docs[i].integer, i+15+docs[i].integer];
    }
    return docs;
  }

  /**
   * Tests with spatial indexes.
   */

  var ddoc = {
    _id: "_design/test",
    language: "javascript",
    spatial: {
      fun1: (function(doc) {
        emit({
          type: "Point",
          coordinates: doc.loc
        }, doc.string);
      }).toString()
    }
  };
  // test with empty dbs
  var dbA, dbB, dbC, dbD, dbE, dbs, docs, resp, resp2, i;
  var xhr, body, subspatialspec;
  dbA = newDb("test_db_a");
  dbB = newDb("test_db_b");
  dbs = [dbA, dbB];

  addDoc(dbs, ddoc);

  resp = mergedQuery(dbs, "test/fun1");

  TEquals("object", typeof resp);
  TEquals(0, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(0, resp.rows.length);

  // Test for existence of error rows when most source databases are
  // are missing the design documents.
  addDoc([dbA], {
    "_id": "_design/testfoobar",
    "spatial": { "foobar": 'function(doc) { '+
      'emit({type: "Point", coordinates: doc.loc}, 1); }' }
  });
  dbC = newDb("test_db_c");
  dbD = newDb("test_db_d");
  dbE = newDb("test_db_e");
  dbs = [dbUri(dbA), dbUri(dbB), dbUri(dbC), dbUri(dbD), dbUri(dbE)];
  resp = mergedQuery(dbs, "testfoobar/foobar");

  TEquals(0, resp.rows.length);
  TEquals(4, resp.errors.length);
  for (i = 0; i < resp.errors.length; i++) {
    TEquals("string", typeof resp.errors[i].from);
    TEquals("string", typeof resp.errors[i].reason);
  }

  // Same as before but with sub spatial merges.
  body = {"spatial": {}};
  body.spatial[dbA.name] = "testfoobar/foobar";
  body.spatial[dbB.name] = "testfoobar/foobar";
  subspatialspec = {
    "spatial": {}
  };
  subspatialspec.views[dbC.name] = "testfoobar/foobar";
  subspatialspec.views[dbD.name] = "testfoobar/foobar";
  subspatialspec.views[dbE.name] = "testfoobar/foobar";
  body.spatial[CouchDB.protocol + CouchDB.host + '/_spatial_merge'] =
      subspatialspec;

  xhr = CouchDB.request("POST", "/_spatial_merge", {
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  });
  TEquals(200, xhr.status);
  resp = JSON.parse(xhr.responseText);

  // 2 error rows, one for local database dbB plus another related to "remote"
  // view merging (all 3 "remote" databases miss the design document).
  TEquals(0, resp.rows.length);
  TEquals(2, resp.errors.length);
  for (i = 0; i < resp.errors.length; i++) {
    TEquals(true, (typeof resp.errors[i].from === "string") || (resp.errors[i].from === null));
    TEquals("string", typeof resp.errors[i].reason);
  }

  addDoc([dbC], {
    "_id": "_design/testfoobar",
    "spatial": { "foobar": 'function(doc) { '+
      'emit({type: "Point", coordinates: doc.loc}, 1); }' }
  });
  xhr = CouchDB.request("POST", "/_spatial_merge", {
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  });
  TEquals(200, xhr.status);
  resp = JSON.parse(xhr.responseText);

  // 3 error rows, one for local database dbB plus 2 related to "remote"
  // databases dbD and dbE.
  TEquals(0, resp.rows.length);
  TEquals(3, resp.errors.length);
  for (i = 0; i < resp.errors.length; i++) {
    TEquals(true, (typeof resp.errors[i].from === "string") || (resp.errors[i].from === null));
    TEquals("string", typeof resp.errors[i].reason);
  }


  // test 1 empty db and one non-empty db
  dbA = newDb("test_db_a");
  dbB = newDb("test_db_b");
  docs = [makeSpatialDocs(1, 11)];
  dbs = [dbA, dbB];

  addDoc(dbs, ddoc);
  populateSequenced([dbA], docs);

  resp = mergedQuery(dbs, "test/fun1");

  TEquals("object", typeof resp);
  TEquals(10, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(10, resp.rows.length);

  testKeysSorted(resp);


  // 2 dbs, alternated keys
  dbA = newDb("test_db_a");
  dbB = newDb("test_db_b");
  docs = makeSpatialDocs(1, 41);
  dbs = [dbA, dbB];

  addDoc(dbs, ddoc);
  populateAlternated(dbs, docs);

  resp = mergedQuery(dbs, "test/fun1");

  TEquals("object", typeof resp);
  TEquals(40, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(40, resp.rows.length);

  testKeysSorted(resp);

  // same, but with a remote db name
  resp2 = mergedQuery([dbA, dbUri(dbB)], "test/fun1");

  compareViewResults(resp, resp2);

  // now test stale=ok works
  populateAlternated(dbs, makeSpatialDocs(41, 43));

  resp = mergedQuery(dbs, "test/fun1", {stale: "ok"});

  TEquals("object", typeof resp);
  TEquals(40, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(40, resp.rows.length);

  // same, but with a remote db name
  resp2 = mergedQuery([dbA, dbUri(dbB)], "test/fun1", {stale: "ok"});

  compareViewResults(resp, resp2);

  // test stale=update_after works

  resp = mergedQuery(dbs, "test/fun1", {stale: "update_after"});

  TEquals("object", typeof resp);
  TEquals(40, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(40, resp.rows.length);

  // wait a bit, the view should now reflect the 2 new documents
  wait(1000);

  resp = mergedQuery(dbs, "test/fun1", {stale: "ok"});

  TEquals("object", typeof resp);
  TEquals(42, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(42, resp.rows.length);
  TEquals(41, resp.rows[40].key);
  TEquals("41", resp.rows[40].id);
  TEquals(42, resp.rows[41].key);
  TEquals("42", resp.rows[41].id);

  testKeysSorted(resp);

  // 2 dbs, sequenced keys (worst case)
  dbA = newDb("test_db_a");
  dbB = newDb("test_db_b");
  docs = [makeSpatialDocs(1, 21), makeSpatialDocs(21, 41)];
  dbs = [dbA, dbB];

  addDoc(dbs, ddoc);
  populateSequenced(dbs, docs);

  resp = mergedQuery(dbs, "test/fun1");

  TEquals("object", typeof resp);
  TEquals(40, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(40, resp.rows.length);

  testKeysSorted(resp);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbUri(dbB)], "test/fun1");

  compareViewResults(resp, resp2);


  // 5 dbs, alternated keys
  dbA = newDb("test_db_a");
  dbB = newDb("test_db_b");
  dbC = newDb("test_db_c");
  dbD = newDb("test_db_d");
  dbE = newDb("test_db_e");
  docs = makeSpatialDocs(1, 51);
  dbs = [dbA, dbB, dbC, dbD, dbE];

  addDoc(dbs, ddoc);
  populateAlternated(dbs, docs);

  resp = mergedQuery(dbs, "test/fun1");

  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(50, resp.rows.length);

  testKeysSorted(resp);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/fun1");

  compareViewResults(resp, resp2);

  // test skip=N query parameter
  resp = mergedQuery(dbs, "test/mapview1", {"skip": 2});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(48, resp.rows.length);
  TEquals(3, resp.rows[0].key);
  TEquals("3", resp.rows[0].id);
  TEquals(4, resp.rows[1].key);
  TEquals("4", resp.rows[1].id);

  testKeysSorted(resp);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/fun1",
    {"skip": 2});

  compareViewResults(resp, resp2);
console.log("this is one is supposed to fail (skip)");
  resp = mergedQuery(dbs, "test/fun1", {"skip": 49});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(1, resp.rows.length);
  TEquals(50, resp.rows[0].key);
  TEquals("50", resp.rows[0].id);

  testKeysSorted(resp);

console.log("this is one is supposed to fail (skip) (b)");
  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/fun1",
    {"skip": 49});

  compareViewResults(resp, resp2);

  resp = mergedQuery(dbs, "test/fun1", {"skip": 0});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(50, resp.rows.length);
  TEquals(1, resp.rows[0].key);
  TEquals("1", resp.rows[0].id);

  testKeysSorted(resp);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/fun1",
    {"skip": 0});

  compareViewResults(resp, resp2);
console.log("this is one is supposed to fail (limit)");
  // test limit=N query parameter
  resp = mergedQuery(dbs, "test/fun1", {"limit": 1});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(1, resp.rows.length);
  TEquals(1, resp.rows[0].key);
  TEquals("1", resp.rows[0].id);

console.log("this is one is supposed to fail (limit) (b)");
  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/fun1",
    {"limit": 1});

  compareViewResults(resp, resp2);

  resp = mergedQuery(dbs, "test/mapview1", {"limit": 10});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(10, resp.rows.length);
  TEquals(1, resp.rows[0].key);
  TEquals("1", resp.rows[0].id);
  TEquals(10, resp.rows[9].key);
  TEquals("10", resp.rows[9].id);

  testKeysSorted(resp);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/fun1",
    {"limit": 10});

  compareViewResults(resp, resp2);

  resp = mergedQuery(dbs, "test/fun1", {"limit": 1000});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(50, resp.rows.length);
  TEquals(1, resp.rows[0].key);
  TEquals("1", resp.rows[0].id);
  TEquals(50, resp.rows[49].key);
  TEquals("50", resp.rows[49].id);

  testKeysSorted(resp);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/fun1",
    {"limit": 1000});

  compareViewResults(resp, resp2);

  // test skip=N with limit=N query parameters
  resp = mergedQuery(dbs, "test/fun1", {"limit": 10, "skip": 10});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(10, resp.rows.length);
  TEquals(11, resp.rows[0].key);
  TEquals("11", resp.rows[0].id);
  TEquals(20, resp.rows[9].key);
  TEquals("20", resp.rows[9].id);

  testKeysSorted(resp);

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/fun1",
    {"limit": 10, "skip": 10});

  compareViewResults(resp, resp2);

/* not supported by GeoCouch, but hopefully in the future
  // test include_docs query parameter
  resp = mergedQuery(dbs, "test/fun1", {"include_docs": "true"});
  TEquals("object", typeof resp);
  TEquals(50, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(50, resp.rows.length);

  testKeysSorted(resp);
  for (i = 0; i < resp.rows.length; i++) {
    var doc = resp.rows[i].doc;
    T((doc !== null) && (typeof doc === 'object'), "row has doc");
    TEquals(i + 1, doc.integer);
    TEquals(String(i + 1), doc.string);
    TEquals(resp.rows[i].id, doc._id);
  }

  // same, but with remote dbs
  resp2 = mergedQuery([dbUri(dbA), dbB, dbUri(dbC), dbD, dbE], "test/fun1",
    {"include_docs": "true"});

  compareViewResults(resp, resp2);

  // test the we get the same result with a sub merge view spec
  body = {"spatial": {}};
  body.spatial[dbA.name] = "test/fun1";
  body.spatial[dbUri(dbB)] = "test/fun1";
  subspatialspec = {
    "spatial": {}
  };
  subspatialspec.views[dbC.name] = "test/fun1";
  subspatialspec.views[dbD.name] = "test/fun1";
  subspatialspec.views[dbE.name] = "test/fun1";
  body.spartial[CouchDB.protocol + CouchDB.host + '/_spatial_merge'] =
    subspatialspec;

  xhr = CouchDB.request("POST",
    "/_spatial_merge?include_docs=true", {
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  });
  TEquals(200, xhr.status);

  resp2 = JSON.parse(xhr.responseText);
  compareViewResults(resp, resp2);
*/

  /**
   * End of tests with map views.
   */



  /**
   * Test that we can merge arbitray map views, that is,
   * the source databases do not need to have the same
   * map function code, nor design document IDs nor view names.
   */

  var ddoc1 = {
    _id: "_design/test1",
    language: "javascript",
    views: {
      mapview1: {
        map:
          (function(doc) {
             emit(doc.integer, doc.string);
          }).toString()
      },
      redview1: {
        map:
          (function(doc) {
             emit([doc.integer, doc.string], doc.integer);
             emit([doc.integer + 1, doc.string], doc.integer + 1);
          }).toString(),
        reduce:
          (function(keys, values, rereduce) {
             return sum(values);
          }).toString()
      }
    }
  };
  var ddoc2 = {
    _id: "_design/test2",
    language: "javascript",
    views: {
      mapview2: {
        map:
          (function(doc) {
             emit(doc.integer, [doc._id, doc.integer]);
          }).toString()
      },
      redview2: {
        map:
          (function(doc) {
             emit([doc.integer, doc.string], doc.integer);
             emit([doc.integer + 1, doc.string], doc.integer + 1);
          }).toString(),
        reduce:
          (function(keys, values, rereduce) {
             return sum(values) * 2;
          }).toString()
      }
    }
  };

  dbA = newDb("test_db_a");
  dbB = newDb("test_db_b");
  dbC = newDb("test_db_c");
  docs = makeDocs(1, 31);
  dbs = [dbA, dbB, dbC];

  addDoc([dbA, dbC], ddoc1);
  addDoc([dbB], ddoc2);
  populateAlternated(dbs, docs);

  body = {"views": {}};
  body.views[dbA.name] = "test1/mapview1";
  body.views[dbUri(dbC)] = "test1/mapview1";
  body.views[dbB.name] = "test2/mapview2";

  xhr = CouchDB.request("POST", "/_view_merge", {
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  });
  TEquals(200, xhr.status);

  resp = JSON.parse(xhr.responseText);

  TEquals("object", typeof resp);
  TEquals(30, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(30, resp.rows.length);

  testKeysSorted(resp);

  /**
   * Test with "centralized"/"foreign" design documents.
   */
  ddoc = {
    _id: "_design/test",
    language: "javascript",
    views: {
      mapview1: {
        map:
          (function(doc) {
             emit(doc.integer, doc.string);
          }).toString()
      }
    }
  };

  var masterDb = newDb("test_db_master");
  dbA = newDb("test_db_a");
  dbB = newDb("test_db_b");
  docs = makeDocs(1, 11);
  dbs = [dbA, dbB];

  addDoc([masterDb], ddoc);
  populateAlternated(dbs, docs);

  resp = mergedQuery(dbs, masterDb.name + "/test/mapview1");

  TEquals("object", typeof resp);
  TEquals(10, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(10, resp.rows.length);

  testKeysSorted(resp);

  resp = mergedQuery(dbs, masterDb.name + "/_design/test/mapview1");

  TEquals("object", typeof resp);
  TEquals(10, resp.total_rows);
  TEquals("object", typeof resp.rows);
  TEquals(10, resp.rows.length);

  testKeysSorted(resp);

  // cleanup
  dbA.deleteDb();
  dbB.deleteDb();
  dbC.deleteDb();
  dbD.deleteDb();
  dbE.deleteDb();
  dbFull.deleteDb();
  masterDb.deleteDb();
};
