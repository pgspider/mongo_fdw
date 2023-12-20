// Cleanup of databases/collections created during regression run
// As 'test' is a default database, any foreign table created when
// database is not mentioned then corresponding collection gets
// created in test database. So dropping as part of cleanup.
use json_regress
db.rows.drop();
db.test_json.drop();
db.foo.drop();
db.jspoptest.drop();
db.json_tbl.drop();
db.repeat_json_tbl.drop();
db.pg_input_is_valid_tbl.drop();

// Below queries will create and insert values in collections
db.rows.insertMany([
    {_id : ObjectId("60efb2d771ff01801caf2036"), x : NumberInt(1), y : "txt1" },
    {_id : ObjectId("60efb2d771ff01801caf2037"), x : NumberInt(2), y : "txt2" },
    {_id : ObjectId("60efb2d771ff01801caf2038"), x : NumberInt(3), y : "txt3" }
]);

db.test_json.insertMany([
    {_id : ObjectId("60efb4f171ff01801caf2039"), json_type : "scalar", test_json : "a scalar" },
    {_id : ObjectId("60efb4f671ff01801caf203a"), json_type : "array", test_json :  ["zero", "one","two",null,"four","five", [NumberInt(1), NumberInt(2), NumberInt(3)],{"f1":NumberInt(9)}] },
    {_id : ObjectId("60efb52d71ff01801caf203b"), json_type : "object", test_json : {"field1":"val1","field2":"val2","field3":null, "field4": NumberInt(4), "field5": [NumberInt(1), NumberInt(2), NumberInt(3)], "field6": {"f1":NumberInt(9)}}}
]);

db.jspoptest.insertMany([
    {_id : ObjectId("60efdfcd71ff01801caf203f"), js : { "jsa": [NumberInt(1), "2", null, NumberInt(4)],"rec": {"a": "abc", "c": "01.02.2003", "x": 43.2}, "reca": [{"a": "abc", "b": NumberInt(456)}, null, {"c": "01.02.2003", "x": 43.2}]} }
]);

db.foo.insertMany([
    {_id : ObjectId("60efd97071ff01801caf203c"), serial_num : NumberInt(847001), name : "t15", type : "GE1043" },
    {_id : ObjectId("60efd97c71ff01801caf203d"), serial_num : NumberInt(847002), name : "t16", type : "GE1043" },
    {_id : ObjectId("60efd97d71ff01801caf203e"), serial_num : NumberInt(847003), name : "sub-alpha", type : "GESS90" }
]);


use jsonb_regress
db.rows.drop();
db.test_jsonb.drop();
db.foo.drop();
db.jspoptest.drop();
db.nestjsonb.drop();

// Below queries will create and insert values in collections
db.rows.insertMany([
    {_id : ObjectId("60f0e4f744497a69702752d1"), x : NumberInt(1), y : "txt1" },
    {_id : ObjectId("60f0e4fd44497a69702752d2"), x : NumberInt(2), y : "txt2" },
    {_id : ObjectId("60f0e50144497a69702752d3"), x : NumberInt(3), y : "txt3" }
]);

db.test_jsonb.insertMany([
    {_id : ObjectId("60f0e50544497a69702752d4"), json_type : "scalar", test_json : "a scalar" },
    {_id : ObjectId("60f0e50a44497a69702752d5"), json_type : "array", test_json :  ["zero", "one","two",null,"four","five", [NumberInt(1), NumberInt(2), NumberInt(3)],{"f1":NumberInt(9)}] },
    {_id : ObjectId("60f0e50e44497a69702752d6"), json_type : "object", test_json : {"field1":"val1","field2":"val2","field3":null, "field4": NumberInt(4), "field5": [NumberInt(1), NumberInt(2), NumberInt(3)], "field6": {"f1":NumberInt(9)}} }
]);

db.jspoptest.insertMany([
    {_id : ObjectId("60f0e50f44497a69702752d7"), js : { "jsa": [NumberInt(1), "2", null, NumberInt(4)],"rec": {"a": "abc", "c": "01.02.2003", "x": 43.2}, "reca": [{"a": "abc", "b": NumberInt(456)}, null, {"c": "01.02.2003", "x": 43.2}]} }
]);

db.foo.insertMany([
    {_id : ObjectId("60f0e51044497a69702752d9"), serial_num : NumberInt(847001), name : "t15", type : "GE1043" },
    {_id : ObjectId("60f0e52644497a69702752da"), serial_num : NumberInt(847002), name : "t16", type : "GE1043" },
    {_id : ObjectId("60f0e52844497a69702752dc"), serial_num : NumberInt(847003), name : "sub-alpha", type : "GESS90" }
]);

db.nestjsonb.insertMany([
    {_id : ObjectId("60f10025c6d173cad75bf388"), j : {"a":[["b",{"x":NumberInt(1)}],["b",{"x":NumberInt(2)}]],"c":NumberInt(3)} },
    {_id : ObjectId("60f10025c6d173cad75bf389"), j : [[NumberInt(14),NumberInt(2),NumberInt(3)]] },
    {_id : ObjectId("60f10025c6d173cad75bf38a"), j : [NumberInt(1),[NumberInt(14),NumberInt(2),NumberInt(3)]] }
]);

db.createCollection("repeat_json_tbl");
db.createCollection("pg_input_is_valid_tbl");