#!/bin/sh
export MONGO_HOST="localhost"
export MONGO_PORT="27017"
export MONGO_USER_NAME="edb"
export MONGO_PWD="edb"

# Below commands must be run in MongoDB to create mongo_fdw_regress and mongo_fdw_regress1 databases
# used in regression tests with edb user and edb password.

# use mongo_fdw_regress
# db.createUser({user:"edb",pwd:"edb",roles:[{role:"dbOwner", db:"mongo_fdw_regress"},{role:"readWrite", db:"mongo_fdw_regress"}]})
# use mongo_fdw_regress1
# db.createUser({user:"edb",pwd:"edb",roles:[{role:"dbOwner", db:"mongo_fdw_regress1"},{role:"readWrite", db:"mongo_fdw_regress1"}]})
# use json_regress
# db.createUser({user:"edb",pwd:"edb",roles:[{role:"dbOwner", db:"json_regress"},{role:"readWrite", db:"json_regress"}]})
# use jsonb_regress
# db.createUser({user:"edb",pwd:"edb",roles:[{role:"dbOwner", db:"jsonb_regress"},{role:"readWrite", db:"jsonb_regress"}]})
# use join_regress
# db.createUser({user:"edb",pwd:"edb",roles:[{role:"dbOwner", db:"join_regress"},{role:"readWrite", db:"join_regress"}]})
# use aggregates_regress
# db.createUser({user:"edb",pwd:"edb",roles:[{role:"dbOwner", db:"aggregates_regress"},{role:"readWrite", db:"aggregates_regress"}]})
# use limit_regress
# db.createUser({user:"edb",pwd:"edb",roles:[{role:"dbOwner", db:"limit_regress"},{role:"readWrite", db:"limit_regress"}]})
# use enhance_regress
# db.createUser({user:"edb",pwd:"edb",roles:[{role:"dbOwner", db:"enhance_regress"},{role:"readWrite", db:"enhance_regress"}]})

mongoimport --host=$MONGO_HOST --port=$MONGO_PORT -u $MONGO_USER_NAME -p $MONGO_PWD --db mongo_fdw_regress --collection countries --jsonArray --drop --maintainInsertionOrder --quiet < data/mongo_fixture.json
mongoimport --host=$MONGO_HOST --port=$MONGO_PORT -u $MONGO_USER_NAME -p $MONGO_PWD --db mongo_fdw_regress --collection warehouse --jsonArray --drop --maintainInsertionOrder --quiet < data/mongo_warehouse.json
mongo --host=$MONGO_HOST --port=$MONGO_PORT -u $MONGO_USER_NAME -p $MONGO_PWD --authenticationDatabase "mongo_fdw_regress" < data/mongo_test_data.js > /dev/null

# for json.sql/jsonb.sql test
mongo --host=$MONGO_HOST --port=$MONGO_PORT -u $MONGO_USER_NAME -p $MONGO_PWD --authenticationDatabase "json_regress" < data/json_test_data.js > /dev/null
mongo --host=$MONGO_HOST --port=$MONGO_PORT -u $MONGO_USER_NAME -p $MONGO_PWD --authenticationDatabase "jsonb_regress" < data/jsonb_testjsonb.js > /dev/null
# for join.sql test
mongo --host=$MONGO_HOST --port=$MONGO_PORT -u $MONGO_USER_NAME -p $MONGO_PWD --authenticationDatabase "join_regress" < data/join_test_data.js > /dev/null
mongoimport --host=$MONGO_HOST --port=$MONGO_PORT -u $MONGO_USER_NAME -p $MONGO_PWD --db join_regress --collection tenk --jsonArray --drop --maintainInsertionOrder --quiet < data/tenk.json

# for aggregates.sql test
mongo --host=$MONGO_HOST --port=$MONGO_PORT -u $MONGO_USER_NAME -p $MONGO_PWD --authenticationDatabase "aggregates_regress" < data/aggregates_test_data.js > /dev/null
mongoimport --host=$MONGO_HOST --port=$MONGO_PORT -u $MONGO_USER_NAME -p $MONGO_PWD --db aggregates_regress --collection tenk --jsonArray --drop --maintainInsertionOrder --quiet < data/tenk.json
# for limit.sql test
mongo --host=$MONGO_HOST --port=$MONGO_PORT -u $MONGO_USER_NAME -p $MONGO_PWD --authenticationDatabase "limit_regress" < data/limit_test_data.js > /dev/null
mongoimport --host=$MONGO_HOST --port=$MONGO_PORT -u $MONGO_USER_NAME -p $MONGO_PWD --db limit_regress --collection tenk --jsonArray --drop --maintainInsertionOrder --quiet < data/tenk.json
# for enhance.sql test
mongo --host=$MONGO_HOST --port=$MONGO_PORT -u $MONGO_USER_NAME -p $MONGO_PWD --authenticationDatabase "enhance_regress" < data/enhance_test_data.js > /dev/null
