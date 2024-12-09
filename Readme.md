# CDC based on pg logical replication

This project shows an example of an implementation of a CDC pattern to capture data changes in postgresql and replicate them in a Clickhouse database.

Postgresql performs better as an OLTP database, optimised for transactions and queries on few rows, whereas Clickhouse is better in OLAP, optimised for queries on many rows.

This example shows how to use the logical replication in Postgresql to capture data changes : a python program connects to Postgresql and subscribes to a replication slot, which is a sort of event queue receiving data from the Postgresql WAL, which are the database data logs storing all transaction occured in the database. WAL are the "single source of truth" in Postgresql and are used to backup data and for replication to downstream Postgresql servers, useful for high availability.

## create resources in postgresql

create in pg a publication for a give table and a replication slot

```sql
create publication mypub for table only test;
-- the slot uses the pgoutput binary replication plugin, available by default in pg
select * from pg_create_logical_replication_slot('test_slot', 'pgoutput');
```

There are several types of events, for example in case of an insert, the following four events are generated in this order: *relation*, *begin*, *insert*, *commit*.

The *relation* event contains data about the table whose events are being captured in the following events, its columns with their type, the schema, etc.

The *begin* and *commit* events correspond to the beginning and the end of the transaction, LSN, transaction id, commit timestamp… In the above example the transaction contains an *insert* event.

The *insert* event contains all data about the insert operation.

### create data in postgresql

```sql
create table test(pk integer primary key, a text, b Integer, c timestamp without time zone DEFAULT now(), d boolean);
insert into test(pk, a, b, c, d) values (1, 'a', 1, current_timestamp, True);
insert into test(pk, a, b, c, d) values (2, 'c', 2, current_timestamp, True);
insert into test(pk, a, b, c, d) values (3, 'e', 3, current_timestamp, True);
```

### create objects in clickhouse

#### create the table to receive data from postgres

```sql
create table if not exists test
(
    pk Integer,
    a String,
    b Nullable(Int64),
    c DateTime64,
    d Bool
) ENGINE = MergeTree
ORDER BY(pk);
```

#### initial load in Clickhouse

```sql
truncate table test;
```

the initial load is performed from clickhouse using its postgresql driver

```sql
insert into test select
    pk, a, b, c, d
FROM postgresql('postgres-host.domain.com:5432', 'db_in_psg', 'test', 'clickhouse_user', 'ClickHouse_123', 'schema');

```

#### Run the python code to capture data

Set the environment variables 
```bash
export DATABASE_NAME=...
export DATABASE_USERNAME=...
export DATABASE_PASSWORD=...
export DATABASE_HOST=...
export DATABASE_PORT=...

export PG_PUBLICATION_NAME=mypub
export PG_REPLICATION_SLOT=test_slot

export CLICKHOUSE_HOSTNAME=...
export CLICKHOUSE_USERNAME=...
export CLICKHOUSE_PORT=...
export CLICKHOUSE_PASSWORD=...
export CLICKHOUSE_DATABASE=...
```
and run the python program with the following command

```bash
python /work/cdc_logical_replication_pgoutput.py
```

or as a docker container (see the dockerfile)

now let's insert some data in postgresql

```sql
insert into test(pk, a, b, c, d) values (4, 'd', 'e', 1, True);
```

and check it has been copied to clickhouse

```sql
select * from test;
```

let's update in postgresql

```sql
update test set a = 'c' where pk = 3;
```

and check it has been performed in clickhouse too

```sql
select * from test;
```

a delete in postgresql

```
delete from test where pk = 3;
```

and check it has been performed in clickhouse too

```sql
select * from test;
```
#### stress test

check the pg server parameter `wal_sender_timeout`, it must be enough large to let enough time to the decoding process

```sql
insert into test(pk, a, b, c, d) select generate_series, 'f', 2, current_timestamp, False from generate_series(1000, 2000);
```

#### tear down resources in postgresql

run the following commands to tear down postgresql publication and replication slot

```sql
select pg_drop_replication_slot('test_slot');
drop publication mypub;
```

#### empty the replication slot

```sql
select pg_logical_slot_get_binary_changes('test_slot', NULL, NULL, 'publication_names', 'mypub', 'proto_version', '1');
```
