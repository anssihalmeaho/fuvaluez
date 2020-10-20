# ValueZ
ValueZ is value store. Values are [FunL programming language](https://github.com/anssihalmeaho/funl) values.
Values are kept in memory (RAM) and accessed primarily from there but are also stored to persistent storage (disk). ValueZ works more efficiently if client is more read-intensive than write-intensive.

ValueZ supports concurrent access to data and guarantees consistency via transactions and views (snapshots). Also collection operations modify values in collection so that consistency remains inside collection.

ValueZ is used in context of one process (not to be shared with several processes).

## Concepts
Database (db) and Collection (col) are main concepts in valuez.

### Database (db)
Database (db) contains many collections. Persistent storage works in scope of database.

### Collection (col)
Collection contains many FunL values. Most operations of ValueZ are applied at collection level.
Consistency is guaranteed at collection level.

#### Transaction (txn)
Transaction is procedure call inside which several operations for collection can be done so that changes are isolated from current collection contents. Depending on return value of procedure call changes are committed or discarded.

#### Views
Views are snapshots of collection. View is implemented with procedure call inside which several operations to collection share same snapshot of values. Only read -type of operations are allowed inside views.

## Storage engine: bbolt
Persistent storage is implemented by using [bbolt](https://github.com/etcd-io/bbolt) key-value storage.

## Value types
Values (in collection) can be any [serializable FunL values](https://github.com/anssihalmeaho/funl/wiki/stdser).

## API
ValueZ provides following kind of interfaces for client:

* db/col operations
    * open
    * new-col
    * get-col
    * get-col-names
    * del-col
    * close
* reading/writing values
    * put-value
    * get-values
    * take-values
    * update
* transactions/views
    * trans
    * view

### db/col operations
Database (db) and collection (col) are represented as [opaque FunL types](https://github.com/anssihalmeaho/funl/wiki/Opaque-Value).

#### open
Opens existing database or if it does not exist creates new database.

```
valuez.open(<db-name:string>) -> list(<ok:bool> <error:string> <db:opaque>)
```

#### new-col
Creates new collection for db.

```
valuez.new-col(<db:opaque> <col-name:string>) -> list(<ok:bool> <error:string> <col:opaque>)
```

#### get-col
Gets collection value by name from db.

```
valuez.get-col(<db:opaque> <col-name:string>) -> list(<ok:bool> <error:string> <col:opaque>)
```

#### get-col-names
Gets list of collection names in db.

```
valuez.get-col-names(<db:opaque> <col-name:string>) -> list(<ok:bool> <error:string> <col-names:list-of-strings>)
```

#### del-col
Deletes collection from db. Waits ongoing operations to finish before removal.

```
valuez.del-col(<col:opaque>) -> list(<ok:bool> <error:string>)
```

#### close
Closes db. Waits ongoing operations to finish before closing.

```
valuez.close(<col:opaque>) -> list(<ok:bool> <error:string> <db:opaque>)
```
