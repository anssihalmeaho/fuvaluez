# valuez
valuez is value store. Values are [FunL programming language](https://github.com/anssihalmeaho/funl) values.
Values are kept in memory (RAM) and accessed primarily from there but are also stored to persistent storage (disk).
Supports concurrent access to data and guarantees consistency via transactions and views (snapshots).
valuez is used in context of one process (not to be shared with several processes).

# Concepts
Database (db) and Collection (col) are main concepts in valuez.

## Database (db)
Database (db) contains many collections. Persistent storage works in scope of database.

## Collection (col)
Collection contains many FunL values. Most operations of valuez are applied at collection level.

## Transaction (txn)
Transaction is procedure call inside which several operations for collection can be done so that changes are isolated from current collection contents. Depending on return value of procedure call changes are committed or discarded.

## Views
Views are snapshots of collection. View is implemented with procedure call inside which several operations to collection share same snapshot of values. Only read -type of operations are allowed inside views.
