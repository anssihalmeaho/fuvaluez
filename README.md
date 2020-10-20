# ValueZ
ValueZ is value store. Values are [FunL programming language](https://github.com/anssihalmeaho/funl) values.
Values are kept in memory (RAM) and accessed primarily from there but are also stored to persistent storage (disk). ValueZ works more efficiently if client is more read-intensive than write-intensive.

ValueZ supports concurrent access to data and guarantees consistency via transactions and views (snapshots). Also collection operations modify values in collection so that consistency remains inside collection.

ValueZ is used in context of one process (not to be shared with several processes).

## Concepts
Database (db) and Collection (col) are main concepts in ValueZ.

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

## Install
Thre are two ways to take ValueZ into use:

1. As plugin module
2. As std module (**locally** add to standard library modules)

Option 1. is preferred but plugin modules can be used only in FreeBSD/Linux/Mac, not in Windows.
Here's more information about [FunL plugin modules](https://github.com/anssihalmeaho/funl/wiki/plugin-modules).
So in Windows option 2. remains way to use ValueZ.

### ValueZ installed as plugin module
To create ValueZ as shared object librarys (__valuez.so__) do following steps:

1. `git clone https://github.com/anssihalmeaho/valuezplugin.git`
2. `cd valuezplugin/`
3. `go get github.com/anssihalmeaho/fuvaluez`
4. `make` -> produces __valuez.so__
5. set FUNLPATH environment variable so that __valuez.so__ is found

See [FunL plugin guide for more information](https://github.com/anssihalmeaho/funl/wiki/plugin-modules)

**Note.** Go modules implementation can be very strict about module/package versions so it may be required
that after cloning FunL repository from Github (`git clone https://github.com/anssihalmeaho/funl.git`)
all other files and folders removed but following ones:

* __main.go__
* __Makefile__
* __go.mod__

And then build __funla__ executable by `make`.

### ValueZ installed as std-module
See here how [built-in modules can be extended with own ones](https://github.com/anssihalmeaho/funl/wiki/External-Modules)
Steps are:

1. clone FunL repository from Github
2. add own Go module into **/std** directory (named for example like __valuezm.go__)
3. `go get github.com/anssihalmeaho/fuvaluez`
4. make -> produces __funla__ executable where ValueZ is buil-in module (with name __valuez__)

Here's how built-in module (__valuezm.go__) should be:

```Go
package std

import (
	"github.com/anssihalmeaho/funl/funl"
	"github.com/anssihalmeaho/fuvaluez/fuvaluez"
)

func init() {
	funl.AddExtensionInitializer(initMyExt)
}

func convGetter(inGetter func(string) fuvaluez.FZProc) func(string) stdFuncType {
	return func(name string) stdFuncType {
		return stdFuncType(inGetter(name))
	}
}

func initMyExt() (err error) {
	stdModuleName := "valuez"
	topFrame := &funl.Frame{
		Syms:     funl.NewSymt(),
		OtherNS:  make(map[funl.SymID]funl.ImportInfo),
		Imported: make(map[funl.SymID]*funl.Frame),
	}
	stdFuncs := []stdFuncInfo{
		{
			Name:   "open",
			Getter: convGetter(fuvaluez.GetVZOpen),
		},
		{
			Name:   "new-col",
			Getter: convGetter(fuvaluez.GetVZNewCol),
		},
		{
			Name:   "get-col",
			Getter: convGetter(fuvaluez.GetVZGetCol),
		},
		{
			Name:   "get-col-names",
			Getter: convGetter(fuvaluez.GetVZGetColNames),
		},
		{
			Name:   "put-value",
			Getter: convGetter(fuvaluez.GetVZPutValue),
		},
		{
			Name:   "get-values",
			Getter: convGetter(fuvaluez.GetVZGetValues),
		},
		{
			Name:   "take-values",
			Getter: convGetter(fuvaluez.GetVZTakeValues),
		},
		{
			Name:   "update",
			Getter: convGetter(fuvaluez.GetVZUpdate),
		},
		{
			Name:   "trans",
			Getter: convGetter(fuvaluez.GetVZTrans),
		},
		{
			Name:   "view",
			Getter: convGetter(fuvaluez.GetVZView),
		},
		{
			Name:   "del-col",
			Getter: convGetter(fuvaluez.GetVZDelCol),
		},
		{
			Name:   "close",
			Getter: convGetter(fuvaluez.GetVZClose),
		},
	}
	err = setSTDFunctions(topFrame, stdModuleName, stdFuncs)
	return
}
```
