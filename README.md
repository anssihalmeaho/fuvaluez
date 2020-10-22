# ValueZ
ValueZ is value store to be used from code written in [FunL programming language](https://github.com/anssihalmeaho/funl). 
Values are [FunL type of values](https://github.com/anssihalmeaho/funl/wiki/Syntax-and-concepts).
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

### Reading and writing values
Procedures for reading and writing from/to collection can be used in two ways:

1. Outside of transaction or view
2. Inside transaction or view

If operation is done outside of transaction/view then operation is directly applied to collection.
If operation is done in context of transaction then changes are applied to transaction, not
directly to collection (after commit changes are applied to collection). Similarly in context of view
operation (only read operations allowed) is applied to snapshot of collection, not to latest version
of collection.

In case of transaction and view (opaque) transaction value is given as argument to operation instead of 
(opaque) collection value.

#### put-value
Writes value to collection.

```
valuez.put-value(<col/txn:opaque> <value>) -> list(<ok:bool> <error:string>)
```

#### get-values
Reads values from collection which satisfy filter condition given as function
arument (2nd argument). Function is called for each value in collection.
Value is given as argument to function, if function returns **true** value
is included in result list, if it returns **false** then it's not included.

```
valuez.get-values(<col/txn:opaque> <func>) -> list(<value>, ...)
```

Example: Get all values from collection

```
ns main

import valuez
import stddbc

main = proc()
	open-ok open-err db = call(valuez.open 'esimdb'):
	_ = call(stddbc.assert open-ok open-err)

	col-ok col-err col = call(valuez.new-col db 'esim-col'):
	_ = call(stddbc.assert col-ok col-err)

	_ = call(valuez.put-value col 'Pizza')
	_ = call(valuez.put-value col 'Burger')
	_ = call(valuez.put-value col 'Hot Dog')
	
	items = call(valuez.get-values col func(x) true end)
	
	_ = call(valuez.close db)
	items
end

endns

-> list('Pizza', 'Burger', 'Hot Dog')
```

Example: Get values which start with 'P'

```
ns main

import valuez
import stddbc

main = proc()
	import stdstr

	open-ok open-err db = call(valuez.open 'esimdb'):
	_ = call(stddbc.assert open-ok open-err)

	col-ok col-err col = call(valuez.new-col db 'esim-col'):
	_ = call(stddbc.assert col-ok col-err)

	_ = call(valuez.put-value col 'Pizza')
	_ = call(valuez.put-value col 'Burger')
	_ = call(valuez.put-value col 'Hot Dog')
	
	items = call(valuez.get-values col func(x) call(stdstr.startswith x 'P') end)
	
	_ = call(valuez.close db)
	items
end

endns

-> list('Pizza')
```

#### take-values
Takes values from collection which satisfy filter condition given as function
arument (2nd argument). Function is called for each value in collection.
Value is given as argument to function, if function returns **true** value
is included in result list and removed from collection, if it returns **false** then 
it's not included in result list and it remains in collection.

```
valuez.take-values(<col/txn:opaque> <func>) -> list(<value>, ...)
```

Example: Take 'Burger' value from collection

```
ns main

import valuez
import stddbc

main = proc()
	open-ok open-err db = call(valuez.open 'esimdb'):
	_ = call(stddbc.assert open-ok open-err)

	col-ok col-err col = call(valuez.new-col db 'esim-col'):
	_ = call(stddbc.assert col-ok col-err)

	_ = call(valuez.put-value col 'Pizza')
	_ = call(valuez.put-value col 'Burger')
	_ = call(valuez.put-value col 'Hot Dog')
	
	taken-items = call(valuez.take-values col func(x) eq(x 'Burger') end)
	left-items = call(valuez.get-values col func(x) true end)
	
	_ = call(valuez.close db)
	sprintf('items taken: %v, items left: %v' taken-items left-items)
end

endns

-> 'items taken: list('Burger'), items left: list('Pizza', 'Hot Dog')'
```

#### update
Applies function given as argument to each value in collection and if function
returns list in which first item is **true** then value is replaced in collection with value given as
second item in list. If first item in returned list is **false** then value remains same
in collection.

```
valuez.update(<col/txn:opaque> <func>) -> <bool>
```

Return value is true if any value in collection was updated and changes were
successfully written to persistent storage.

Example: Change all values (strings) to lowercase strings

```
ns main

import valuez
import stddbc

main = proc()
	open-ok open-err db = call(valuez.open 'esimdb'):
	_ = call(stddbc.assert open-ok open-err)

	col-ok col-err col = call(valuez.new-col db 'esim-col'):
	_ = call(stddbc.assert col-ok col-err)

	_ = call(valuez.put-value col 'Pizza')
	_ = call(valuez.put-value col 'Burger')
	_ = call(valuez.put-value col 'Hot Dog')

	import stdstr
	_ = call(valuez.update col func(x) list(true call(stdstr.lowercase x)) end)
	
	items = call(valuez.get-values col func(x) true end)
	
	_ = call(valuez.close db)
	items
end

endns

-> list('pizza', 'burger', 'hot dog')
```

## Install
There are two ways to take ValueZ into use:

1. As plugin module
2. As std module (**locally** add to standard library modules)

Option 1. is preferred but plugin modules can be used only in FreeBSD/Linux/Mac, not in Windows.
Here's more information about [FunL plugin modules](https://github.com/anssihalmeaho/funl/wiki/plugin-modules).
So in Windows option 2. remains way to use ValueZ.

### ValueZ installed as plugin module
There is own Github repository for [ValueZ plugin building](https://github.com/anssihalmeaho/valuezplugin).
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

1. clone FunL repository from Github: `git clone https://github.com/anssihalmeaho/funl.git`
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
