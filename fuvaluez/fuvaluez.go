package fuvaluez

import (
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/anssihalmeaho/funl/funl"

	bolt "go.etcd.io/bbolt"
)

type FZProc func(frame *funl.Frame, arguments []funl.Value) (retVal funl.Value)

func newOpaqueDB(dbName string) *OpaqueDB {
	return &OpaqueDB{
		name: dbName,
		cols: make(map[string]*OpaqueCol),
	}
}

// OpaqueDB represents database
type OpaqueDB struct {
	sync.RWMutex
	name    string
	cols    map[string]*OpaqueCol
	Ch      chan changes
	AdminCh chan adminOP
	Closing bool
}

type adminOP struct {
	optype  string
	replych chan error
	colName string
	col     *OpaqueCol
}

type chType int

const newValue chType = 1
const delValue chType = 2

type changeItem struct {
	ChType  chType
	Key     string      // id
	Val     *funl.Value // nil in case of delValue
	ColName string
}

type changes struct {
	ReplyCh    chan error
	Changelist []changeItem
}

// Start starts db
func (db *OpaqueDB) Start(frame *funl.Frame) (bool, string) {
	if db.Ch != nil {
		return false, "db already started"
	}
	db.Ch = make(chan changes)
	db.AdminCh = make(chan adminOP)

	pStore, err := bolt.Open(fmt.Sprintf("%s.db", db.name), 0600, nil)
	if err != nil {
		return false, fmt.Sprintf("Storage opening failed: %v", err)
	}

	err = db.readAllcolsFromPersistent(pStore, frame)
	if err != nil {
		return false, fmt.Sprintf("Storage reading failed: %v", err)
	}
	go db.run(pStore, frame)
	return true, ""
}

func (db *OpaqueDB) addColToPersistent(boltDB *bolt.DB, colName string, col *OpaqueCol) error {
	return boltDB.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(colName))
		if err != nil {
			return err
		}

		b, err := tx.CreateBucketIfNotExists([]byte("__cols"))
		if err != nil {
			return err
		}
		if err := b.Put([]byte(colName), []byte{}); err != nil {
			return err
		}
		return nil
	})
}

func (db *OpaqueDB) readAllcolsFromPersistent(boltDB *bolt.DB, frame *funl.Frame) (err error) {
	var colNames []string

	err = boltDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("__cols"))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			colNames = append(colNames, string(k))
			return nil
		})
	})
	if err != nil {
		return
	}

	db.Lock()
	defer db.Unlock()

	err = boltDB.View(func(tx *bolt.Tx) error {
		for _, colName := range colNames {
			colBucket := tx.Bucket([]byte(colName))
			if colBucket == nil {
				return fmt.Errorf("Col not found (%s)", colName)
			}

			col := &OpaqueCol{
				Items:          make(map[string]funl.Value),
				ch:             make(chan req),
				latestSnapshot: nil,
				Db:             db,
				colName:        colName,
			}

			var biggestID int
			kvErr := colBucket.ForEach(func(k, v []byte) error {
				idVal := string(k)
				idNum, idErr := strconv.Atoi(idVal)
				if idErr != nil {
					return idErr
				}
				if idNum > biggestID {
					biggestID = idNum
				}

				decItem := &funl.Item{
					Type: funl.ValueItem,
					Data: funl.Value{
						Kind: funl.StringValue,
						Data: "call(proc() import stdser import stdbytes proc(__s) b = call(stdbytes.str-to-bytes __s) _ _ __v = call(stdser.decode b): __v end end)",
					},
				}
				decoderVal := funl.HandleEvalOP(frame, []*funl.Item{decItem})
				decArgs := []*funl.Item{
					&funl.Item{
						Type: funl.ValueItem,
						Data: decoderVal,
					},
					&funl.Item{
						Type: funl.ValueItem,
						Data: funl.Value{Kind: funl.StringValue, Data: string(v)},
					},
				}
				res := funl.HandleCallOP(frame, decArgs)

				col.Items[idVal] = res

				return nil
			})
			if kvErr != nil {
				return kvErr
			}

			col.idCounter = biggestID + 1
			db.cols[colName] = col
			go col.Run()
		}
		return nil
	})
	return
}

func (db *OpaqueDB) putKVtoPersistent(tx *bolt.Tx, frame *funl.Frame, colName string, key string, val funl.Value) error {
	colBucket := tx.Bucket([]byte(colName))
	if colBucket == nil {
		return fmt.Errorf("Col not found (%s)", colName)
	}

	encItem := &funl.Item{
		Type: funl.ValueItem,
		Data: funl.Value{
			Kind: funl.StringValue,
			Data: "call(proc() import stdser import stdbytes proc(x) _ _ b = call(stdser.encode x): call(stdbytes.string b) end end)",
		},
	}
	encoderVal := funl.HandleEvalOP(frame, []*funl.Item{encItem})
	encArgs := []*funl.Item{
		&funl.Item{
			Type: funl.ValueItem,
			Data: encoderVal,
		},
		&funl.Item{
			Type: funl.ValueItem,
			Data: val,
		},
	}
	res := funl.HandleCallOP(frame, encArgs)
	value := res.Data.(string)

	return colBucket.Put([]byte(key), []byte(value))
}

func (db *OpaqueDB) delKVfromPersistent(tx *bolt.Tx, frame *funl.Frame, colName string, key string) error {
	colBucket := tx.Bucket([]byte(colName))
	if colBucket == nil {
		return fmt.Errorf("Col not found (%s)", colName)
	}
	return colBucket.Delete([]byte(key))
}

func (db *OpaqueDB) consistentChangeWrites(boltDB *bolt.DB, frame *funl.Frame, changelist []changeItem) error {
	err := boltDB.Update(func(tx *bolt.Tx) error {
		for _, chItem := range changelist {
			switch chItem.ChType {
			case newValue:
				//fmt.Println(fmt.Sprintf("Write -> key: %s, val: %#v", chItem.Key, *chItem.Val))
				err := db.putKVtoPersistent(tx, frame, chItem.ColName, chItem.Key, *chItem.Val)
				if err != nil {
					return err
				}

			case delValue:
				//fmt.Println(fmt.Sprintf("Delete -> key: %s", chItem.Key))
				err := db.delKVfromPersistent(tx, frame, chItem.ColName, chItem.Key)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	return err
}

func (db *OpaqueDB) delColFromPersistent(boltDB *bolt.DB, colName string, col *OpaqueCol) error {
	return boltDB.Update(func(tx *bolt.Tx) error {
		errDelB := tx.DeleteBucket([]byte(colName))

		b, err := tx.CreateBucketIfNotExists([]byte("__cols"))
		if err != nil {
			return err
		}
		errRemoFrom := b.Delete([]byte(colName))
		if errDelB != nil {
			return errDelB
		}
		return errRemoFrom
	})
}

func (db *OpaqueDB) closePersistent(boltDB *bolt.DB) error {
	return boltDB.Close()
}

func (db *OpaqueDB) delCol(colName string) (bool, string) {
	db.Lock()
	defer db.Unlock()

	delete(db.cols, colName)
	return true, ""
}

func (db *OpaqueDB) run(boltDB *bolt.DB, frame *funl.Frame) {
	waitCols := make(map[string]bool)
	var closeReplych chan error
	for {
		if db.Closing {
			allColsSuspended := true
			for _, isSuspended := range waitCols {
				if !isSuspended {
					allColsSuspended = false
					break
				}
			}
			if allColsSuspended {
				closeReplych <- db.closePersistent(boltDB) // reply to close requester
				return                                     // exit from db handler
			}
		}

	reqSwitch:
		select {
		case changes := <-db.Ch:
			//fmt.Println(fmt.Sprintf("Ch-List: %#v", changes.Changelist))
			err := db.consistentChangeWrites(boltDB, frame, changes.Changelist)
			changes.ReplyCh <- err

		case adminOp := <-db.AdminCh:
			switch adminOp.optype {
			case "col-suspended":
				if db.Closing {
					waitCols[adminOp.colName] = true
				}

			case "close-db":
				if db.Closing {
					adminOp.replych <- fmt.Errorf("Already closing")
					break reqSwitch
				}
				db.Closing = true
				closeReplych = adminOp.replych

				for colName, col := range db.cols {
					waitCols[colName] = false

					// start goroutine for shutting col down
					go func(colv *OpaqueCol) {
						replyCh := make(chan funl.Value)
						errCh := make(chan string)
						request := &req{
							reqType: shutdownReq,
							reqData: funl.Value{Kind: funl.BoolValue, Data: true}, // doesnt matter
							replyCh: replyCh,
							errCh:   errCh,
							frame:   nil, // hoping it doesnt use that...
						}
						colv.ch <- *request
						// responses dont matter, shutdown counting comes other way
						select {
						case <-replyCh:
						case <-errCh:
						}
					}(col)
				}

			case "add-col":
				if db.Closing {
					adminOp.replych <- fmt.Errorf("db closing, add new col rejected")
					break reqSwitch
				}
				err := db.addColToPersistent(boltDB, adminOp.colName, adminOp.col)
				if err == nil {
					db.addCol(adminOp.col, adminOp.colName)
				}
				adminOp.replych <- err

			case "del-col":
				err := db.delColFromPersistent(boltDB, adminOp.colName, adminOp.col)
				db.delCol(adminOp.colName)
				if db.Closing {
					waitCols[adminOp.colName] = true
				}
				adminOp.replych <- err

			default:
				adminOp.replych <- errors.New("Unknown op")
			}
		}
	}
}

// TypeName gives type name
func (db *OpaqueDB) TypeName() string {
	return "db"
}

// Str returs value as string
func (db *OpaqueDB) Str() string {
	return fmt.Sprintf("db:%s", db.name)
}

// Equals returns equality
func (db *OpaqueDB) Equals(with funl.OpaqueAPI) bool {
	return false
}

func (db *OpaqueDB) addCol(col *OpaqueCol, colName string) (bool, string) {
	db.Lock()
	defer db.Unlock()

	if _, found := db.cols[colName]; found {
		return false, "col already exists"
	}
	db.cols[colName] = col
	return true, ""
}

func (db *OpaqueDB) getCol(colName string) (*OpaqueCol, bool) {
	db.RLock()
	defer db.RUnlock()

	col, found := db.cols[colName]
	return col, found
}

func (db *OpaqueDB) getColNames() []string {
	var colNames []string
	db.RLock()
	defer db.RUnlock()

	for k := range db.cols {
		colNames = append(colNames, k)
	}
	return colNames
}

// OpaqueCol represents collection
type OpaqueCol struct {
	sync.RWMutex
	Items          map[string]funl.Value
	ch             chan req
	idCounter      int
	latestSnapshot map[string]funl.Value
	Db             *OpaqueDB
	colName        string
	Closed         bool
	closedMutex    sync.RWMutex
}

// TypeName gives type name
func (col *OpaqueCol) TypeName() string {
	return "col"
}

// Str returs value as string
func (col *OpaqueCol) Str() string {
	return fmt.Sprintf("col:%s", col.colName)
}

// Equals returns equality
func (col *OpaqueCol) Equals(with funl.OpaqueAPI) bool {
	return false
}

// TypeName gives type name
func (txn *OpaqueTxn) TypeName() string {
	return "txn"
}

// Str returs value as string
func (txn *OpaqueTxn) Str() string {
	var txnColName string
	if txn.col != nil {
		txnColName = txn.col.colName
	}
	return fmt.Sprintf("txn:%s", txnColName)
}

// Equals returns equality
func (txn *OpaqueTxn) Equals(with funl.OpaqueAPI) bool {
	return false
}

// OpaqueTxn represents transaction
type OpaqueTxn struct {
	sync.RWMutex
	isReadTxn  bool
	newM       map[string]funl.Value
	newDeleted map[string]bool
	snapM      map[string]funl.Value
	col        *OpaqueCol
}

func newTxn(col *OpaqueCol, isReadTxn bool) *OpaqueTxn {
	txn := &OpaqueTxn{
		isReadTxn:  isReadTxn,
		newM:       make(map[string]funl.Value),
		newDeleted: make(map[string]bool),
		col:        col,
	}
	return txn
}

func getUpdateRetVal(frame *funl.Frame, updRetVal funl.Value) (bool, funl.Value, string) {
	if updRetVal.Kind != funl.ListValue {
		return false, funl.Value{}, "update: assuming list value"
	}
	lit := funl.NewListIterator(updRetVal)
	doUpdV := lit.Next()
	updVal := lit.Next()
	if doUpdV.Kind != funl.BoolValue {
		return false, funl.Value{}, "update: assuming bool value"
	}
	if updVal == nil {
		return false, funl.Value{}, "update: too short list"
	}
	return doUpdV.Data.(bool), *updVal, ""
}

// IsClosed tells whether col is closed
func (col *OpaqueCol) IsClosed() bool {
	col.closedMutex.RLock()
	defer col.closedMutex.RUnlock()

	return col.Closed
}

// lets create automic replier for further requests
func autoResponser(reqch chan req) {
	for {
		newReq := <-reqch

		replyValues := []funl.Value{
			{
				Kind: funl.BoolValue,
				Data: false,
			},
			{
				Kind: funl.StringValue,
				Data: "col closed",
			},
		}
		replyVal := funl.MakeListOfValues(newReq.frame, replyValues)
		newReq.replyCh <- replyVal
	}
}

// Run runs updator
func (col *OpaqueCol) Run() {
	//col.idCounter = 100
	for {
		req := <-col.ch
	reqSwitch:
		switch req.reqType {

		case shutdownReq:
			col.Closed = true
			adminOp := adminOP{
				optype:  "col-suspended",
				replych: nil,
				colName: col.colName,
				col:     col,
			}
			go autoResponser(col.ch)
			col.Db.AdminCh <- adminOp
			req.replyCh <- funl.Value{Kind: funl.BoolValue, Data: true}
			return // exit from goroutine

		case delColReq:
			if col.Closed {
				replyValues := []funl.Value{
					{
						Kind: funl.BoolValue,
						Data: false,
					},
					{
						Kind: funl.StringValue,
						Data: "closing already initiated",
					},
				}
				replyVal := funl.MakeListOfValues(req.frame, replyValues)
				req.replyCh <- replyVal
				break reqSwitch
			}
			col.Closed = true

			// removing col from db and storage
			replych := make(chan error)
			adminOp := adminOP{
				optype:  "del-col",
				replych: replych,
				colName: col.colName,
				col:     col,
			}
			col.Db.AdminCh <- adminOp
			err := <-replych

			// make response
			var errText string
			if err != nil {
				errText = fmt.Sprintf("Col delete error: %v", err)
			}
			replyValues := []funl.Value{
				{
					Kind: funl.BoolValue,
					Data: err == nil,
				},
				{
					Kind: funl.StringValue,
					Data: errText,
				},
			}
			replyVal := funl.MakeListOfValues(req.frame, replyValues)
			req.replyCh <- replyVal

			go autoResponser(col.ch)
			return // exit from goroutine

		case putReq:
			col.idCounter++
			idVal := strconv.Itoa(col.idCounter)
			col.Items[idVal] = req.reqData

			// to storage
			replyCh := make(chan error)
			chItem := changeItem{
				ChType:  newValue,
				Key:     idVal,
				Val:     &req.reqData,
				ColName: col.colName,
			}
			col.Db.Ch <- changes{Changelist: []changeItem{chItem}, ReplyCh: replyCh}
			storeErr := <-replyCh

			var errText string
			if storeErr != nil {
				errText = fmt.Sprintf("Put to persistent store failed: %v", errText)
			}
			replyValues := []funl.Value{
				{
					Kind: funl.BoolValue,
					Data: storeErr == nil,
				},
				{
					Kind: funl.StringValue,
					Data: errText,
				},
			}
			replyVal := funl.MakeListOfValues(req.frame, replyValues)
			col.latestSnapshot = nil
			req.replyCh <- replyVal

		case takeReq:
			// no need for any kind of locking yet
			filterFunc := &funl.Item{
				Type: funl.ValueItem,
				Data: req.reqData,
			}
			var takenIDs []string
			var results []funl.Value
			for k, v := range col.Items {
				argsForCall := []*funl.Item{
					filterFunc,
					&funl.Item{
						Type: funl.ValueItem,
						Data: v,
					},
				}
				filterResult, callErr := func() (rv funl.Value, errDesc string) {
					defer func() {
						if r := recover(); r != nil {
							var rtestr string
							if err, isError := r.(error); isError {
								rtestr = err.Error()
							}
							errDesc = fmt.Sprintf("take-values: handler made RTE: %s", rtestr)
						}
					}()
					return funl.HandleCallOP(req.frame, argsForCall), ""
				}()
				if callErr != "" {
					req.errCh <- callErr
					break reqSwitch
				}
				if filterResult.Kind != funl.BoolValue {
					req.errCh <- "assuming bool value"
					break reqSwitch
				}
				if filterResult.Data.(bool) {
					takenIDs = append(takenIDs, k)
					results = append(results, v)
				}
			}

			if len(takenIDs) == 0 {
				req.replyCh <- funl.MakeListOfValues(req.frame, []funl.Value{})
				break reqSwitch
			}
			// to storage
			var committedToPersistent bool
			replyCh := make(chan error)
			var chlist []changeItem
			for _, itemID := range takenIDs {
				chItem := changeItem{
					ChType:  delValue,
					Key:     itemID,
					Val:     nil,
					ColName: col.colName,
				}
				chlist = append(chlist, chItem)
			}
			col.Db.Ch <- changes{Changelist: chlist, ReplyCh: replyCh}
			storeErr := <-replyCh
			committedToPersistent = (storeErr == nil)

			if !committedToPersistent {
				req.replyCh <- funl.MakeListOfValues(req.frame, []funl.Value{})
				break reqSwitch
			}
			// now lock
			col.Lock()
			for _, itemID := range takenIDs {
				delete(col.Items, itemID)
			}
			col.Unlock()
			col.latestSnapshot = nil // could be optimized (if any deleted then invalidate)
			req.replyCh <- funl.MakeListOfValues(req.frame, results)

		case updateReq:
			// no need for any kind of locking yet
			updFunc := &funl.Item{
				Type: funl.ValueItem,
				Data: req.reqData,
			}
			newMap := make(map[string]funl.Value)
			var isAnyUpdates bool
			for k, v := range col.Items {
				argsForCall := []*funl.Item{
					updFunc,
					&funl.Item{
						Type: funl.ValueItem,
						Data: v,
					},
				}
				updRetVal, callErr := func() (rv funl.Value, errDesc string) {
					defer func() {
						if r := recover(); r != nil {
							var rtestr string
							if err, isError := r.(error); isError {
								rtestr = err.Error()
							}
							errDesc = fmt.Sprintf("update: handler made RTE: %s", rtestr)
						}
					}()
					return funl.HandleCallOP(req.frame, argsForCall), ""
				}()
				if callErr != "" {
					req.errCh <- callErr
					break reqSwitch
				}
				doUpdate, newValue, errStr := getUpdateRetVal(req.frame, updRetVal)
				if errStr != "" {
					req.errCh <- errStr
					break reqSwitch
				}
				if doUpdate {
					newMap[k] = newValue
					isAnyUpdates = true
				} else {
					newMap[k] = v
				}
			}
			var commitUpdates bool
			if isAnyUpdates {
				// to storage
				replyCh := make(chan error)
				var chlist []changeItem
				for k, v := range newMap {
					copyV := v
					chItem := changeItem{
						ChType:  newValue,
						Key:     k,
						Val:     &copyV,
						ColName: col.colName,
					}
					chlist = append(chlist, chItem)
				}
				col.Db.Ch <- changes{Changelist: chlist, ReplyCh: replyCh}
				storeErr := <-replyCh
				commitUpdates = (storeErr == nil)

				if commitUpdates {
					// now lock
					col.Lock()
					col.Items = newMap
					col.Unlock()
					col.latestSnapshot = nil
				}
			}
			req.replyCh <- funl.Value{Kind: funl.BoolValue, Data: commitUpdates}

		case transReq:
			transProc := &funl.Item{
				Type: funl.ValueItem,
				Data: req.reqData,
			}
			txn := newTxn(col, false)
			argsForCall := []*funl.Item{
				transProc,
				&funl.Item{
					Type: funl.ValueItem,
					Data: funl.Value{Kind: funl.OpaqueValue, Data: txn},
				},
			}
			retv, callErr := func() (rv funl.Value, errDesc string) {
				defer func() {
					if r := recover(); r != nil {
						var rtestr string
						if err, isError := r.(error); isError {
							rtestr = err.Error()
						}
						errDesc = fmt.Sprintf("trans: handler made RTE: %s", rtestr)
					}
				}()
				return funl.HandleCallOP(req.frame, argsForCall), ""
			}()
			if callErr != "" {
				req.errCh <- callErr
				break reqSwitch
			}
			if retv.Kind != funl.BoolValue {
				req.errCh <- "txn proc returned non-bool value"
				break reqSwitch
			}
			doCommit := retv.Data.(bool)
			if doCommit {
				// to storage
				var committedToPersistent bool
				replyCh := make(chan error)
				var chlist []changeItem
				for k, v := range txn.newM {
					copyV := v
					chItem := changeItem{
						ChType:  newValue,
						Key:     k,
						Val:     &copyV,
						ColName: col.colName,
					}
					chlist = append(chlist, chItem)
				}
				for itemID := range txn.newDeleted {
					chItem := changeItem{
						ChType:  delValue,
						Key:     itemID,
						Val:     nil,
						ColName: col.colName,
					}
					chlist = append(chlist, chItem)
				}
				col.Db.Ch <- changes{Changelist: chlist, ReplyCh: replyCh}
				storeErr := <-replyCh
				committedToPersistent = (storeErr == nil)

				if committedToPersistent {
					// to memory
					col.Lock()
					for k, v := range txn.newM {
						col.Items[k] = v
					}
					for itemID := range txn.newDeleted {
						delete(col.Items, itemID)
					}
					col.Unlock()
					col.latestSnapshot = nil
				} else {
					retv = funl.Value{Kind: funl.BoolValue, Data: false}
				}
			}
			req.replyCh <- retv

		case viewReq:
			txn := newTxn(col, true)
			if col.latestSnapshot == nil {
				col.latestSnapshot = make(map[string]funl.Value)
				for k, v := range col.Items {
					col.latestSnapshot[k] = v
				}
			}
			txn.snapM = col.latestSnapshot
			req.replyCh <- funl.Value{Kind: funl.OpaqueValue, Data: txn}

		default:
			funl.RunTimeError2(req.frame, "invalid req: %#v", req)
		}
	}
}

func newOpaqueCol(colName string, dbVal *OpaqueDB) *OpaqueCol {
	col := &OpaqueCol{
		Items:          make(map[string]funl.Value),
		ch:             make(chan req),
		latestSnapshot: nil,
		Db:             dbVal,
		colName:        colName,
		idCounter:      100,
	}
	go col.Run()
	return col
}

type reqType int

const (
	updateReq   = 1
	putReq      = 2
	takeReq     = 3
	transReq    = 4
	viewReq     = 5
	delColReq   = 6
	shutdownReq = 7
)

type req struct {
	reqType reqType
	reqData funl.Value
	replyCh chan funl.Value
	frame   *funl.Frame
	errCh   chan string
}

// services

func GetVZOpen(name string) FZProc {
	checkValidity := func(arguments []funl.Value) (bool, string) {
		if l := len(arguments); l != 1 {
			return false, fmt.Sprintf("%s: wrong amount of arguments (%d), need one", name, l)
		}
		if arguments[0].Kind != funl.StringValue {
			return false, fmt.Sprintf("%s: requires string value", name)
		}
		return true, ""
	}

	return func(frame *funl.Frame, arguments []funl.Value) (retVal funl.Value) {
		ok, errStr := checkValidity(arguments)
		var values []funl.Value
		if !ok {
			values := []funl.Value{
				{
					Kind: funl.BoolValue,
					Data: false,
				},
				{
					Kind: funl.StringValue,
					Data: errStr,
				},
				funl.Value{Kind: funl.OpaqueValue, Data: &OpaqueDB{}},
			}
			retVal = funl.MakeListOfValues(frame, values)
			return
		}
		dbName := arguments[0].Data.(string)
		dbVal := newOpaqueDB(dbName)
		dbOk, errText := dbVal.Start(frame)
		values = []funl.Value{
			{
				Kind: funl.BoolValue,
				Data: dbOk,
			},
			{
				Kind: funl.StringValue,
				Data: errText,
			},
			funl.Value{Kind: funl.OpaqueValue, Data: dbVal},
		}
		retVal = funl.MakeListOfValues(frame, values)
		return
	}
}

func GetVZView(name string) FZProc {
	checkValidity := func(arguments []funl.Value) (bool, string) {
		if l := len(arguments); l != 2 {
			return false, fmt.Sprintf("%s: wrong amount of arguments (%d)", name, l)
		}
		if arguments[0].Kind != funl.OpaqueValue {
			return false, fmt.Sprintf("%s: requires opaque value", name)
		}
		if arguments[1].Kind != funl.FunctionValue {
			return false, fmt.Sprintf("%s: requires func/proc value", name)
		}
		return true, ""
	}

	return func(frame *funl.Frame, arguments []funl.Value) (retVal funl.Value) {
		ok, errStr := checkValidity(arguments)
		if !ok {
			funl.RunTimeError2(frame, errStr)
		}
		var col *OpaqueCol
		if ok {
			col, ok = arguments[0].Data.(*OpaqueCol)
			if !ok {
				funl.RunTimeError2(frame, "%s: invalid col", name)
			}
		}
		replyCh := make(chan funl.Value)
		request := &req{
			reqType: viewReq,
			reqData: arguments[1], // needed ?
			replyCh: replyCh,
			frame:   frame,
		}
		col.ch <- *request
		txnVal := <-replyCh

		viewProc := &funl.Item{
			Type: funl.ValueItem,
			Data: arguments[1],
		}
		argsForCall := []*funl.Item{
			viewProc,
			&funl.Item{
				Type: funl.ValueItem,
				Data: txnVal,
			},
		}
		retVal = funl.HandleCallOP(frame, argsForCall)
		return
	}
}

func GetVZTrans(name string) FZProc {
	checkValidity := func(arguments []funl.Value) (bool, string) {
		if l := len(arguments); l != 2 {
			return false, fmt.Sprintf("%s: wrong amount of arguments (%d)", name, l)
		}
		if arguments[0].Kind != funl.OpaqueValue {
			return false, fmt.Sprintf("%s: requires opaque value", name)
		}
		if arguments[1].Kind != funl.FunctionValue {
			return false, fmt.Sprintf("%s: requires func/proc value", name)
		}
		return true, ""
	}

	return func(frame *funl.Frame, arguments []funl.Value) (retVal funl.Value) {
		ok, errStr := checkValidity(arguments)
		if !ok {
			funl.RunTimeError2(frame, errStr)
		}
		var col *OpaqueCol
		if ok {
			col, ok = arguments[0].Data.(*OpaqueCol)
			if !ok {
				funl.RunTimeError2(frame, "%s: invalid col", name)
			}
		}
		replyCh := make(chan funl.Value)
		errCh := make(chan string)
		request := &req{
			reqType: transReq,
			reqData: arguments[1],
			replyCh: replyCh,
			errCh:   errCh,
			frame:   frame,
		}
		col.ch <- *request
		select {
		case retVal = <-replyCh:
		case retErr := <-errCh:
			funl.RunTimeError2(frame, retErr)
		}
		return
	}
}

func GetVZUpdate(name string) FZProc {
	checkValidity := func(arguments []funl.Value) (bool, string) {
		if l := len(arguments); l != 2 {
			return false, fmt.Sprintf("%s: wrong amount of arguments (%d)", name, l)
		}
		if arguments[0].Kind != funl.OpaqueValue {
			return false, fmt.Sprintf("%s: requires opaque value", name)
		}
		if arguments[1].Kind != funl.FunctionValue {
			return false, fmt.Sprintf("%s: requires func/proc value", name)
		}
		return true, ""
	}

	return func(frame *funl.Frame, arguments []funl.Value) (retVal funl.Value) {
		ok, errStr := checkValidity(arguments)
		if !ok {
			funl.RunTimeError2(frame, errStr)
		}
		isTxn, col, txn := getColAndTxn(arguments[0])
		if (col == nil) && (txn == nil) {
			funl.RunTimeError2(frame, "%s: invalid col", name)
		}
		if isTxn {
			if txn.isReadTxn {
				funl.RunTimeError2(frame, "%s: not allowed in read txn", name)
			}
			updFunc := &funl.Item{
				Type: funl.ValueItem,
				Data: arguments[1],
			}
			newMap := make(map[string]funl.Value)
			m := make(map[string]funl.Value)

			txn.RLock()
			for k, v := range txn.col.Items {
				if !txn.newDeleted[k] {
					m[k] = v
				}
			}
			for k, v := range txn.newM {
				if !txn.newDeleted[k] {
					m[k] = v
				}
			}
			txn.RUnlock()

			var isAnyUpdates bool
			for k, v := range m {
				argsForCall := []*funl.Item{
					updFunc,
					&funl.Item{
						Type: funl.ValueItem,
						Data: v,
					},
				}
				updRetVal := funl.HandleCallOP(frame, argsForCall)
				doUpdate, newValue, errStr := getUpdateRetVal(frame, updRetVal)
				if errStr != "" {
					funl.RunTimeError2(frame, errStr)
				}
				if doUpdate {
					newMap[k] = newValue
					isAnyUpdates = true
				} else {
					newMap[k] = v
				}
			}
			if isAnyUpdates {
				// now lock
				txn.Lock()
				txn.newM = newMap
				txn.Unlock()
			}
			retVal = funl.Value{Kind: funl.BoolValue, Data: true}
			return
		}

		replyCh := make(chan funl.Value)
		errCh := make(chan string)
		request := &req{
			reqType: updateReq,
			reqData: arguments[1],
			replyCh: replyCh,
			errCh:   errCh,
			frame:   frame,
		}
		col.ch <- *request
		select {
		case retVal = <-replyCh:
		case retErr := <-errCh:
			funl.RunTimeError2(frame, retErr)
		}
		return
	}
}

func GetVZTakeValues(name string) FZProc {
	checkValidity := func(arguments []funl.Value) (bool, string) {
		if l := len(arguments); l != 2 {
			return false, fmt.Sprintf("%s: wrong amount of arguments (%d)", name, l)
		}
		if arguments[0].Kind != funl.OpaqueValue {
			return false, fmt.Sprintf("%s: requires opaque value", name)
		}
		if arguments[1].Kind != funl.FunctionValue {
			return false, fmt.Sprintf("%s: requires func/proc value", name)
		}
		return true, ""
	}

	return func(frame *funl.Frame, arguments []funl.Value) (retVal funl.Value) {
		ok, errStr := checkValidity(arguments)
		if !ok {
			funl.RunTimeError2(frame, errStr)
		}
		isTxn, col, txn := getColAndTxn(arguments[0])
		if (col == nil) && (txn == nil) {
			funl.RunTimeError2(frame, "invalid col")
		}
		if isTxn {
			if txn.isReadTxn {
				funl.RunTimeError2(frame, "%s: not allowed in read txn", name)
			}
			filterFunc := &funl.Item{
				Type: funl.ValueItem,
				Data: arguments[1],
			}
			var takenIDs []string
			var results []funl.Value

			m := make(map[string]funl.Value)
			txn.RLock()
			for k, v := range txn.col.Items {
				if !txn.newDeleted[k] {
					m[k] = v
				}
			}
			for k, v := range txn.newM {
				if !txn.newDeleted[k] {
					m[k] = v
				}
			}
			txn.RUnlock()

			for k, v := range m {
				argsForCall := []*funl.Item{
					filterFunc,
					&funl.Item{
						Type: funl.ValueItem,
						Data: v,
					},
				}
				filterResult := funl.HandleCallOP(frame, argsForCall)
				if filterResult.Kind != funl.BoolValue {
					funl.RunTimeError2(frame, "assuming bool value")
				}
				if filterResult.Data.(bool) {
					takenIDs = append(takenIDs, k)
					results = append(results, v)
				}
			}
			// now lock
			txn.Lock()
			for _, itemID := range takenIDs {
				txn.newDeleted[itemID] = true
			}
			txn.Unlock()
			retVal = funl.MakeListOfValues(frame, results)
			return
		}

		replyCh := make(chan funl.Value)
		errCh := make(chan string)
		request := &req{
			reqType: takeReq,
			reqData: arguments[1],
			replyCh: replyCh,
			errCh:   errCh,
			frame:   frame,
		}
		col.ch <- *request
		select {
		case retVal = <-replyCh:
		case retErr := <-errCh:
			funl.RunTimeError2(frame, retErr)
		}
		return
	}
}

func GetVZGetValues(name string) FZProc {
	checkValidity := func(arguments []funl.Value) (bool, string) {
		if l := len(arguments); l != 2 {
			return false, fmt.Sprintf("%s: wrong amount of arguments (%d)", name, l)
		}
		if arguments[0].Kind != funl.OpaqueValue {
			return false, fmt.Sprintf("%s: requires opaque value", name)
		}
		if arguments[1].Kind != funl.FunctionValue {
			return false, fmt.Sprintf("%s: requires func/proc value", name)
		}
		return true, ""
	}

	return func(frame *funl.Frame, arguments []funl.Value) (retVal funl.Value) {
		ok, errStr := checkValidity(arguments)
		if !ok {
			funl.RunTimeError2(frame, errStr)
		}
		isTxn, col, txn := getColAndTxn(arguments[0])
		if (col == nil) && (txn == nil) {
			funl.RunTimeError2(frame, "%s: invalid col", name)
		}
		filterFunc := &funl.Item{
			Type: funl.ValueItem,
			Data: arguments[1],
		}
		var results []funl.Value
		if isTxn {
			// read-only view
			if txn.isReadTxn {
				func() {
					txn.RLock() // is it really needed ?
					defer txn.RUnlock()

					for _, v := range txn.snapM {
						argsForCall := []*funl.Item{
							filterFunc,
							&funl.Item{
								Type: funl.ValueItem,
								Data: v,
							},
						}
						filterResult := funl.HandleCallOP(frame, argsForCall)
						if filterResult.Kind != funl.BoolValue {
							funl.RunTimeError2(frame, "%s: assuming bool value", name)
						}
						if filterResult.Data.(bool) {
							results = append(results, v)
						}
					}
				}()
				retVal = funl.MakeListOfValues(frame, results)
				return
			}
			// write transaction
			m := make(map[string]funl.Value)
			txn.RLock()
			for k, v := range txn.col.Items {
				if !txn.newDeleted[k] {
					m[k] = v
				}
			}
			for k, v := range txn.newM {
				if !txn.newDeleted[k] {
					m[k] = v
				}
			}
			txn.RUnlock()
			for _, v := range m {
				argsForCall := []*funl.Item{
					filterFunc,
					&funl.Item{
						Type: funl.ValueItem,
						Data: v,
					},
				}
				filterResult := funl.HandleCallOP(frame, argsForCall)
				if filterResult.Kind != funl.BoolValue {
					funl.RunTimeError2(frame, "%s: assuming bool value", name)
				}
				if filterResult.Data.(bool) {
					results = append(results, v)
				}
			}
			retVal = funl.MakeListOfValues(frame, results)
			return
		}
		// not in transaction/view
		func() {
			col.RLock()
			defer col.RUnlock()

			for _, v := range col.Items {
				argsForCall := []*funl.Item{
					filterFunc,
					&funl.Item{
						Type: funl.ValueItem,
						Data: v,
					},
				}
				filterResult := funl.HandleCallOP(frame, argsForCall)
				if filterResult.Kind != funl.BoolValue {
					funl.RunTimeError2(frame, "%s: assuming bool value", name)
				}
				if filterResult.Data.(bool) {
					results = append(results, v)
				}
			}
		}()
		retVal = funl.MakeListOfValues(frame, results)
		return
	}
}

func getColAndTxn(val funl.Value) (isTxn bool, col *OpaqueCol, txn *OpaqueTxn) {
	var convOK bool
	txn, convOK = val.Data.(*OpaqueTxn)
	if convOK {
		return true, nil, txn
	}
	col, convOK = val.Data.(*OpaqueCol)
	if convOK {
		return false, val.Data.(*OpaqueCol), nil
	}
	return false, nil, nil
}

func GetVZPutValue(name string) FZProc {
	checkValidity := func(arguments []funl.Value) (bool, string) {
		if l := len(arguments); l != 2 {
			return false, fmt.Sprintf("%s: wrong amount of arguments (%d)", name, l)
		}
		if arguments[0].Kind != funl.OpaqueValue {
			return false, fmt.Sprintf("%s: requires opaque value", name)
		}
		return true, ""
	}

	return func(frame *funl.Frame, arguments []funl.Value) (retVal funl.Value) {
		ok, errStr := checkValidity(arguments)
		if !ok {
			funl.RunTimeError2(frame, errStr)
		}
		isTxn, col, txn := getColAndTxn(arguments[0])
		if (col == nil) && (txn == nil) {
			funl.RunTimeError2(frame, "invalid col")
		}
		if isTxn {
			if txn.isReadTxn {
				funl.RunTimeError2(frame, "%s: not allowed in read txn", name)
			}
			txn.col.idCounter++
			idVal := strconv.Itoa(txn.col.idCounter)
			txn.Lock()
			txn.newM[idVal] = arguments[1]
			delete(txn.newDeleted, idVal)
			txn.Unlock()
			replyValues := []funl.Value{
				{
					Kind: funl.BoolValue,
					Data: true,
				},
				{
					Kind: funl.StringValue,
					Data: "",
				},
			}
			retVal = funl.MakeListOfValues(frame, replyValues)
			return
		}
		replyCh := make(chan funl.Value)
		request := &req{
			reqType: putReq,
			reqData: arguments[1],
			replyCh: replyCh,
			frame:   frame,
		}
		col.ch <- *request
		retVal = <-replyCh
		return
	}
}

func GetVZNewCol(name string) FZProc {
	checkValidity := func(arguments []funl.Value) (bool, string) {
		if l := len(arguments); l != 2 {
			return false, fmt.Sprintf("%s: wrong amount of arguments (%d), need two", name, l)
		}
		if arguments[0].Kind != funl.OpaqueValue {
			return false, fmt.Sprintf("%s: requires opaque value", name)
		}
		if arguments[1].Kind != funl.StringValue {
			return false, fmt.Sprintf("%s: requires string value", name)
		}
		return true, ""
	}

	return func(frame *funl.Frame, arguments []funl.Value) (retVal funl.Value) {
		ok, errStr := checkValidity(arguments)
		var dbVal *OpaqueDB
		if ok {
			dbVal, ok = arguments[0].Data.(*OpaqueDB)
			if !ok {
				errStr = "assuming db value"
			}
		}
		var values []funl.Value
		if !ok {
			values = []funl.Value{
				{
					Kind: funl.BoolValue,
					Data: false,
				},
				{
					Kind: funl.StringValue,
					Data: errStr,
				},
				funl.Value{Kind: funl.OpaqueValue, Data: &OpaqueCol{}},
			}
			retVal = funl.MakeListOfValues(frame, values)
			return
		}
		colName := arguments[1].Data.(string)
		col := newOpaqueCol(colName, dbVal)

		replych := make(chan error)
		adminOp := adminOP{
			optype:  "add-col",
			replych: replych,
			colName: colName,
			col:     col,
		}
		dbVal.AdminCh <- adminOp
		err := <-replych
		if err != nil {
			values = []funl.Value{
				{
					Kind: funl.BoolValue,
					Data: false,
				},
				{
					Kind: funl.StringValue,
					Data: fmt.Sprintf("%s: error in creating col: %v", name, err),
				},
				funl.Value{Kind: funl.OpaqueValue, Data: &OpaqueCol{}},
			}
			retVal = funl.MakeListOfValues(frame, values)
			return
		}
		//dbVal.addCol(col, colName) <- doesnt need to be here
		values = []funl.Value{
			{
				Kind: funl.BoolValue,
				Data: true,
			},
			{
				Kind: funl.StringValue,
				Data: "",
			},
			funl.Value{Kind: funl.OpaqueValue, Data: col},
		}
		retVal = funl.MakeListOfValues(frame, values)
		return
	}
}

func GetVZGetColNames(name string) FZProc {
	checkValidity := func(arguments []funl.Value) (bool, string) {
		if l := len(arguments); l != 1 {
			return false, fmt.Sprintf("%s: wrong amount of arguments (%d), need one", name, l)
		}
		if arguments[0].Kind != funl.OpaqueValue {
			return false, fmt.Sprintf("%s: requires opaque value", name)
		}
		return true, ""
	}

	return func(frame *funl.Frame, arguments []funl.Value) (retVal funl.Value) {
		ok, errStr := checkValidity(arguments)
		var dbVal *OpaqueDB
		if ok {
			dbVal, ok = arguments[0].Data.(*OpaqueDB)
			if !ok {
				errStr = "assuming db value"
			}
		}
		var values []funl.Value
		if !ok {
			values = []funl.Value{
				{
					Kind: funl.BoolValue,
					Data: false,
				},
				{
					Kind: funl.StringValue,
					Data: errStr,
				},
				funl.MakeListOfValues(frame, []funl.Value{}),
			}
			retVal = funl.MakeListOfValues(frame, values)
			return
		}
		colNames := dbVal.getColNames()
		var colNameValues []funl.Value
		for _, colName := range colNames {
			colNameVal := funl.Value{Kind: funl.StringValue, Data: colName}
			colNameValues = append(colNameValues, colNameVal)
		}
		values = []funl.Value{
			{
				Kind: funl.BoolValue,
				Data: true,
			},
			{
				Kind: funl.StringValue,
				Data: "",
			},
			funl.MakeListOfValues(frame, colNameValues),
		}
		retVal = funl.MakeListOfValues(frame, values)
		return
	}
}

func GetVZGetCol(name string) FZProc {
	checkValidity := func(arguments []funl.Value) (bool, string) {
		if l := len(arguments); l != 2 {
			return false, fmt.Sprintf("%s: wrong amount of arguments (%d), need two", name, l)
		}
		if arguments[0].Kind != funl.OpaqueValue {
			return false, fmt.Sprintf("%s: requires opaque value", name)
		}
		if arguments[1].Kind != funl.StringValue {
			return false, fmt.Sprintf("%s: requires string value", name)
		}
		return true, ""
	}

	return func(frame *funl.Frame, arguments []funl.Value) (retVal funl.Value) {
		ok, errStr := checkValidity(arguments)
		var dbVal *OpaqueDB
		if ok {
			dbVal, ok = arguments[0].Data.(*OpaqueDB)
			if !ok {
				errStr = "assuming db value"
			}
		}
		var values []funl.Value
		if !ok {
			values = []funl.Value{
				{
					Kind: funl.BoolValue,
					Data: false,
				},
				{
					Kind: funl.StringValue,
					Data: errStr,
				},
				funl.Value{Kind: funl.OpaqueValue, Data: &OpaqueCol{}},
			}
			retVal = funl.MakeListOfValues(frame, values)
			return
		}
		colName := arguments[1].Data.(string)
		col, found := dbVal.getCol(colName)
		var errText string
		if !found {
			errText = "col not found"
		}
		values = []funl.Value{
			{
				Kind: funl.BoolValue,
				Data: found,
			},
			{
				Kind: funl.StringValue,
				Data: errText,
			},
			funl.Value{Kind: funl.OpaqueValue, Data: col},
		}
		retVal = funl.MakeListOfValues(frame, values)
		return
	}
}

func GetVZDelCol(name string) FZProc {
	checkValidity := func(arguments []funl.Value) (bool, string) {
		if l := len(arguments); l != 1 {
			return false, fmt.Sprintf("%s: wrong amount of arguments (%d), need one", name, l)
		}
		if arguments[0].Kind != funl.OpaqueValue {
			return false, fmt.Sprintf("%s: requires opaque value", name)
		}
		return true, ""
	}

	return func(frame *funl.Frame, arguments []funl.Value) (retVal funl.Value) {
		ok, errStr := checkValidity(arguments)
		if !ok {
			funl.RunTimeError2(frame, errStr)
		}
		isTxn, col, txn := getColAndTxn(arguments[0])
		if (col == nil) && (txn == nil) {
			funl.RunTimeError2(frame, "invalid col")
		}
		var values []funl.Value
		if isTxn {
			values := []funl.Value{
				{
					Kind: funl.BoolValue,
					Data: false,
				},
				{
					Kind: funl.StringValue,
					Data: "del-col unusable for transaction",
				},
			}
			retVal = funl.MakeListOfValues(frame, values)
			return
		}

		replyCh := make(chan funl.Value)
		request := &req{
			reqType: delColReq,
			reqData: arguments[0],
			replyCh: replyCh,
			frame:   frame,
		}
		col.ch <- *request
		retVal = <-replyCh

		values = []funl.Value{
			{
				Kind: funl.BoolValue,
				Data: true,
			},
			{
				Kind: funl.StringValue,
				Data: "",
			},
		}
		retVal = funl.MakeListOfValues(frame, values)
		return
	}
}

func GetVZClose(name string) FZProc {
	checkValidity := func(arguments []funl.Value) (bool, string) {
		if l := len(arguments); l != 1 {
			return false, fmt.Sprintf("%s: wrong amount of arguments (%d), need one", name, l)
		}
		if arguments[0].Kind != funl.OpaqueValue {
			return false, fmt.Sprintf("%s: requires opaque value", name)
		}
		return true, ""
	}

	return func(frame *funl.Frame, arguments []funl.Value) (retVal funl.Value) {
		ok, errStr := checkValidity(arguments)
		var dbVal *OpaqueDB
		if ok {
			dbVal, ok = arguments[0].Data.(*OpaqueDB)
			if !ok {
				errStr = "assuming db value"
			}
		}
		var values []funl.Value
		if !ok {
			values := []funl.Value{
				{
					Kind: funl.BoolValue,
					Data: false,
				},
				{
					Kind: funl.StringValue,
					Data: errStr,
				},
				funl.Value{Kind: funl.OpaqueValue, Data: &OpaqueDB{}},
			}
			retVal = funl.MakeListOfValues(frame, values)
			return
		}

		replych := make(chan error)
		adminOp := adminOP{
			optype:  "close-db",
			replych: replych,
		}
		dbVal.AdminCh <- adminOp
		err := <-replych

		var errText string
		if err != nil {
			errText = fmt.Sprintf("%s: error: %v", name, err)
		}
		values = []funl.Value{
			{
				Kind: funl.BoolValue,
				Data: err == nil,
			},
			{
				Kind: funl.StringValue,
				Data: errText,
			},
			funl.Value{Kind: funl.OpaqueValue, Data: dbVal},
		}
		retVal = funl.MakeListOfValues(frame, values)
		return
	}
}
