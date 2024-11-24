package fuvaluez

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/anssihalmeaho/funl/funl"
)

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
	AsList         *funl.Value
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
	AsList     *funl.Value
}

func (txn *OpaqueTxn) InvalidateList() {
	txn.AsList = nil
}

func (txn *OpaqueTxn) MakeList(frame *funl.Frame) {
	newItems := map[string]funl.Value{}
	if txn.isReadTxn {
		for k, v := range txn.snapM {
			newItems[k] = v
		}
	} else {
		for k, v := range txn.col.Items {
			newItems[k] = v
		}
		for k, v := range txn.newM {
			newItems[k] = v
		}
		for itemID := range txn.newDeleted {
			delete(newItems, itemID)
		}
	}
	values := []funl.Value{}
	for _, v := range newItems {
		values = append(values, v)
	}

	list := funl.MakeListOfValues(frame, values)
	txn.AsList = &list
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

func (col *OpaqueCol) InvalidateList() {
	col.AsList = nil
}

func (col *OpaqueCol) MakeList(frame *funl.Frame) {
	values := []funl.Value{}
	for _, v := range col.Items {
		values = append(values, v)
	}
	list := funl.MakeListOfValues(frame, values)
	col.AsList = &list
}

// Run runs updator
func (col *OpaqueCol) Run(frame *funl.Frame) {
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

		case asListReq:
			if col.AsList == nil {
				col.MakeList(req.frame)
			}
			req.replyCh <- *col.AsList

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
			col.InvalidateList()
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
					{
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
			col.InvalidateList()
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
					{
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
					col.InvalidateList()
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
				{
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
					col.InvalidateList()
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

func newOpaqueCol(frame *funl.Frame, colName string, dbVal *OpaqueDB) *OpaqueCol {
	col := &OpaqueCol{
		Items:          make(map[string]funl.Value),
		ch:             make(chan req),
		latestSnapshot: nil,
		Db:             dbVal,
		colName:        colName,
		idCounter:      100,
	}
	go col.Run(frame)
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
	asListReq   = 8
)

type req struct {
	reqType reqType
	reqData funl.Value
	replyCh chan funl.Value
	frame   *funl.Frame
	errCh   chan string
}
