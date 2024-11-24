package fuvaluez

import (
	"fmt"
	"strconv"

	"github.com/anssihalmeaho/funl/funl"
)

type FZProc func(frame *funl.Frame, arguments []funl.Value) (retVal funl.Value)

func GetVZOpen(name string) FZProc {
	checkValidity := func(arguments []funl.Value) (bool, string) {
		l := len(arguments)
		if l != 1 && l != 2 {
			return false, fmt.Sprintf("%s: wrong amount of arguments (%d), need one", name, l)
		}
		if arguments[0].Kind != funl.StringValue {
			return false, fmt.Sprintf("%s: requires string value", name)
		}
		if l == 2 && arguments[1].Kind != funl.MapValue {
			return false, fmt.Sprintf("%s: requires map value", name)
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
				{Kind: funl.OpaqueValue, Data: &OpaqueDB{}},
			}
			retVal = funl.MakeListOfValues(frame, values)
			return
		}

		// parse options map (if given)
		var isInMem bool
		if len(arguments) == 2 {
			keyvals := funl.HandleKeyvalsOP(frame, []*funl.Item{{Type: funl.ValueItem, Data: arguments[1]}})
			kvListIter := funl.NewListIterator(keyvals)
			for {
				nextKV := kvListIter.Next()
				if nextKV == nil {
					break
				}
				kvIter := funl.NewListIterator(*nextKV)
				keyv := *(kvIter.Next())
				valv := *(kvIter.Next())
				if keyv.Kind != funl.StringValue {
					funl.RunTimeError2(frame, "%s: option key not a string: %v", name, keyv)
				}
				switch keyStr := keyv.Data.(string); keyStr {
				case "in-mem":
					if valv.Kind != funl.BoolValue {
						funl.RunTimeError2(frame, "%s: %s value not bool: %v", name, keyStr, keyv)
					}
					isInMem = valv.Data.(bool)
				}
			}
		}

		dbName := arguments[0].Data.(string)
		dbVal := newOpaqueDB(dbName)
		dbVal.inMemOnly = isInMem
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
			{Kind: funl.OpaqueValue, Data: dbVal},
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
			{
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
					{
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
				txn.InvalidateList()
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
					{
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
			txn.InvalidateList()
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
							{
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
					{
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
					{
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
		return false, col, nil
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
			txn.InvalidateList()
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

func GetVZItems(name string) FZProc {
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

		if isTxn {
			func() {
				txn.Lock()
				defer txn.Unlock()

				if txn.AsList == nil {
					txn.MakeList(frame)
				}
				retVal = *txn.AsList
			}()
			return
		}

		replyCh := make(chan funl.Value)
		request := &req{
			reqType: asListReq,
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
				{Kind: funl.OpaqueValue, Data: &OpaqueCol{}},
			}
			retVal = funl.MakeListOfValues(frame, values)
			return
		}
		colName := arguments[1].Data.(string)
		col := newOpaqueCol(frame, colName, dbVal)

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
				{Kind: funl.OpaqueValue, Data: &OpaqueCol{}},
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
			{Kind: funl.OpaqueValue, Data: col},
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
				{Kind: funl.OpaqueValue, Data: &OpaqueCol{}},
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
			{Kind: funl.OpaqueValue, Data: col},
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
		<-replyCh

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
				{Kind: funl.OpaqueValue, Data: &OpaqueDB{}},
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
			{Kind: funl.OpaqueValue, Data: dbVal},
		}
		retVal = funl.MakeListOfValues(frame, values)
		return
	}
}
