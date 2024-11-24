package fuvaluez

import (
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/anssihalmeaho/funl/funl"
	bolt "go.etcd.io/bbolt"
)

func newOpaqueDB(dbName string) *OpaqueDB {
	return &OpaqueDB{
		name: dbName,
		cols: make(map[string]*OpaqueCol),
	}
}

// OpaqueDB represents database
type OpaqueDB struct {
	sync.RWMutex
	name       string
	cols       map[string]*OpaqueCol
	Ch         chan changes
	AdminCh    chan adminOP
	Closing    bool
	encoderVal funl.Value
	inMemOnly  bool
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

	encItem := &funl.Item{
		Type: funl.ValueItem,
		Data: funl.Value{
			Kind: funl.StringValue,
			Data: "call(proc() import stdser import stdbytes proc(x) _ _ b = call(stdser.encode x): call(stdbytes.string b) end end)",
		},
	}
	db.encoderVal = funl.HandleEvalOP(frame, []*funl.Item{encItem})

	var pStore *bolt.DB
	if !db.inMemOnly {
		var err error
		pStore, err = bolt.Open(fmt.Sprintf("%s.db", db.name), 0600, nil)
		if err != nil {
			return false, fmt.Sprintf("Storage opening failed: %v", err)
		}

		err = db.readAllcolsFromPersistent(pStore, frame)
		if err != nil {
			return false, fmt.Sprintf("Storage reading failed: %v", err)
		}
	}
	go db.run(pStore, frame)
	return true, ""
}

func (db *OpaqueDB) addColToPersistent(boltDB *bolt.DB, colName string, col *OpaqueCol) error {
	if db.inMemOnly {
		return nil
	}
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

	decItem := &funl.Item{
		Type: funl.ValueItem,
		Data: funl.Value{
			Kind: funl.StringValue,
			Data: "call(proc() import stdser import stdbytes proc(__s) b = call(stdbytes.str-to-bytes __s) _ _ __v = call(stdser.decode b): __v end end)",
		},
	}
	decoderVal := funl.HandleEvalOP(frame, []*funl.Item{decItem})

	db.Lock()
	defer db.Unlock()

	err = boltDB.View(func(tx *bolt.Tx) error {
		for _, colName := range colNames {
			colBucket := tx.Bucket([]byte(colName))
			if colBucket == nil {
				return fmt.Errorf("col not found (%s)", colName)
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

				decArgs := []*funl.Item{
					{
						Type: funl.ValueItem,
						Data: decoderVal,
					},
					{
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
			go col.Run(frame)
		}
		return nil
	})
	return
}

func (db *OpaqueDB) putKVtoPersistent(tx *bolt.Tx, frame *funl.Frame, colName string, key string, val funl.Value) error {
	colBucket := tx.Bucket([]byte(colName))
	if colBucket == nil {
		return fmt.Errorf("col not found (%s)", colName)
	}

	encArgs := []*funl.Item{
		{
			Type: funl.ValueItem,
			Data: db.encoderVal,
		},
		{
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
		return fmt.Errorf("col not found (%s)", colName)
	}
	return colBucket.Delete([]byte(key))
}

func (db *OpaqueDB) consistentChangeWrites(boltDB *bolt.DB, frame *funl.Frame, changelist []changeItem) error {
	if db.inMemOnly {
		return nil
	}
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
	if db.inMemOnly {
		return nil
	}
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
	if db.inMemOnly {
		return nil
	}
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
					adminOp.replych <- fmt.Errorf("already closing")
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
				adminOp.replych <- errors.New("unknown op")
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
