/* 
	Copyright 2016 Deepak Agarwal
	Author : Deepak Agarwal
*/

package main

import (
	"sync"
	"github.com/emirpasic/gods/sets/treeset"
	"fmt"
	"time"
	"errors"
	"bytes"
)



/*
  Storing data of form ZADD key score1 member1 member 2 [score3 member3]
  setmapEntry map[key] value is pointer to setmapData
  setmapData map[score] value is set - member1 member 2

*/

type setmapData struct {
	setEntry map[int]*treeset.Set
	lock *sync.RWMutex
}

/*
  Storing data for form SET key member
  mapEntry map[key] value is pointer to mapData
  mapData val is member
*/

type mapData struct {
	val string
	Expiration int64
	lock *sync.RWMutex
}

/*
  DB structure
  
  mapEntry is holding key-data pair
  mapDBLock is glocal RW lock on mapEntry
  mapData.lock is RW lock per key of mapEntry

  setmapEntry is holding key-data pair
  setmapData is holding score member1 member2 for a key
  setmapDBLock is glocal RW lock on setmapEntry
  setmapData.lock is RW lock per key of setmapEntry

  Synchrinization
  1. mapEnry
     a. GET - Holds global read lock and key read lock for operation
     b. SET - Holds global read lock and key write lock for operation, iff key present
              Holds global write lock and key write lock for operation, iff key absent
     c. DELETE - Holds global write lock for operation
     d. DB SAVE\LOAD - Holds global write lock for operation

     The scheme ensures 
     i. GET\SET concurrent operations
     ii. DELETE exclusive operation
     iii. SAVE\LOAD exclusive operation

  2. setmapEntry
     a. ZRANGE\ZCOUNT\ZCARD - Holds key read lock for operation
     b. ZADD - Holds global read lock and key write lock for operation, iff key present
               Holds global write lock and key write lock for operation, iff key absent
     c. DB SAVE\LOAD - Holds global write lock for operation

     The scheme ensures 
     i. ZRANGE\ZCOUNT\ZCARD\ZADD concurrent operations
     ii. SAVE\LOAD exclusive operation
*/

type db struct {
	mapEntry map[string]*mapData
	mapDBLock *sync.RWMutex
	onEvicted  func(string, interface{})

	setmapEntry map[string]*setmapData
	setmapDBLock *sync.RWMutex
	caretaker *caretaker
}


/*
func (store *db) Close() error {
	
	fmt.Println("ctrl+c signal captured : ")
	store.Save(dbFile)

	fmt.Println("ctrl+c done")

	return nil
}
*/


// Sets an (optional) function that is called with the key and value when an
// item is evicted from the cache. (Including when it is deleted manually, but
// not when it is overwritten.) Set to nil to disable.
func (store *db) OnEvicted(f func(string, interface{})) {
	store.mapDBLock.Lock()
	store.onEvicted = f
	store.mapDBLock.Unlock()
}


type keyAndValue struct {
	key   string
	value interface{}
}

// Delete all expired items from the cache.
func (store *db) DeleteExpired() {
	if store == nil {
		fmt.Println("DeleteExpired : store is nil")
		return
	}

	var evictedItems []keyAndValue
	now := time.Now().UnixNano()

	store.mapDBLock.Lock()
	defer store.mapDBLock.Unlock()
	
	for key, entry := range store.mapEntry {
		// "Inlining" of expired
		if entry.Expiration > 0 && now > entry.Expiration {
			entry, evicted := store.Delete(key)
			if evicted {
				evictedItems = append(evictedItems, keyAndValue{key, entry})
			}
		}
	}

	for _, v := range evictedItems {
		store.onEvicted(v.key, v.value)
	}
}


func (store *db) Delete(key string) (interface{}, bool) {
	if store == nil {
		fmt.Println("Delete : store is nil")
		return nil, false
	}

	if store.onEvicted != nil {
		if entry, found := store.mapEntry[key]; found {
			delete(store.mapEntry, key)
			return entry, true
		}
	}
	delete(store.mapEntry, key)
	return nil, false
}







func (store *db) Get(key string) (string, error) {
	if store == nil {
		fmt.Println("Get : store is nil")
		return "", errors.New(fmt.Sprint("GET : store is nil"))
	}

	/* Take Global Read lock to hold Delete key until Get operation is finished */
	store.mapDBLock.RLock()
	defer store.mapDBLock.RUnlock()

	entry, ok := store.mapEntry[key]

	if ok == false {
		return "", errors.New(fmt.Sprint("GET : key ", key, " not found"))
	}

	/* Take DB entry Rlock before get, this is lock per key entry to have key level mutual exclusion between get and set */
	entry.lock.RLock()
	defer entry.lock.RUnlock()

	if len(entry.val) == 0 {
		return entry.val, nil
	}
	
	var b bytes.Buffer
	for i := 0; i < len(entry.val); i++ {
		var byt byte = entry.val[i]

		// Printing ASCII with value range 33-126 as char and others as hex
		if byt < 33 || byt > 126 {
			fmt.Fprint(&b, "\\x")
			fmt.Fprint(&b, fmt.Sprintf("%x",byt))
		} else {
			fmt.Fprint(&b, fmt.Sprint(string(entry.val[i])))
		}
	}

	return string(b.Bytes()), nil
}


func (store *db) GetBit(key string, offset int) (string, error) {
	if store == nil {
		fmt.Println("GetBit : store is nil")
		return "", errors.New(fmt.Sprint("GETBIT : store is nil"))
	}

	/* Take Global Read lock to hold Delete key until Get operation is finished */
	store.mapDBLock.RLock()
	defer store.mapDBLock.RUnlock()
	
	entry, ok := store.mapEntry[key]
	
	var bitFlag string = "0"

	if ok == false {
		return "", errors.New(fmt.Sprint("GETBIT : key ", key, " not found"))
	} 

	var val []byte = []byte(entry.val)

	/* Take DB entry Rlock before get, this is lock per key entry to have key level mutual exclusion between get and set */
	entry.lock.RLock()
	defer entry.lock.RUnlock()

	var sliceIndex int = (offset / 8) 

	if sliceIndex < len(val) {

		byteData := val[sliceIndex]

		byteoffset := 7 - (uint)(offset % 8)
			
		if (byteData & (1 << byteoffset)) != 0 {
			bitFlag = "1"
		} else {
			bitFlag = "0"
		}

		return bitFlag, nil
	} else {
		/* Out of index case */
		return bitFlag, nil
	}

}

func (store *db) Set(key string, val string, d time.Duration) (bool, error) {
	if store == nil {
		fmt.Println("Set : store is nil")
		return false, errors.New(fmt.Sprint("SET : store is nil"))
	}

	var entry *mapData
	var ok bool
	var e int64

	if d > 0 {
		e = time.Now().Add(d).UnixNano()
	}
	
	/* Take Global Read lock to hold Delete key or Save DB until Set operation is finished */
	store.mapDBLock.RLock()

	/* Check if db has the key entry */
	entry, ok = store.mapEntry[key]

	/* If entry not present */
	if ok == false {
		/* Take Global DB write lock before creating entry for this key, to ensure only one entry gets created */
		store.mapDBLock.RUnlock()
		store.mapDBLock.Lock()

		/* Again Check if db has the key entry, between above if & db lock it is possible other routine has created this entry */
		var okrecheck bool
		entry, okrecheck = store.mapEntry[key]

		if okrecheck == false {
			entry = &mapData{
				val: val, 
				Expiration : e,
				lock: &sync.RWMutex{},
				}

			/* ToDo : Handle allocation failure of struct, now sure how todo in Go  */
		}
	}
	
	/* Take DB entry lock before set, this is lock per key entry to prevent race-condition in multiple sets on same key */
	entry.lock.Lock()

	entry.val = val
	entry.Expiration = e
	store.mapEntry[key] = entry

	entry.lock.Unlock()

	/* Both locks got released in LIFO order, entry lock followed by db lock */
	if ok == false {
		store.mapDBLock.Unlock()
	} else {
		store.mapDBLock.RUnlock()
	}

	return true, nil
}


func (store *db) SetBit(key string, offset int, bit byte, d time.Duration) (string, error){
	if store == nil {
		fmt.Println("SetBit : store is nil")
		return "", errors.New(fmt.Sprint("SETBIT : store is nil"))
	}

	var sliceIndex int = (offset / 8) 
	var sliceLength int = sliceIndex +1
	
	byteoffset := 7 - (uint)(offset % 8)
	var val []byte 

	var entry *mapData
	var ok bool
	var e int64

	if d > 0 {
		e = time.Now().Add(d).UnixNano()
	}
	
	/* Take Glocal Read lock to hold Delete key or Save DB until Set operation is finished */
	store.mapDBLock.RLock()

	/* Check if db has the key entry */
	entry, ok = store.mapEntry[key]

	/* If entry not present */
	if ok == false {
		/* Take DB lock before creating entry for this key, to ensure only one entry gets created */
		store.mapDBLock.RUnlock()
		store.mapDBLock.Lock()
		
		/* Again Check if db has the key entry, between above if & db lock it is possible other routine has created this entry */
		var okrecheck bool
		entry, okrecheck = store.mapEntry[key]

		if okrecheck == false {
			entry = &mapData{
				val: "", 
				Expiration : e,
				lock: &sync.RWMutex{},
				}
			/* ToDo : Handle allocation failure of struct, now sure how todo in Go  */
		}
	}
	
	/* Take DB entry lock before set, this is lock per key entry */
	entry.lock.Lock()
	

	if ok == true {
		if sliceLength < len([]byte(entry.val)) {
			// Do nothing here
			val = []byte(entry.val)
		} else {
			val_old := []byte(entry.val)
			val = make([]byte, sliceLength)	

			for i:= 0; i< sliceLength ; i++ {
				if i < len([]byte(entry.val)) {
					val[i] = val_old[i]
				} else {
					val[i] = 0
				}
			}
		}
	
	} else {
		val = make([]byte, sliceLength)
		for i:= 0; i< sliceLength ; i++ {
			val[i] = 0
		}	
	}

	var oldBit byte = 0
	var maskBit byte = (1 << byteoffset)
	oldBit = val[sliceIndex] & maskBit
	bitFlag := ""
	if oldBit == 0 {
		bitFlag = "0"
	} else {
		bitFlag = "1"
	}

	if bit == 0 {
		var mask byte = ^(1 << byteoffset)
		val[sliceIndex] = val[sliceIndex] & mask
	} else {
		var mask byte = (1 << byteoffset)
		val[sliceIndex] = val[sliceIndex] | mask
	}

	entry.val = string(val)
	entry.Expiration = e
	store.mapEntry[key] = entry

	entry.lock.Unlock()
	
	if ok == false {
		store.mapDBLock.Unlock()
	} else {
		store.mapDBLock.RUnlock()
	}

	return bitFlag, nil
}



func (store *db) SetNX(key string, val string, d time.Duration) (bool, error) {
	if store == nil {
		fmt.Println("SetNX : store is nil")
		return false, errors.New(fmt.Sprint("SET NX : store is nil"))
	}
	
	var entry *mapData
	var ok bool
	var e int64

	if d > 0 {
		e = time.Now().Add(d).UnixNano()
	}

	/* Take Glocal Read lock to hold Delete key or Save DB until Set operation is finished */
	store.mapDBLock.RLock()

	/* Check if db has the key entry */
	entry, ok = store.mapEntry[key]

	/* If entry not present */
	if ok == true {
		store.mapDBLock.RUnlock()
		return false, errors.New(fmt.Sprintf("SET NX : err store.setmapEntry key", key, "present"))
	} else {
		/* Take DB lock before creating entry for this key, to ensure only one entry gets created */
		store.mapDBLock.RUnlock()
		store.mapDBLock.Lock()
		
		/* Again Check if db has the key entry, between above if & db lock it is possible other routine has created this entry */
		entry, ok = store.mapEntry[key]

		if ok == false {
			entry = &mapData{
				val: val, 
				Expiration : e,
				lock: &sync.RWMutex{},
				}
			/* ToDo : Handle allocation failure of struct, now sure how todo in Go  */
		}
	}
	
	/* Take DB entry lock before set, this is lock per key entry */
	entry.lock.Lock()

	entry.val = val
	entry.Expiration = e
	store.mapEntry[key] = entry

	entry.lock.Unlock()

	/* Both locks got released in LIFO order, entry lock followed by db lock */
	if ok == false {
		store.mapDBLock.Unlock()
	} else {
		store.mapDBLock.RUnlock()
	}

	return true, nil
}


func (store *db) SetXX(key string, val string, d time.Duration) (bool, error){
	if store == nil {
		fmt.Println("SetXX : store is nil")
		return false, errors.New(fmt.Sprint("SET XX : store is nil"))
	}

	store.mapDBLock.RLock()
	defer store.mapDBLock.RUnlock()

	entry, ok := store.mapEntry[key]
	if ok == true {
		/* Take DB entry lock before set, this is lock per key entry */
		entry.lock.Lock()
		defer entry.lock.Unlock()

		entry.val = val
		/* entry is pointer so no need to set again in map. store.mapEntry[key] = entry */
	} else {
		return ok, errors.New(fmt.Sprint("SET XX : key ", key, " not present"))
	}

	return ok, nil
}






/* You may assume single [score member] inserts.  */
func (store *db) ZADD(key string, zaddMap *map[string]int) (int, error) {
	var memberAdded int = 0

	if store == nil {
		fmt.Println("ZADD : store is nil")
		return 0, errors.New(fmt.Sprint("ZADD : store is nil"))
	}

	if zaddMap == nil {
		fmt.Println("ZADD : argument zaddMap is nil")
		return 0, errors.New(fmt.Sprint("ZADD : Internal error"))
	}

	var entry *setmapData
	var ok bool

	store.setmapDBLock.RLock()

	/* Check if db has the key entry */
	entry, ok = store.setmapEntry[key]

	/* If entry not present */
	if ok == false {
		/* Take DB lock before creating entry for this key, to ensure only one entry gets created */
		store.setmapDBLock.RUnlock()
		store.setmapDBLock.Lock()
		
		/* Again Check if db has the key entry, between above if & db lock it is possible other routine has created this entry */
		var okrecheck bool
		entry, okrecheck = store.setmapEntry[key]

		if okrecheck == false {
			entry = &setmapData{
				setEntry: make(map[int]*treeset.Set),
				lock: &sync.RWMutex{},
				}

			/* ToDo : Handle allocation failure of struct, now sure how todo in Go  */
						
		}
	}
	
	/* Take DB entry lock before set, this is lock per key entry */
	entry.lock.Lock()

	for member, score := range *zaddMap {

		/* Remove if same member exist at any score */
		for _,value := range entry.setEntry {
			if true == value.Contains(member) {
				memberAdded = memberAdded - 1
				value.Remove(member)
			}
		}

		/* Insert the score member pair */
		if entry.setEntry[score] == nil {
			entry.setEntry[score] = treeset.NewWithStringComparator()
		}

		entry.setEntry[score].Add(member)	
		memberAdded = memberAdded + 1
	}
	
	store.setmapEntry[key] = entry

	entry.lock.Unlock()

	/* Both locks got released in LIFO order, entry lock followed by db lock */
	if ok == false {
		store.setmapDBLock.Unlock()
	} else {
		store.setmapDBLock.RUnlock()
	}
	
	return memberAdded, nil

}

func (store *db) ZCARD(key string) (int, error) {
	if store == nil {
		fmt.Println("ZCARD : store is nil")
		return 0, errors.New(fmt.Sprint("ZCARD : store is nil"))
	}

	entry, ok := store.setmapEntry[key]
	var count int = 0

	if ok == false {
		return count, errors.New(fmt.Sprint("ZCARD : key ", key, " not found"))
	}

	/* Take DB entry Rlock before get, this is lock per key entry */
	entry.lock.RLock()
	defer entry.lock.RUnlock()
	
	for _,value := range entry.setEntry {
		count = count + value.Size() 
	}

	return count, nil
}

/* Assume min and max are always inclusive. ie, You do not need to support the '(' notation for exclusive ranges */
func (store *db) ZCOUNT(key string, min int, max int) (int, error) {  
	if store == nil {
		fmt.Println("ZCARD : store is nil")
		return 0, errors.New(fmt.Sprint("ZCOUNT : store is nil"))
	}

	entry, ok := store.setmapEntry[key]
	var count int = 0

	if ok == false {
		return count, errors.New(fmt.Sprint("ZCOUNT : key ", key, "not found"))
	}

	/* Take DB entry Rlock before get, this is lock per key entry */
	entry.lock.RLock()
	defer entry.lock.RUnlock()
	
	for key,value := range entry.setEntry {
		if key >= min && key <= max {
			count = count + value.Size() 
			fmt.Println("set : ", value.String())
		}
	}

	return count, nil
}

func (store *db) ZRANGE(key string, start int, stop int) (*map[string]int, error) {
	if store == nil {
		fmt.Println("ZRANGE : store is nil")
		return nil, errors.New(fmt.Sprint("ZADD : store is nil"))
	}

	entry, ok := store.setmapEntry[key]
	retMap := make(map[string]int)

	if ok == false {
		return nil, errors.New(fmt.Sprint("ZRANGE : key ", key, " not found"))
	}

	/* Take DB entry Rlock before get, this is lock per key entry */
	entry.lock.RLock()
	defer entry.lock.RUnlock()
	
	for key,value := range entry.setEntry {
		if key >= start && key <= stop {

			items := []string{}
			for _, v := range value.Values() {
				items = append(items, fmt.Sprintf("%v", v))
			}


			for _, elementString := range items {
				retMap[elementString] = key
			}
		}
	}

	return &retMap, nil
}



