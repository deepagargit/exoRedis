package main

import (
	"sync"
	"encoding/gob"
         "os"
	 "github.com/emirpasic/gods/sets/treeset"
	//"log"
	  "fmt"
	  "errors"
	  "bytes"
	  "time"
)

const (
	// For use with functions that take an expiration time.
	NoExpiration time.Duration = -1
	// For use with functions that take an expiration time. Equivalent to
	// passing in the same expiration duration as was given to New() or
	// NewFrom() when the cache was created (e.g. 5 minutes.)
	DefaultExpiration time.Duration = 0
)

type mapData struct {
	val string
	Expiration int64
	lock *sync.RWMutex
}


/*
  Storing data of form ZADD key score1 member1 member 2 [score3 member3]
  setmapEntry map[key] value is pointer to setmapData
  setmapData map[score] value is set - member1 member 2

*/
type setmapData struct {
	setEntry map[int]*treeset.Set
	lock *sync.RWMutex
}

type db struct {
	mapEntry map[string]*mapData
	mapDBLock *sync.Mutex
	onEvicted  func(string, interface{})

	setmapEntry map[string]*setmapData
	setmapDBLock *sync.Mutex
	caretaker *caretaker
}


func (store *db) Close() error {

    fmt.Println("ctrl+c signal captured : ")
		//time.Sleep(2000 * time.Second)
		store.Save(dbFile)

		fmt.Println("ctrl+c done")

	return nil
}


func (store *db) delete(key string) (interface{}, bool) {
	if store.onEvicted != nil {
		if entry, found := store.mapEntry[key]; found {
			delete(store.mapEntry, key)
			return entry, true
		}
	}
	delete(store.mapEntry, key)
	return nil, false
}

type keyAndValue struct {
	key   string
	value interface{}
}

// Delete all expired items from the cache.
func (store *db) DeleteExpired() {
	var evictedItems []keyAndValue
	now := time.Now().UnixNano()

	store.mapDBLock.Lock()
	defer store.mapDBLock.Unlock()
	
	for key, entry := range store.mapEntry {
		// "Inlining" of expired
		if entry.Expiration > 0 && now > entry.Expiration {
			entry, evicted := store.delete(key)
			if evicted {
				evictedItems = append(evictedItems, keyAndValue{key, entry})
			}
		}
	}

	for _, v := range evictedItems {
		store.onEvicted(v.key, v.value)
	}
}


// Sets an (optional) function that is called with the key and value when an
// item is evicted from the cache. (Including when it is deleted manually, but
// not when it is overwritten.) Set to nil to disable.
func (store *db) OnEvicted(f func(string, interface{})) {
	store.mapDBLock.Lock()
	store.onEvicted = f
	store.mapDBLock.Unlock()
}

func (store *db) Save(filename string) (bool){

	 store.mapDBLock.Lock()
	 store.setmapDBLock.Lock()

	 defer store.mapDBLock.Lock()
	 defer store.setmapDBLock.Lock()

         // create a file
         dataFile, err := os.Create(filename)

         if err != nil {
                 fmt.Println("Save Create error : " , err)
                 return false
         }

         enc := gob.NewEncoder(dataFile)
         err = enc.Encode(store)

	 if err != nil {
		fmt.Printf("Save Encode error : ", err)
		return false
	}

         dataFile.Close()
	 return true
}


func (store *db) Load(filename string) (bool){
         // open data file

         dataFile, err := os.Open(filename)

         if err != nil {
                 fmt.Println("Load Open error : ", err)
                 return false
         }

         dec := gob.NewDecoder(dataFile)
	 
         err = dec.Decode(store)

         if err != nil {
                 fmt.Println("Load Decode error : ", err)
                 return false
         }

         dataFile.Close()

	 return true
 }



func (store *db) MarshalBinary() ([]byte, error) {

	if store == nil {
		return nil, errors.New(fmt.Sprintf("MarshalBinary : store nil"))
	}

	if store.mapEntry == nil {
		return nil, errors.New(fmt.Sprintf("MarshalBinary : store.mapEntry nil"))
	}

	if store.setmapEntry == nil {
		return nil, errors.New(fmt.Sprintf("MarshalBinary : store.setmapEntry nil"))
	}

	var b bytes.Buffer
	
	//Marshal mapEntry
	if len(store.mapEntry) != 0 {
		fmt.Fprintln(&b, len(store.mapEntry))
		
		for key,value := range store.mapEntry {
			fmt.Fprintln(&b, key)

			//Marshal mapData.val
			if value != nil {
				fmt.Fprintln(&b, value.val)
				fmt.Fprintln(&b, value.Expiration)
				
				//Marshal mapData.lock skipped - not required
			} else {
				return nil, errors.New(fmt.Sprintf("MarshalBinary : err store.mapEntry key :  %v value : nil", key))
			}
		}

		
	}

	//Marshal mapDBLock skipped - not required
	fmt.Fprintln(&b, len(store.setmapEntry))

	//Marshal setmapEntry
	if len(store.setmapEntry) != 0 {
		
		fmt.Println("len(store.setmapEntry) : ", len(store.setmapEntry))
		
		for key,value := range store.setmapEntry {
			fmt.Fprintln(&b, key)
			fmt.Println(" key : ", key)
			
			//Marshal setmapData.setEntry
				fmt.Fprintln(&b, len(value.setEntry))
				fmt.Println(" len(value.setEntry) : ", len(value.setEntry))

				if value != nil {

					for key2,value2 := range value.setEntry {
						fmt.Fprintln(&b, key2)

						if value2 != nil {

							fmt.Fprintln(&b, value2.Size())
							for _, v := range value2.Values() {
								fmt.Fprintln(&b, fmt.Sprintf("%v",v))
							}
						} else {
							return nil, errors.New(fmt.Sprintf("MarshalBinary : err store.setmapEntry key : %v setEntry key2 : %v value2 : nill", key, key2))
						}
					}
				} else {
					return nil, errors.New(fmt.Sprintf("MarshalBinary : err store.setmapEntry key :  %v value : nill", key))
				}

			//Marshal setmapData.lock skipped - not required
		}
	}

	//Marshal setmapLock skipped - not required

	return b.Bytes(), nil
}

func (store *db) UnmarshalBinary(data []byte) error {
	
	if store == nil {
		return errors.New(fmt.Sprintf("UnmarshalBinary : store nil"))
	}

	if store.mapEntry == nil {
		return errors.New(fmt.Sprintf("UnmarshalBinary : store.mapEntry nil"))
	}

	if store.setmapEntry == nil {
		return errors.New(fmt.Sprintf("UnmarshalBinary : store.setmapEntry nil"))
	}

	b := bytes.NewBuffer(data)

	//UnMarshal mapEntry
	var len int = 0
	_, err := fmt.Fscanln(b, &len)
	if err != nil {
		return errors.New(fmt.Sprintf("UnmarshalBinary : mapEntry len nil"))
	}
	
	for i:= 0 ; i<len; i++ {
		var key string
		var value string
		var e int64
		
		_, err = fmt.Fscanln(b, &key)
		if err != nil {
			return errors.New(fmt.Sprintf("UnmarshalBinary : mapEntry key nil"))
		}

		_, err = fmt.Fscanln(b, &value)
		if err != nil {
			return errors.New(fmt.Sprintf("UnmarshalBinary : mapEntry val nil"))
		}

		_, err = fmt.Fscanln(b, &e)
		if err != nil {
			return errors.New(fmt.Sprintf("UnmarshalBinary : mapEntry expiration nil"))
		}

		mapEntry := &mapData{
				val: value, 
				Expiration: e,
				lock: &sync.RWMutex{},
				}

		//UnMarshal mapData.lock skipped - not required

		store.mapEntry[key] = mapEntry
	}
	
	//UnMarshal setmapEntry
	len = 0
	_, err = fmt.Fscanln(b, &len)
	if err != nil {
		return errors.New(fmt.Sprintf("UnmarshalBinary : setmapEntry len nil"))
	}
	fmt.Println(" len : ", len)
	
	for i:= 0 ; i<len; i++ {
		var key string
		var len2 int
		var key2 int
		
		_, err = fmt.Fscanln(b, &key)
		if err != nil {
			return errors.New(fmt.Sprintf("UnmarshalBinary : setmapEntry key nil"))
		}
		fmt.Println(" key : ", key)

		_, err = fmt.Fscanln(b, &len2)
		if err != nil {
			return errors.New(fmt.Sprintf("UnmarshalBinary : setmapEntry key : %v setEntry len2 nil", key))
		}
		fmt.Println(" len2 : ", len2)

		setmapEntry := &setmapData{
					setEntry: make(map[int]*treeset.Set),
					lock: &sync.RWMutex{},
					}

		for k:=0; k<len2; k++ {

			_, err = fmt.Fscanln(b, &key2)
			if err != nil {
				return errors.New(fmt.Sprintf("UnmarshalBinary : setmapEntry key : %v setEntry len2 : %v key nil", key, len2))
			}
			fmt.Println(" key2 : ", key2)

			

			setmapEntry.setEntry[key2] = treeset.NewWithStringComparator()

			var setlen int = 0
			_, err = fmt.Fscanln(b, &setlen)
			if err != nil {
				return errors.New(fmt.Sprintf("UnmarshalBinary : setmapEntry key : %v setEntry len nil", key))
			}
			fmt.Println(" setlen : ", setlen)
			
			for j:=0; j< setlen; j++ {
				var val string
				_, err = fmt.Fscanln(b, &val)
				if err != nil {
					return errors.New(fmt.Sprintf("UnmarshalBinary : setmapEntry key : %v setEntry len : %v key2 : %v value nil for cursetIndex : %v ", key, setlen, key2, j))
				}

				setmapEntry.setEntry[key2].Add(val)
				fmt.Println(" key2 : ", key2 , " val : ", val)
			}
					
		}
		//UnMarshal setmapData.lock skipped - not required

		store.setmapEntry[key] = setmapEntry

	}

	//UnMarshal setmapDBLock skipped - not required

	return err
}


func (store *db) Get(key string) (string, bool) {
	
	entry, ok := store.mapEntry[key]

	if ok == false {
		return "", ok
	}

	/* Take DB entry Rlock before get, this is lock per key entry */
	entry.lock.RLock()
	defer entry.lock.RUnlock()

	return entry.val, ok
}


func (store *db) GetBit(key string, offset int) (string) {
		
	entry, ok := store.mapEntry[key]

	var val []byte = []byte(entry.val)
	var bitFlag string = "0"

	if ok == false {
		return bitFlag
	} else {

		/* Take DB entry Rlock before get, this is lock per key entry */
		entry.lock.RLock()
		defer entry.lock.RUnlock()

		var sliceIndex int = (offset / 8) 

		if sliceIndex < len(val) {
			/*
			log.Printf(" val = %s", string(val))
			fmt.Println(" byte array : ", val)
			for i:= 0; i < len(val); i++ {
				log.Printf("i = %d byte = %d", i, val[i])
			}
			*/

			byteData := val[sliceIndex]

			byteoffset := (uint)(offset % 8)
			//log.Printf("index = %d byte = %d byteoffset = %d", sliceIndex, byteData, byteoffset)
			
			if (byteData & (1 << byteoffset)) != 0 {
				bitFlag = "1"
			} else {
				bitFlag = "0"
			}

				

			//log.Printf("bitFlag = %s", bitFlag)

			return bitFlag
		} else {
			/* Out of index case */
			return bitFlag
		}
	}
}

func (store *db) Set(key string, val string, d time.Duration) (bool) {
	
	var entry *mapData
	var ok bool
	var e int64

	if d > 0 {
		e = time.Now().Add(d).UnixNano()
	}

	/* Check if db has the key entry */
	entry, ok = store.mapEntry[key]

	/* If entry not present */
	if ok == false {
		/* Take DB lock before creating entry for this key, to ensure only one entry gets created */
		store.mapDBLock.Lock()
		defer store.mapDBLock.Unlock()
		
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
	defer entry.lock.Unlock()

	entry.val = val
	entry.Expiration = e
	store.mapEntry[key] = entry

	/* Both locks got released in LIFO order, entry lock followed by db lock */

	return true
}


func (store *db) SetBit(key string, offset int, bit byte, d time.Duration) {

	var sliceIndex int = (offset / 8) 
	var sliceLength int = sliceIndex +1
	
	byteoffset := (uint)(offset % 8)
	var val []byte 

	var entry *mapData
	var ok bool
	var e int64

	if d > 0 {
		e = time.Now().Add(d).UnixNano()
	}

	/* Check if db has the key entry */
	entry, ok = store.mapEntry[key]

	/* If entry not present */
	if ok == false {
		/* Take DB lock before creating entry for this key, to ensure only one entry gets created */
		store.mapDBLock.Lock()
		defer store.mapDBLock.Unlock()
		
		/* Again Check if db has the key entry, between above if & db lock it is possible other routine has created this entry */
		entry, ok = store.mapEntry[key]

		if ok == false {
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
	defer entry.lock.Unlock()
	

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

	//fmt.Printf("val = ", entry.val)
	
	//log.Printf("sliceLength = %d sliceIndex = %d ", sliceLength, sliceIndex)
	
}

/*

func (store *db) SetEX(key string, val string) {
	store.mapDBLock.Lock()
	defer store.mapDBLock.Unlock()

	entry, ok := store.mapEntry[key]
	if ok == true {
		entry.val = val
		store.mapEntry[key] = entry
	} else {
		entry := &mapData{
			val: val, 
			lock: &sync.RWMutex{},
			}
		
		store.mapEntry[key] = entry
	}
}

func (store *db) SetPX(key string, val string) {
	store.mapDBLock.Lock()
	defer store.mapDBLock.Unlock()

	entry, ok := store.mapEntry[key]
	if ok == true {
		entry.val = val
		store.mapEntry[key] = entry
	} else {
		entry := &mapData{
			val: val, 
			lock: &sync.RWMutex{},
			}
		
		store.mapEntry[key] = entry
	}
}

*/


func (store *db) SetNX(key string, val string, d time.Duration) (bool) {
	
	var entry *mapData
	var ok bool
	var e int64

	if d > 0 {
		e = time.Now().Add(d).UnixNano()
	}

	/* Check if db has the key entry */
	entry, ok = store.mapEntry[key]

	/* If entry not present */
	if ok == true {
		return false
	} else {
		/* Take DB lock before creating entry for this key, to ensure only one entry gets created */
		store.mapDBLock.Lock()
		defer store.mapDBLock.Unlock()
		
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
	defer entry.lock.Unlock()

	entry.val = val
	entry.Expiration = e
	store.mapEntry[key] = entry

	/* Both locks got released in LIFO order, entry lock followed by db lock */

	return true
}


func (store *db) SetXX(key string, val string, d time.Duration) (bool){
	store.mapDBLock.Lock()
	defer store.mapDBLock.Unlock()

	entry, ok := store.mapEntry[key]
	if ok == true {
		/* Take DB entry lock before set, this is lock per key entry */
		entry.lock.Lock()
		defer entry.lock.Unlock()

		entry.val = val
		/* entry is pointer so no need to set again in map. store.mapEntry[key] = entry */
	}

	return ok
}






/* You may assume single [score member] inserts.  */
func (store *db) ZADD(key string, zaddMap *map[string]int) (bool) {

	if store == nil {
		fmt.Println("ZADD : store is nil")
		return false
	}

	if zaddMap == nil {
		fmt.Println("ZADD : argument zaddMap is nil")
		return false
	}

	fmt.Println(*zaddMap)

	var entry *setmapData
	var ok bool

	/* Check if db has the key entry */
	entry, ok = store.setmapEntry[key]

	/* If entry not present */
	if ok == false {
		/* Take DB lock before creating entry for this key, to ensure only one entry gets created */
		store.setmapDBLock.Lock()
		defer store.setmapDBLock.Unlock()
		
		/* Again Check if db has the key entry, between above if & db lock it is possible other routine has created this entry */
		entry, ok = store.setmapEntry[key]

		if ok == false {
			entry = &setmapData{
				setEntry: make(map[int]*treeset.Set),
				lock: &sync.RWMutex{},
				}

			/* ToDo : Handle allocation failure of struct, now sure how todo in Go  */
						
		}
	}
	
	/* Take DB entry lock before set, this is lock per key entry */
	entry.lock.Lock()
	defer entry.lock.Unlock()

	for member, score := range *zaddMap {

		/* Remove if same member exist at any score */
		for _,value := range entry.setEntry {
			if true == value.Contains(member) {
				value.Remove(member)
			}
		}

		/* Insert the score member pair */
		if entry.setEntry[score] == nil {
			entry.setEntry[score] = treeset.NewWithStringComparator()
		}

		entry.setEntry[score].Add(member)	
	}
	
	store.setmapEntry[key] = entry

	/* Both locks got released in LIFO order, entry lock followed by db lock */
	
	return true

}

func (store *db) ZCARD(key string) (int, bool) {
	entry, ok := store.setmapEntry[key]
	var count int = 0

	if ok == false {
		return count, ok
	}

	/* Take DB entry Rlock before get, this is lock per key entry */
	entry.lock.RLock()
	defer entry.lock.RUnlock()
	
	for _,value := range entry.setEntry {
		count = count + value.Size() 
	}

	return count, ok
}

/* Assume min and max are always inclusive. ie, You do not need to support the '(' notation for exclusive ranges */
func (store *db) ZCOUNT(key string, min int, max int) (int, bool) {  
	entry, ok := store.setmapEntry[key]
	var count int = 0

	if ok == false {
		return count, ok
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

	return count, ok
}

func (store *db) ZRANGE(key string, start int, stop int) (map[string]int, bool) {
	entry, ok := store.setmapEntry[key]
	retMap := make(map[string]int)

	if ok == false {
		return retMap, ok
	}

	/* Take DB entry Rlock before get, this is lock per key entry */
	entry.lock.RLock()
	defer entry.lock.RUnlock()
	
	for key,value := range entry.setEntry {
		if key >= start && key <= stop {
			fmt.Println("set : ", value.String())

			items := []string{}
			for _, v := range value.Values() {
				items = append(items, fmt.Sprintf("%v", v))
				fmt.Println("v : ", v)
			}

			fmt.Println("items : ", items)

			for _, elementString := range items {
				retMap[elementString] = key
			}

			fmt.Println("retMap : ", retMap)
		}
	}

	return retMap, ok
}



