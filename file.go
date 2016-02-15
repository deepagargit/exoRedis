/* 
	Copyright 2016 Deepak Agarwal
	Author : Deepak Agarwal
*/

package main

import (
         "encoding/gob"
         "os"
	 "fmt"
	 "errors"
	 "bytes"
	 "sync"
	 "github.com/emirpasic/gods/sets/treeset"
)

func (store *db) Save(filename string) (bool){

	 store.mapDBLock.Lock()
	 store.setmapDBLock.Lock()

	 defer store.mapDBLock.Unlock()
	 defer store.setmapDBLock.Unlock()
	 
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
         
	 store.mapDBLock.Lock()
	 store.setmapDBLock.Lock()

	 defer store.mapDBLock.Unlock()
	 defer store.setmapDBLock.Unlock()
	 
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
		
		
		for key,value := range store.setmapEntry {
			fmt.Fprintln(&b, key)
			
			//Marshal setmapData.setEntry
				fmt.Fprintln(&b, len(value.setEntry))

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
	
	for i:= 0 ; i<len; i++ {
		var key string
		var len2 int
		var key2 int
		
		_, err = fmt.Fscanln(b, &key)
		if err != nil {
			return errors.New(fmt.Sprintf("UnmarshalBinary : setmapEntry key nil"))
		}

		_, err = fmt.Fscanln(b, &len2)
		if err != nil {
			return errors.New(fmt.Sprintf("UnmarshalBinary : setmapEntry key : %v setEntry len2 nil", key))
		}

		setmapEntry := &setmapData{
					setEntry: make(map[int]*treeset.Set),
					lock: &sync.RWMutex{},
					}

		for k:=0; k<len2; k++ {

			_, err = fmt.Fscanln(b, &key2)
			if err != nil {
				return errors.New(fmt.Sprintf("UnmarshalBinary : setmapEntry key : %v setEntry len2 : %v key nil", key, len2))
			}

			

			setmapEntry.setEntry[key2] = treeset.NewWithStringComparator()

			var setlen int = 0
			_, err = fmt.Fscanln(b, &setlen)
			if err != nil {
				return errors.New(fmt.Sprintf("UnmarshalBinary : setmapEntry key : %v setEntry len nil", key))
			}
			
			for j:=0; j< setlen; j++ {
				var val string
				_, err = fmt.Fscanln(b, &val)
				if err != nil {
					return errors.New(fmt.Sprintf("UnmarshalBinary : setmapEntry key : %v setEntry len : %v key2 : %v value nil for cursetIndex : %v ", key, setlen, key2, j))
				}

				setmapEntry.setEntry[key2].Add(val)
			}
					
		}
		//UnMarshal setmapData.lock skipped - not required

		store.setmapEntry[key] = setmapEntry

	}

	//UnMarshal setmapDBLock skipped - not required

	return err
}
