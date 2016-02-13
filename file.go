package main

/*


 import (
         "encoding/gob"
         "os"
	 "fmt"
 )

 func (store *db) save(file string) (bool){

         // create a file
         dataFile, err := os.Create(file)

         if err != nil {
                 fmt.Println(err)
                 return false
         }

         dataEncoder := gob.NewEncoder(dataFile)
         dataEncoder.Encode(store)

         dataFile.Close()
	 return true
 }


 func (store *db) Load(file string) (bool){
         // open data file

         dataFile, err := os.Open(file)

         if err != nil {
                 fmt.Println(err)
                 return false
         }

         dataDecoder := gob.NewDecoder(dataFile)
         err = dataDecoder.Decode(store)

         if err != nil {
                 fmt.Println(err)
                 return false
         }

         dataFile.Close()

	 return true
 }


*/

