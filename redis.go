
/* 
   Compile by go build

   Run by ./exoRedis
*/


package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"sync"
	"time"
	//DEATH "github.com/vrecan/death"
	//SYS "syscall"
	//"io"
)


const dbFile string = "exoRedisDBFile.gob"

const timeInterval = time.Duration(30 * time.Second)

func main() {
	log.Printf("Server started\n")
	addr := ":15000"

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Printf("Error: listen(): %s", err)
		os.Exit(1)
	}

	log.Printf("Accepting connections at: %s", addr)
	store := &db{
		mapEntry: make(map[string]*mapData),
		mapDBLock: &sync.Mutex{},
		onEvicted: display,
		setmapEntry: make(map[string]*setmapData),
		setmapDBLock: &sync.Mutex{},
	}

	runCaretaker(store, timeInterval)
	defer stopCaretaker(store)

	if len(os.Args) > 1 {

		dbFileLoad := os.Args[1]
		ok := store.Load(dbFileLoad)
		if ok == false {
			log.Printf("Load of db file %s failed", dbFileLoad)
		}
	}
	
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	var check bool = false

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	//signal.Notify(c, os.Interrupt)
	//signal.Notify(c, syscall.SIGTERM)
	
	
	
	go func(store *db, file string) {

		sig := <-sigs
		check = true

		fmt.Println("Signal captured : ", sig)
		store.Save(file)

		done <- true
		
	} (store, dbFile)
	
	var id int64
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error: Accept(): %s", err)
			continue
		}

		id++
		client := &client{id: id, conn: conn, store: store}
		go client.serve()
	}
	
	if check == true {
		fmt.Println("awaiting signal")
		<-done
	}
        fmt.Println("exiting")	
}



func display(key string, value interface{}) {
	entry := value.(*mapData)
	fmt.Println("Evicted - key : ", key, "  val : ", entry.val, "  Expiration : ", entry.Expiration, "  Now: ", time.Now().UnixNano())
}


type caretaker struct {
	Interval time.Duration
	stop     chan bool
}


func (c *caretaker) Run(store *db) {
	c.stop = make(chan bool)
	ticker := time.NewTicker(c.Interval)
	for {
		select {
		case <-ticker.C:
			store.DeleteExpired()
		case <-c.stop:
			ticker.Stop()
			return
		}
	}
}


func stopCaretaker(store *db) {
	store.caretaker.stop <- true
}

func runCaretaker(store *db, ci time.Duration) {
	c := &caretaker{
		Interval: ci,
	}
	store.caretaker = c
	go c.Run(store)
}







