
/* 
	Copyright 2016 Deepak Agarwal
	Author : Deepak Agarwal
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
)


const (
	// Port no for Server to listen
	addr string = ":15000"

	// File name to store the DB on disk
	dbFile string = "exoRedisDBFile.gob"

	// Time interval to run the cleanup of expired map entry
	timeInterval = time.Duration(30 * time.Second)

	// For use with functions that take an expiration time.
	NoExpiration time.Duration = -1
)


func main() {
	log.Printf("Server started\n")
	
	// Start the Server to listen at port number addr
	listener, err := net.Listen("tcp", addr)
	
	if err != nil {
		log.Printf("Error: listen(): %s", err)
		os.Exit(1)
	}
	
	// Create the db instance
	store := &db{
		mapEntry: make(map[string]*mapData),
		mapDBLock: &sync.RWMutex{},
		onEvicted: display,
		setmapEntry: make(map[string]*setmapData),
		setmapDBLock: &sync.RWMutex{},
	}

	// Run the Caretaker to periodicly clean the expired map entry
	runCaretaker(store, timeInterval)

	// Stop the Caretaker with the Server exit
	defer stopCaretaker(store)
	
	// Handle initialization of db with db input file from user
	if len(os.Args) > 1 {

		dbFileLoad := os.Args[1]
		ok := store.Load(dbFileLoad)
		if ok == false {
			log.Printf("Load of db file %s failed", dbFileLoad)
		}
	}
	
	// Logic to handle the sever shutdown (ie, Ctrl-C or SIGINT)
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	var sigCheck bool = false

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	
	// Anonymous go routine to handle sever shutdown and save db to disk
	go func(store *db, file string) {

		sig := <-sigs
		sigCheck = true

		fmt.Println("Server captured", sig, "signal")
		listener.Close()

		store.Save(file)
		done <- true	
	} (store, dbFile)
	
	// Server listening for incoming connections
	log.Printf("Accepting connections at: %s", addr)

	var id int64
	for {
		conn, err := listener.Accept()

		if sigCheck == true {
			break
		}

		if err != nil {
			log.Printf("Error: Accept(): %s", err)
			continue
		}

		id++
		client := &client{id: id, conn: conn, store: store}
		go client.serve()
	}
	
	// If server shutdown signal, wait for shutdown handler to finish
	if sigCheck == true {
		fmt.Println("Server awaiting shutdown handler ...")
		<-done
	}

        fmt.Println("Server Exiting")	
}







