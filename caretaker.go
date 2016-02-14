/* 
	Copyright 2016 Deepak Agarwal
	Author : Deepak Agarwal
*/

package main

import (
	"fmt"
	"time"
)


type caretaker struct {
	Interval time.Duration
	stop     chan bool
}


func runCaretaker(store *db, ci time.Duration) {
	c := &caretaker{
		Interval: ci,
	}
	store.caretaker = c
	go c.Run(store)
}


func stopCaretaker(store *db) {
	store.caretaker.stop <- true
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

func display(key string, value interface{}) {
	entry := value.(*mapData)
	fmt.Println("Evicted - key : ", key, "  val : ", entry.val, "  Expiration : ", entry.Expiration, "  Now: ", time.Now().UnixNano())
}


