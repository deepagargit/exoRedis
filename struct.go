package main

import (
	"bufio"
	"net"
)

type client struct {
	id     int64
	conn   net.Conn
	reader *bufio.Reader
	store  *db
}


type command struct {
	Name string
	Args []string
}
