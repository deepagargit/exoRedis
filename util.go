
package main

import (
	"bufio"
	"fmt"
	"log"
	"io"
	//"os"
	"strconv"
	"strings"
	"time"
)


func (client *client) log(msg string, args ...interface{}) {
	prefix := fmt.Sprintf("Client #%d: ", client.id)
	log.Printf(prefix+msg, args...)
}

func (client *client) logError(msg string, args ...interface{}) {
	client.log("Error: "+msg, args...)
}

func (client *client) send(val string) {
	//fmt.Fprintf(client.conn, "$%d\r\n%s\r\n", len(val), val)
	fmt.Fprintf(client.conn, "\r%s\r\n", val)
}

func (client *client) sendError(err error) {
	client.logError(err.Error())
	client.sendLine("-ERR " + err.Error() + "\r\n")
}

func (client *client) sendLine(line string) {
	if _, err := io.WriteString(client.conn, line); err != nil {
		client.log("Error for client.sendLine(): %s", err)
	}
}

type protocolError string

func (e protocolError) Error() string {
	return string(e)
}


func (client *client) readCommand() (*command, error) {
	for {
		line, err := client.readLine()
		if err != nil {
			return nil, err
		}

		args := strings.Split(line, " ")

		return &command{Name: args[0], Args: args[1:]}, nil
	}
}


func (client *client) readLine() (string, error) {
	var line string
	for {
		partialLine, isPrefix, err := client.reader.ReadLine()
		if err != nil {
			return "", err
		}

		line += string(partialLine)
		if isPrefix {
			continue
		}

		return line, nil
	}
}



func (client *client) serve() {
	defer client.conn.Close()

	client.log("Accepted connection: %s", client.conn.LocalAddr())
	client.reader = bufio.NewReader(client.conn)

	for {
		cmd, err := client.readCommand()

		if err != nil {
			if err == io.EOF {
				client.log("Disconnected")
			} else if _, ok := err.(protocolError); ok {
				client.sendError(err)
			} else {
				client.logError("readCommand(): %s", err)
			}
			return
		}

		switch cmd.Name {
		case "GET":
			if len(cmd.Args) < 1 {
				client.sendError(fmt.Errorf("GET expects 1 argument"))
				continue
			}
			val, ok := client.store.Get(cmd.Args[0])

			if ok == false {
				client.send("nil\r\n")
			} else {
				client.send(val)
			}


	        case "GETBIT":
			if len(cmd.Args) < 2 {
				client.sendError(fmt.Errorf("GETBIT expects 2 argument"))
				continue
			}
			
			// string to int
			offset, err := strconv.Atoi(cmd.Args[1])

			if err != nil {
				client.sendError(fmt.Errorf("GETBIT expects unsigned offset"))
				continue
			}

			val := client.store.GetBit(cmd.Args[0], offset)

			client.send(val)
	

		case "SET":
			
			if len(cmd.Args) < 2 {
				client.sendError(fmt.Errorf("SET expects atleast 2 arguments"))
				continue
			}
			ok := false

			if len(cmd.Args) == 2 {
				ok = client.store.Set(cmd.Args[0], cmd.Args[1], NoExpiration)
			} else {
				if cmd.Args[2] == "NX" {
					ok = client.store.SetNX(cmd.Args[0], cmd.Args[1], NoExpiration)
				} else if cmd.Args[2] == "XX" {
					ok = client.store.SetXX(cmd.Args[0], cmd.Args[1], NoExpiration)
				} else if cmd.Args[2] == "EX" {
					if len(cmd.Args) == 4 {

						val, valErr := strconv.Atoi(cmd.Args[3])
						if valErr != nil {
							client.sendError(fmt.Errorf("SET EX expects time (seconds) in integer"))
							continue
						}
						fmt.Println("SET EX val : ", val)
						ok = client.store.Set(cmd.Args[0], cmd.Args[1], time.Duration(30 * val))
					} else {
						client.sendError(fmt.Errorf("SET EX expects 4 arguments"))
						continue
					}
				} else if cmd.Args[2] == "PX" {
					if len(cmd.Args) == 4 {
						val, valErr := strconv.Atoi(cmd.Args[3])
						if valErr != nil {
							client.sendError(fmt.Errorf("SET EX expects time (milliseconds) in integer"))
							continue
						}
						ok = client.store.Set(cmd.Args[0], cmd.Args[1], time.Duration(30 * val))
					} else {
						client.sendError(fmt.Errorf("SET PX expects 4 arguments"))
						continue
					}
				}

			}

			if ok == true {
				fmt.Fprintf(client.conn, "+OK\r\n")
			} else {
				fmt.Fprintf(client.conn, "+Error\r\n")
			}


		case "SETBIT":
			if len(cmd.Args) < 3 {
				client.sendError(fmt.Errorf("SETBIT expects 3 arguments"))
				//return
				continue
			}

			// string to int
			offset, err := strconv.Atoi(cmd.Args[1])
			

			if err != nil {
				client.sendError(fmt.Errorf("GETBIT expects unsigned offset"))
				continue
			}

			val, bitErr := strconv.Atoi(cmd.Args[2])
			if bitErr != nil {
				client.sendError(fmt.Errorf("GETBIT expects binary bit 0 or 1"))
				continue
			}

			var bitFlag byte = 0
			
			if val > 0 {
				 bitFlag = 1
			}


			client.store.SetBit(cmd.Args[0], offset, bitFlag, NoExpiration)
			fmt.Fprintf(client.conn, "+OK\r\n")

		
		case "ZADD":
			if len(cmd.Args) < 3 {
				client.sendError(fmt.Errorf("ZADD expects 3 arguments"))
				continue
			}

			if len(cmd.Args) % 2 == 0 {
				client.sendError(fmt.Errorf("ZADD expects pair of score-member in arguments"))
				continue
			}
			
			/* 
			    Taking map of member to score 
			    The member has to be unique over a key 
			    If user enters ZADD mykey 2 q 2 w 1 q
			    only 2 w and 1 q will be added
			    q a unique mwmber need to have latest score which is 1 in this case
			*/
			zaddMap := make(map[string]int)

			var i int = 1
			for i=1; (i+1)<len(cmd.Args); i=i+2 {
				// string to int
				score, err := strconv.Atoi(cmd.Args[i])

				if err != nil {
					client.sendError(fmt.Errorf("ZADD expects int score"))
					break
				}

				zaddMap[cmd.Args[i+1]] = score
			}

			if i < len(cmd.Args) {
				continue
			}

			ok := client.store.ZADD(cmd.Args[0], &zaddMap)

			if ok == true {
				fmt.Fprintf(client.conn, "+OK\r\n")
			} else {
				fmt.Fprintf(client.conn, "+Error\r\n")
			}

		case "ZCARD":
			if len(cmd.Args) < 1 {
				client.sendError(fmt.Errorf("ZCARD expects 1 argument"))
				continue
			}

			count, ok := client.store.ZCARD(cmd.Args[0])

			if ok == true {
				client.send(strconv.Itoa(count))
			} else {
				client.send(strconv.Itoa(0))
			}

		case "ZCOUNT":
			if len(cmd.Args) < 3 {
				client.sendError(fmt.Errorf("ZCOUNT expects 3 arguments"))
				continue
			}

			// string to int
			min, err := strconv.Atoi(cmd.Args[1])
			

			if err != nil {
				client.sendError(fmt.Errorf("ZCOUNT expects int min"))
				continue
			}

			// string to int
			max, err := strconv.Atoi(cmd.Args[2])
			

			if err != nil {
				client.sendError(fmt.Errorf("ZCOUNT expects int max"))
				continue
			}

			count, ok := client.store.ZCOUNT(cmd.Args[0], min, max)

			if ok == true {
				client.send(strconv.Itoa(count))
			} else {
				client.send(strconv.Itoa(0))
			}

		case "ZRANGE":
			if len(cmd.Args) < 3 {
				client.sendError(fmt.Errorf("ZRANGE expects minimum 3 arguments"))
				continue
			}

			// string to int
			start, err := strconv.Atoi(cmd.Args[1])
			

			if err != nil {
				client.sendError(fmt.Errorf("ZRANGE expects int start"))
				continue
			}

			// string to int
			stop, err := strconv.Atoi(cmd.Args[2])
			

			if err != nil {
				client.sendError(fmt.Errorf("ZRANGE expects int stop"))
				continue
			}


			retMap, ok := client.store.ZRANGE(cmd.Args[0], start, stop)

			if ok == true {
				if len(cmd.Args) == 4 && cmd.Args[3] == "WITHSCORES" {
					for key,value := range retMap {
						client.send(key)
						client.send(strconv.Itoa(value))
					}
				} else {
					for key,_ := range retMap {
						client.send(key)
					}
				}
			} else {
				fmt.Fprintf(client.conn, "+Error\r\n")
			}


		case "SAVE":
			ok := client.store.Save(dbFile)
			if ok == true {
				client.send("+OK\r\n") 
			} else {
				client.send("-Error DB save failed\r\n")
			}

		case "EXIT":
			client.conn.Close()
			return
		
		default:
			client.sendError(fmt.Errorf("unkonwn command: %s", cmd.Name))
		}
	}
}



