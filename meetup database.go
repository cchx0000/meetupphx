package main

import (
	"net"
	"bufio"
	"fmt"
	"strings"
	"reflect"
)
var store=new(database)
func main(){
	store.kv=make(map[string]string)
    l, err:=net.Listen("tcp", "localhost:8888")
		if err!=nil {
			
		}
    for {
        conn,err:=l.Accept()
        if err!=nil {
			
		}
		go parseAndHandleRequest(conn)
		defer conn.Close()
    }
    
}

type transactionCache struct{
	commands []command
	view *database
} 

type database struct{
	kv map[string]string
}

type command [3]string



func (cache *transactionCache) GET(key string, _... string) string {
	return cache.view.kv[key]

}

func (cache *transactionCache) SET(key string, value string, _... string) {
	cache.view.kv[key]=value
	return

}

func (cache *transactionCache) DEL(key string, _... string) {
	cache.view.kv[key]=""
	return

}

func (db *database) GET(key string, _... string) string {
	return db.kv[key]

}


func (db *database) SET(key string, value string, _... string) {
	db.kv[key]=value
	return

}

func (db *database) DEL(key string, _... string) {
	db.kv[key]=""
	return

}
func parseAndHandleRequest(conn net.Conn){
	//conn.SetReadTimeout(5e9)
	var interactionIsRegular=true
	cache:=new(transactionCache)
	scannedCommand:=make(chan command)
	scanner:=bufio.NewScanner(conn)
	go scanInput(*scanner,scannedCommand)

	go handleCommand(&interactionIsRegular,scannedCommand,conn,cache)
    
}

func Invoke(any interface{}, name string, args... interface{})[]reflect.Value {
	inputs := make([]reflect.Value, len(args))
	for i, _ := range args {
		inputs[i] = reflect.ValueOf(args[i])
	}
	return reflect.ValueOf(any).MethodByName(name).Call(inputs)
}


func handleRegularInteraction(cmd command,interactionIsRegular *bool,cache *transactionCache){
	fmt.Printf("handling regular interaction: %s\n",cmd[0])
	if cmd[0]!="QUIT"&&cmd[0]!="BEGIN" {
		result:=Invoke(store,cmd[0],cmd[1],cmd[2])
		
		fmt.Printf("result is %s\n",result)
	}
	if cmd[0]=="BEGIN" {
		*interactionIsRegular=false
		
		view:=*store
		cache.view=&view
	} 
}

func handleTransactionalInteraction(cmd command, interactionIsRegular *bool,cache *transactionCache){
	fmt.Printf("handling transactional interaction: %s\n",cmd[0])
	if cmd[0]!="QUIT"&&cmd[0]!="COMMIT" {
		result:=Invoke(cache,cmd[0],cmd[1],cmd[2])
	
		fmt.Printf("result is %s\n",result)
	}
	if cmd[0]=="COMMIT" {
		*interactionIsRegular=true
		for _,cmd :=range cache.commands {
			Invoke(store,cmd[0],cmd[1],cmd[2])
		}
	} 

}

func scanInput(scanner bufio.Scanner,scannedCommand chan<-command){
	for scanner.Scan(){
		var cmd command
		
		for i,s :=range strings.Split(scanner.Text()," ") {
			cmd[i]=s
		}
		
		scannedCommand<-cmd
	}
}

func handleCommand(interactionIsRegular *bool,scannedCommand chan command, conn net.Conn, cache *transactionCache){
	for {
		select {
		case cmd:=<-scannedCommand:
			if cmd[0]=="QUIT" {conn.Close()}
			switch *interactionIsRegular{

	
				case true: handleRegularInteraction(cmd,interactionIsRegular,cache)
		
		
				case false: handleTransactionalInteraction(cmd,interactionIsRegular,cache)
				}
			
		default:
			{}

			
		}
	}
}