package main

import (
	"net"
	"bufio"
	"fmt"
	"strings"
	"reflect"
	"sync"
)
var mutex=&sync.Mutex{}
var store=new(database)

var allcache=make(map[net.Conn]*transactionCache)
func main(){
	store.kv=make(map[string]string)
	
    l, err:=net.Listen("tcp", "0.0.0.0:8888")
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
	
	
	scannedCommand:=make(chan command)
	scanner:=bufio.NewScanner(conn)
	go scanInput(*scanner,scannedCommand)

	go handleCommand(&interactionIsRegular,scannedCommand,conn)
    
}

func Invoke(any interface{}, name string, args... interface{})[]reflect.Value {
	inputs := make([]reflect.Value, len(args))
	for i, _ := range args {
		inputs[i] = reflect.ValueOf(args[i])
	}
	return reflect.ValueOf(any).MethodByName(name).Call(inputs)
}


func handleRegularInteraction(cmd command,interactionIsRegular *bool,conn net.Conn){
	fmt.Printf("handling regular interaction: %s\n",cmd[0])
	if cmd[0]!="QUIT"&&cmd[0]!="BEGIN" {
		mutex.Lock()
		result:=Invoke(store,cmd[0],cmd[1],cmd[2])
		
		mutex.Unlock()
		fmt.Printf("result is %s\n",result)
	}
	if cmd[0]=="BEGIN" {
		*interactionIsRegular=false
		
		mutex.Lock()
		cache:=new(transactionCache)
		view:=new(database)
		kv:=make(map[string]string)
		
		for k,v:=range store.kv{
			kv[k]=v
		}
		view.kv=kv
		cache.view=view
		allcache[conn]=cache

		mutex.Unlock()
		
	} 
}

func handleTransactionalInteraction(cmd command, interactionIsRegular *bool,conn net.Conn){
	fmt.Printf("handling transactional interaction: %s\n",cmd[0])
	if cmd[0]!="QUIT"&&cmd[0]!="COMMIT" {
		result:=Invoke(allcache[conn],cmd[0],cmd[1],cmd[2])
		allcache[conn].commands=append(allcache[conn].commands,cmd)
		fmt.Printf("result is %s\n",result)
	}
	if cmd[0]=="COMMIT" {
		mutex.Lock()
		for _,cmd :=range allcache[conn].commands {
			Invoke(store,cmd[0],cmd[1],cmd[2])
			fmt.Printf("commiting %s\n",cmd[0])
		}
		mutex.Unlock()
		*interactionIsRegular=true
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

func handleCommand(interactionIsRegular *bool,scannedCommand chan command, conn net.Conn){
	for {
		select {
		case cmd:=<-scannedCommand:
			if cmd[0]=="QUIT" {conn.Close()}
			switch *interactionIsRegular{

	
				case true: handleRegularInteraction(cmd,interactionIsRegular,conn)
		
		
				case false: handleTransactionalInteraction(cmd,interactionIsRegular,conn)
				}
			
		default:
			{}

			
		}
	}
}