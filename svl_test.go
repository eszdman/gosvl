package gosvl

import (
	"github.com/dgraph-io/badger/v4"
	"log"
	"testing"
)

func TestAdd(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("/tmp/badger"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.DropAll()
	svl, _ := NewSVL().
		//SetEncodeElements(false).
		SetBadger(db).
		UseLog().
		UseGobEncDec().
		Build()

	type User struct {
		ID   uint `svl:"primary"`
		Name string
		Age  int
	}
	svl.SetIncrement(&User{}, nil)
	var user User
	user.Age = 21
	user.Name = "John"
	user2 := User{Age: 22, Name: "John"}
	err = svl.Add(&user)
	err = svl.Add(&user2)
	if err != nil {
		t.Error(err)
	}
	log.Printf("Added user: %+v", user)
	log.Printf("Added user: %+v", user2)
}

func TestGet(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("/tmp/badger"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	//db.DropAll()
	svl, _ := NewSVL().
		//SetEncodeElements(false).
		SetBadger(db).
		UseLog().
		UseGobEncDec().
		Build()

	type User struct {
		ID   uint `svl:"primary"`
		Name string
		Age  int
	}
	svl.SetIncrement(&User{}, nil)
	var user []User
	err = svl.Get(&user)
	if err != nil {
		t.Error(err)
	}
	log.Printf("Read users: %+v", user)
}
