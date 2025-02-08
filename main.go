package main

import (
	"fmt"
	"log"

	"github.com/raman20/goli"
)

func main() {
	user := map[string]string{
		"name": "Raman",
		"age":  "26",
	}

	db, err := goli.New("TEST")
	if err != nil {
		log.Fatal(err)
	}

	userBucket, err := db.GetCollection("user")
	if err != nil {
		log.Fatal(err)
	}

	id, err := userBucket.Create(user)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Print(id)

	db.Close()
}
