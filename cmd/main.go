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

	db, err := goli.New()
	if err != nil {
		log.Fatal(err)
	}

	id, err := db.Put("user", user)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Print(id)
}
