package db

import (
	"database/sql"
	"log"
	"os"

	_ "github.com/lib/pq"
)

func ConnectPG() *sql.DB { 
	db, err := sql.Open("postgres", os.Getenv("PG_URL"))
	if err != nil {
		log.Fatalln(err)
	}

	return db
}