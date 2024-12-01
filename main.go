package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

var (
	DSN = ""
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf(err.Error())
	}
	DSN = os.Getenv("DSN")
}

func main() {
	db, err := sql.Open("mysql", DSN)
	if err != nil {
		log.Fatalf(err.Error())
	}
	err = db.Ping()
	if err != nil {
		log.Fatalf(err.Error())
	}

	handler, err := NewDBExplorer(db)
	if err != nil {
		log.Fatalf(err.Error())
	}

	fmt.Println("starting server at :8082")
	if err := http.ListenAndServe(":8082", handler); err != nil {
		log.Fatalf("error listenAndServer: %v", err)
	}
}
