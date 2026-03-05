package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os/exec"

	"github.com/rabbit-backend/gotiler/db"
)

func main() {
	DB := db.ConnectPG()

	cmd := exec.Command(
		"tippecanoe", 
		"-zg", 
		"-o", "tmp/data.pmtiles", 
		"--drop-densest-as-needed", 
		"-l", "data",
	)

	stdin, _ := cmd.StdinPipe()
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()

	cmd.Start()

	go func () {
		scanner := bufio.NewScanner(stdout)
		scanner.Split(bufio.ScanBytes)

		for scanner.Scan() {
			fmt.Print(string(scanner.Bytes()))
		}
	} ()

	go func () {
		scanner := bufio.NewScanner(stderr)
		scanner.Split(bufio.ScanBytes)

		for scanner.Scan() {
			fmt.Print(string(scanner.Bytes()))
		}
	} ()

	rows, err := DB.QueryContext(context.Background(), db.QUERY)
	if err != nil {
		log.Fatalln(err)
	}

	for rows.Next() {
		var data string

		rows.Scan(&data)
		stdin.Write([]byte(data))
	}

	stdin.Close()
	cmd.Wait()
}