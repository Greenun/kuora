package web

import (
	mux "github.com/gorilla/mux"
	"net/http"
	"strconv"
)

const (
	PORT = 10000
)

func RunWebServer() {
	r := mux.NewRouter()
	// register handlers
	r.HandleFunc("/create", CreateHandler) // Create + Write
	r.HandleFunc("/read/{key}", ReadHandler)
	//r.HandleFunc("/write/{key}", WriteHandler)
	r.HandleFunc("/delete/{key}", DeleteHandler)

	http.Handle("/", r)
	http.ListenAndServe(":"+strconv.Itoa(PORT), nil)
}