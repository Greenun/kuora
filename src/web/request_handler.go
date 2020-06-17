package web

import (
	"net/http"
)

func CreateHandler(w http.ResponseWriter, r *http.Request) {

}

func ReadHandler(w http.ResponseWriter, r *http.Request) {
	//vars := mux.Vars(r)
	w.WriteHeader(http.StatusOK)

}

func DeleteHandler(w http.ResponseWriter, r *http.Request) {

}

func handleWrite(length int64, data []byte) {

}