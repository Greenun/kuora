package web

import (
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
)

const (
	CLIENT_PORT = 15000
	STATIC_DIR = "src/web/static"
)

func RunClient() {
	http.HandleFunc("/", serveHttp)
	http.ListenAndServe(":"+strconv.Itoa(CLIENT_PORT), nil)
}

func serveHttp(w http.ResponseWriter, r *http.Request) {
	var filename string
	if r.URL.Path == "/" {
		filename = "index.html"
	}
	current, err := os.Getwd()
	filepath := path.Join(current, STATIC_DIR, filename)
	content, err := ioutil.ReadFile(filepath)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(http.StatusText(http.StatusNotFound)))
		return
	}
	w.Header().Add("Content-Type", "text/html")
	w.Write(content)
}