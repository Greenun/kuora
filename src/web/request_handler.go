package web

import (
	"client"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
)

type RequestHandler struct {
	http.Handler
	client *client.Client
}

func (h *RequestHandler) CreateHandler(w http.ResponseWriter, r *http.Request) {

}

func (h *RequestHandler) ReadHandler(w http.ResponseWriter, r *http.Request) {
	//vars := mux.Vars(r)
	w.WriteHeader(http.StatusOK)

}
func (h *RequestHandler) DeleteHandler(w http.ResponseWriter, r *http.Request) {

}

func (h *RequestHandler) ListHandler(w http.ResponseWriter, r *http.Request) {
	logger.Infof("List Key Operation")
	content, err := h.client.ListKeys()
	ret := ""
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		ret = fmt.Sprintf("REQUEST ERROR - %s", err.Error())
		w.Write([]byte(ret))
	}
	for i, key := range content {
		ret += fmt.Sprintf("%d: %s", i+1, key) + "\n"
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(ret))
}

func (h *RequestHandler) StatusHandler(w http.ResponseWriter, r *http.Request) {

}

func (h *RequestHandler) handleWrite(length int64, data []byte) {

}

func ServeHttp(w http.ResponseWriter, r *http.Request) {
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