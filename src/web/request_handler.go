package web

import (
	"client"
	"common"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
)

type RequestHandler struct {
	http.Handler
	client *client.Client
}

type WriteData struct {
	Data string
}

func (h *RequestHandler) CreateHandler(w http.ResponseWriter, r *http.Request) {
	var body WriteData
	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		logger.Errorf(err.Error())
	}
	logger.Infof("body - %v", body.Data)
	buffer := make([]byte, len(body.Data))
	key, cerr := h.client.Create(uint64(len(buffer)))
	if cerr != nil {
		logger.Errorf(cerr.Error())
	}
	// write data
	h.handleWrite(key, buffer)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(key))
	//h.handleWrite()
}

func (h *RequestHandler) ReadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	logger.Infof("%v", vars)
	key := vars["key"]
	length, _ := strconv.Atoi(vars["length"])
	buffer := make([]byte, length) // temp
	_, err := h.client.Read(common.HotKey(key), 0, buffer)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(buffer)
	}
	w.WriteHeader(http.StatusOK)
	w.Write(buffer)
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
		return
	}
	for i, key := range content {
		ret += fmt.Sprintf("%d: %s", i+1, key) + "\n"
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(ret))
}

func (h *RequestHandler) StatusHandler(w http.ResponseWriter, r *http.Request) {
	logger.Infof("Get Status")
	content, err := h.client.NodeStatus()
	ret := ""
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		ret = fmt.Sprintf("REQUEST ERROR - %s", err.Error())
		w.Write([]byte(ret))
		return
	}
	for address, nodeInfo := range content {
		ret += string(address) + " - "
		for key, b := range nodeInfo.Blocks {
			if b {
				ret += "Block-" + strconv.Itoa(int(key)) + " | "
			}
		}
		ret += "\n"
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(ret))
}

func (h *RequestHandler) handleWrite(key common.HotKey, data []byte) {
	err := h.client.Write(key, 0, data)
	if err != nil {
		logger.Error(err.Error())
	}

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