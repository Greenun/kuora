package web

import (
	"client"
	"common"
	"github.com/Sirupsen/logrus"
	mux "github.com/gorilla/mux"
	"net/http"
	"strconv"
)

const (
	PORT = 10000
	STATIC_DIR = "src/web/static"
	MASTER = "127.0.0.1:40000"
)

var logger = logrus.New()

// temp in memory key store ( for test )
var keyStore map[common.HotKey][]byte

func RunWebServer() {
	keyStore = make(map[common.HotKey][]byte)
	rh := RequestHandler{client:client.NewClient(MASTER)}
	r := mux.NewRouter()
	// register handlers
	r.HandleFunc("/", ServeHttp)
	r.HandleFunc("/create", rh.CreateHandler) // Create + Write
	r.HandleFunc("/read/{key}", rh.ReadHandler)
	r.HandleFunc("/delete/{key}", rh.DeleteHandler)
	r.HandleFunc("/list", rh.ListHandler)
	r.HandleFunc("/status", rh.StatusHandler)

	http.Handle("/", r)
	http.ListenAndServe(":"+strconv.Itoa(PORT), nil)
}
