package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sync"
)

type ReplicaStatus struct {
	ActiveRequests int `json:"active_requests"`
}

type OllamaRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	Stream bool   `json:"stream"`
}

type OllamaResponse struct {
	Response string `json:"response"`
}

var (
	mu             sync.Mutex
	activeRequests int
)

func main() {
	port := flag.String("port", "9001", "port to listen on")
	model := flag.String("model", "llama3.2:1b", "ollama model name")
	flag.Parse()

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		status := ReplicaStatus{ActiveRequests: activeRequests}
		mu.Unlock()
		json.NewEncoder(w).Encode(status)
	})

	http.HandleFunc("/generate", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var reqBody struct {
			Prompt string `json:"prompt"`
		}
		json.Unmarshal(body, &reqBody)

		mu.Lock()
		activeRequests++
		mu.Unlock()
		defer func() {
			mu.Lock()
			activeRequests--
			mu.Unlock()
		}()

		ollamaReq := OllamaRequest{Model: *model, Prompt: reqBody.Prompt, Stream: false}
		reqJSON, _ := json.Marshal(ollamaReq)

		resp, err := http.Post("http://localhost:11434/api/generate", "application/json", bytes.NewBuffer(reqJSON))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		var ollamaResp OllamaResponse
		json.NewDecoder(resp.Body).Decode(&ollamaResp)

		json.NewEncoder(w).Encode(map[string]string{"response": ollamaResp.Response})
	})

	fmt.Println("replica listening on port", *port)
	http.ListenAndServe(":"+*port, nil)
}
