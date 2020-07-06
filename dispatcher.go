package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"

	"github.com/go-chi/chi"
	"github.com/gorilla/websocket"
	"github.com/jessevdk/go-flags"
	"github.com/rs/zerolog/log"

	"github.com/danilkastar440/TRRP_LAST/pkg/models"
	"github.com/danilkastar440/TRRP_LAST/pkg/pubsub"
)

var (
	connMu sync.Mutex
)

type internalRequest struct {
	ResCh chan models.AgentDataRes
	Req   models.AgentDataReq
}

type options struct {
	ProjectID     string `long:"projectID" env:"PROJECT_ID" required:"true" default:"trrv-univer"`
	DataTopicName string `long:"dataTopicName" env:"DATA_TOPIC_NAME" required:"true" default:"results"`
	DataSubName   string `long:"dataSubName" env:"DATA_SUB_NAME" required:"true" default:"results-sub"`
	Port          string `long:"port" env:"PORT" required:"true" default:"8080"`
}

type service struct {
	dataClient  *pubsub.Client
	resChan     chan models.AgentDataRes
	upgrader    websocket.Upgrader
	connections map[string]chan internalRequest
}

// handle results
func (s *service) HandleResults() {
	for res := range s.resChan {
		// Black magic
		res := res
		log.Info().Msgf("Sent to pubsub from HandleResults: %#v", res)
		data, err := json.Marshal(res)
		if err != nil {
			log.Error().Err(err).Msg("Failed to marshal result")
			continue
		}
		// Publish raw bytes to pubsub
		if err := s.dataClient.Publish(context.Background(), data); err != nil {
			log.Error().Err(err).Msg("Failed to publish msg")
			continue
		}
	}
}

// Subscribe to changes handler
func (s *service) Subscribe(w http.ResponseWriter, r *http.Request) {
	log.Info().Msg("Subscribe")
	c, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Error().Err(err).Msg("Failed to upgrade conn")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer c.Close()

	cnt := 0
	ch := make(chan internalRequest)
	connMu.Lock()
	s.connections[r.RemoteAddr] = ch
	connMu.Unlock()
	for req := range ch {
		func() {
			cnt++
			log.Info().Msgf("Got %d msg for %v", cnt, r.RemoteAddr)

			if err := c.WriteJSON(req.Req); err != nil {
				log.Error().Err(err).Msg("Failed to write json")
				close(req.ResCh)
				return
			}

			var res models.AgentDataRes
			if err := c.ReadJSON(&res); err != nil {
				log.Error().Err(err).Msg("Failed to read json")
				close(req.ResCh)
				return
			}
			req.ResCh <- res
		}()
	}
}

// Publish commands handler
func (s *service) PublishCommand(w http.ResponseWriter, r *http.Request) {
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Error().Err(err).Msg("Failed to read msg")
		w.WriteHeader(http.StatusUnprocessableEntity)
		return
	}

	var msg models.SourceDefinition
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Error().Err(err).Msg("Failed to unmarshal msg")
		w.WriteHeader(http.StatusUnprocessableEntity)
		return
	}

	wg := sync.WaitGroup{}
	id := uuid.NewV4().String()
	req := internalRequest{
		Req: models.AgentDataReq{
			RequestId: id,
			Def:       msg,
		},
	}

	log.Info().Msgf("Publish command: %v", msg)

	connMu.Lock()
	var results []models.AgentDataRes
	mu := sync.Mutex{}

	for k, v := range s.connections {
		v := v
		k := k
		go func() {
			wg.Add(1)
			defer wg.Done()
			resCh := make(chan models.AgentDataRes)
			req.ResCh = resCh
			v <- req
			log.Info().Msg("Sent")
			resp, ok := <-req.ResCh
			log.Info().Msgf("Get: %#v", resp)
			if !ok {
				log.Error().Msgf("Failed to do req for: %v", k)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			results = append(results, resp)
		}()
	}
	connMu.Unlock()

	wg.Wait()

	wg.Add(2)
	go func() {
		defer log.Info().Msg("wg1 DONE")
		for _, v := range results {
			s.resChan <- v
			log.Info().Msgf("Sent to pubsub chanel: %#v", v)
		}
		wg.Done()
	}()

	go func() {
		defer log.Info().Msg("wg2 DONE")
		defer wg.Done()
		data1, err := json.Marshal(results)
		if err != nil {
			log.Error().Err(err).Msg("Failed to marshal results")
			w.WriteHeader(http.StatusUnprocessableEntity)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(data1)
	}()

	wg.Wait()
	log.Info().Msgf("results: %#v", results)
}

// Check server handler
func (s *service) Check(w http.ResponseWriter, r *http.Request) {
	log.Info().Msg("Health check")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func main() {
	var opts options
	if _, err := flags.Parse(&opts); err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize env")
	}

	// Initialize pub sub dataClient
	dataClient, err := pubsub.NewClient(opts.ProjectID, opts.DataTopicName, opts.DataSubName, 60*time.Second)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize pubsub dataClient")
	}

	s := service{
		dataClient:  dataClient,
		resChan:     make(chan models.AgentDataRes),
		upgrader:    websocket.Upgrader{},
		connections: make(map[string]chan internalRequest),
	}
	go func() {
		s.HandleResults()
	}()

	// Initialize server
	r := chi.NewRouter()
	r.Post("/command", s.PublishCommand)
	r.HandleFunc("/subscribe", s.Subscribe)
	r.Get("/health", s.Check)

	srv := http.Server{
		Addr:    fmt.Sprintf(":%v", opts.Port),
		Handler: r,
	}

	log.Info().Msg("Start to serve")
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal().Err(err).Msg("Failed to listen and serve")
	}
}
