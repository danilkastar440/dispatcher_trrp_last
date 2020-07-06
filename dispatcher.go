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

	"github.com/danilkastar440/test_project/pkg/models"
	"github.com/danilkastar440/test_project/pkg/pubsub"
)

type options struct {
	ProjectID     string `long:"projectID" env:"PROJECT_ID" required:"true" default:"trrv-univer"`
	DataTopicName string `long:"dataTopicName" env:"DATA_TOPIC_NAME" required:"true" default:"results"`
	DataSubName   string `long:"dataSubName" env:"DATA_SUB_NAME" required:"true" default:"results-sub"`
	Port          string `long:"port" env:"PORT" required:"true" default:"9992"`
}

type service struct {
	dataClient   *pubsub.Client
	commandsChan chan models.AgentDataRes
	upgrader     websocket.Upgrader
	connections  map[string]chan models.InternalRequest
	results      map[string]chan models.AgentDataRes
}

// handle results
func (s *service) HandleResults() {
	for res := range s.commandsChan {
		res1 := res
		s.results[res.RequestId] <- res1
		data, err := json.Marshal(&res1)
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
	ch := make(chan models.InternalRequest)
	//TODO: upgrade map
	s.connections[r.RemoteAddr] = ch
	for req := range ch {
		func() {
			cnt++
			defer req.Wg.Done()
			log.Info().Msgf("Got %d msg for %v", cnt, r.RemoteAddr)

			if err := c.WriteJSON(req.Req); err != nil {
				log.Error().Err(err).Msg("Failed to write json")
				return
			}

			var res models.AgentDataRes
			if err := c.ReadJSON(&res); err != nil {
				log.Error().Err(err).Msg("Failed to read json")
				return
			}
			s.commandsChan <- res
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
	req := models.InternalRequest{
		Wg: &wg,
		Req: models.AgentDataReq{
			RequestId: id,
			Def:       msg,
		},
	}

	log.Info().Msgf("Publish command: %v", msg)

	ch := make(chan models.AgentDataRes)
	s.results[id] = ch

	//TODO: make it safe
	for _, v := range s.connections {
		wg.Add(1)
		c := v
		c <- req
	}

	wg.Wait()
	var results []models.AgentDataRes
	for result := range ch {
		results = append(results, result)
	}
	data1, err := json.Marshal(&results)
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal results")
		w.WriteHeader(http.StatusUnprocessableEntity)
		return
	}

	//TODO: FIX IT!!!!
	w.WriteHeader(http.StatusOK)
	w.Write(data1)
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
	dataClient, err := pubsub.NewClient(opts.ProjectID, opts.DataTopicName, opts.DataSubName, 5*time.Second)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize pubsub dataClient")
	}

	s := service{
		dataClient:   dataClient,
		commandsChan: make(chan models.AgentDataRes),
		upgrader:     websocket.Upgrader{},
		connections:  make(map[string]chan models.InternalRequest),
		results:      make(map[string]chan models.AgentDataRes),
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
