package admin

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/premchandkpc/kafka-simulate-rule/go/internal/engine"
)

type Server struct {
	engine *engine.Engine
	mux    *http.ServeMux
}

func New(eng *engine.Engine) *Server {
	s := &Server{
		engine: eng,
		mux:    http.NewServeMux(),
	}
	s.mux.HandleFunc("POST /rules", s.deployRule)
	s.mux.HandleFunc("DELETE /rules/{id}", s.removeRule)
	s.mux.HandleFunc("GET /rules", s.listRules)
	s.mux.HandleFunc("GET /health", s.health)
	return s
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) deployRule(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID  string `json:"id"`
		DSL string `json:"dsl"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	log.Printf("deploy rule: id=%s", req.ID)
	if err := s.engine.Deploy(req.ID, req.DSL); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"id": req.ID})
}

func (s *Server) removeRule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.engine.Remove(id)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) listRules(w http.ResponseWriter, r *http.Request) {
	rules := s.engine.Rules()
	type ruleView struct {
		ID  string `json:"id"`
		DSL string `json:"dsl"`
	}
	view := make([]ruleView, len(rules))
	for i, ru := range rules {
		view[i] = ruleView{ID: ru.ID, DSL: ru.DSL}
	}
	json.NewEncoder(w).Encode(view)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
