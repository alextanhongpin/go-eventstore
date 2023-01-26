start:
	@go run main.go


up:
	@docker-compose up -d


down:
	@docker-compose down


web:
	@open http://localhost:2113
