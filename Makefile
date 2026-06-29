.PHONY: help test compose-up compose-down e2e-test

# 預設行為：顯示說明
help:
	@echo "================ momo-gacha Makefile 指令說明 ================"
	@echo "make test         : 執行所有單元測試"
	@echo "make bench        : 執行效能基準測試 (Benchmark)"
	@echo "make compose-up   : 一鍵啟動所有 Docker Compose 容器與服務"
	@echo "make compose-down : 停止並清除所有 Docker Compose 容器與資料"
	@echo "make e2e-test     : 在 Docker 內運行高併發 E2E 整合測試 (含 QPS/延遲等性能指標)"
	@echo "=============================================================="

# 執行所有單元測試
test:
	go test -v ./...

# 執行效能基準測試
bench:
	go test -bench=. -benchmem ./internal/usecase/...

# 一鍵 Docker 啟動所有服務 (MySQL, Redis, Kafka, API, Worker)
compose-up:
	docker-compose up --build -d

# 停止並清除所有容器與資料卷
compose-down:
	docker-compose down -v

# 執行 E2E 併發壓力整合測試
e2e-test:
	docker-compose run --rm e2e-test
