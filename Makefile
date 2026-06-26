.PHONY: help test compose-up compose-down benchmark

# 預設行為：顯示說明
help:
	@echo "================ momo-gacha Makefile 指令說明 ================"
	@echo "make test         : 執行所有單元測試"
	@echo "make compose-up   : 一鍵啟動所有 Docker Compose 容器與服務"
	@echo "make compose-down : 停止並清除所有 Docker Compose 容器與資料"
	@echo "make benchmark    : 在本地執行 E2E 併發壓力與自癒測試腳本 (對應 Docker 服務)"
	@echo "=============================================================="

# 執行所有單元測試
test:
	go test -v ./...

# 一鍵 Docker 啟動所有服務 (MySQL, Redis, Kafka, API, Worker)
compose-up:
	docker-compose up --build -d

# 停止並清除所有容器與資料卷
compose-down:
	docker-compose down -v

# 執行基準壓力測試腳本
benchmark:
	docker-compose run --rm benchmark
