# Command to run a mock server
docker rm -f ecg-mock-buffer-server 2>$null

docker run -d `
  --name ecg-mock-buffer-server `
  -p 18080:8080 `
  -e HTTP_HOST=0.0.0.0 `
  -e HTTP_PORT=8080 `
  -e SOURCE_ID=mock-external-buffer-server-001 `
  -e SESSION_ID=longpoll-session-001 `
  -e SAMPLING_RATE=500 `
  -e LEAD_ID=II `
  -e CHUNK_DURATION_SECONDS=1 `
  -e GENERATION_INTERVAL_SECONDS=1.0 `
  -e BUFFER_CHUNKS=100 `
  -e PRELOAD_CHUNKS=30 `
  -e LOG_LEVEL=INFO `
  ecg/mock-external-buffer-server:v1