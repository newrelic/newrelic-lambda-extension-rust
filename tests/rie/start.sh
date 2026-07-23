#!/bin/sh
# Start mock Lambda runtime then the NR extension.

# Start mock Lambda runtime in background
python3 -u /mock_lambda_runtime.py &
RUNTIME_PID=$!

# Wait for the runtime API port to be listening
for i in $(seq 1 30); do
  if python3 -c "
import socket; s = socket.socket()
s.settimeout(0.5)
result = s.connect_ex(('127.0.0.1', 9001))
s.close()
exit(0 if result == 0 else 1)
" 2>/dev/null; then
    echo "[start] Runtime API ready on :9001"
    break
  fi
  sleep 0.2
done

# Launch the extension — it will register with 127.0.0.1:9001
export AWS_LAMBDA_RUNTIME_API=127.0.0.1:9001
echo "[start] Starting extension..."
/opt/extensions/newrelic-lambda-extension &
EXT_PID=$!

# Keep container alive; exit if extension dies
wait $EXT_PID
echo "[start] Extension exited — shutting down"
kill $RUNTIME_PID 2>/dev/null
