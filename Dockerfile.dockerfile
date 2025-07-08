FROM python:3.9-slim

WORKDIR /app
COPY . .

RUN pip install -r requirements.txt

# Run both servers when container starts
CMD streamlit run app.py --server.port=8501 --server.address=0.0.0.0 & \
    uvicorn ws_server:app --host 0.0.0.0 --port 8502