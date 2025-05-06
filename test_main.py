import json
import pytest
from unittest import mock
from main import app, parse_pubsub_message, validate_file_info
import base64
import json

@pytest.fixture
def client():
    with app.test_client() as client:
        yield client

def test_parse_pubsub_message_valid():
    original_data = {
        "bucket": "gcs-csv-reporter",
        "name": "raw-data/sample.csv"
    }
    encoded_data = base64.b64encode(json.dumps(original_data).encode("utf-8")).decode("utf-8")
    
    payload = {
        "message": {
            "data": encoded_data
        }
    }

    result = parse_pubsub_message(payload)
    assert result["bucket"] == "gcs-csv-reporter"
    assert result["name"] == "raw-data/sample.csv"

def test_parse_pubsub_message_invalid():
    with pytest.raises(ValueError):
        parse_pubsub_message({})  # No message

def test_validate_file_info_valid():
    validate_file_info("gcs-csv-reporter", "raw-data/sample.csv")  # No exception expected

def test_validate_file_info_invalid_bucket():
    with pytest.raises(ValueError):
        validate_file_info("wrong-bucket", "raw-data/sample.csv")

def test_validate_file_info_invalid_path():
    with pytest.raises(ValueError):
        validate_file_info("gcs-csv-reporter", "some-folder/file.txt")

@mock.patch("main.check_already_processed", return_value=False)
@mock.patch("main.process_csv", return_value={
    "row_count": 2, "column_count": 3
})
def test_main_success(mock_process_csv, mock_check_processed, client):
    data = {
        "message": {
            "data": base64.b64encode(json.dumps({
                "bucket": "gcs-csv-reporter",
                "name": "raw-data/test.csv"
            }).encode()).decode()
        }
    }
    response = client.post("/", json=data)
    assert response.status_code == 200
    json_data = response.get_json()
    assert json_data["status"] == "success"
    assert json_data["metrics_summary"]["row_count"] == 2

@mock.patch("main.check_already_processed", return_value=True)
def test_main_skipped(mock_check_processed, client):
    data = {
        "message": {
            "data": base64.b64encode(json.dumps({
                "bucket": "gcs-csv-reporter",
                "name": "raw-data/test.csv"
            }).encode()).decode()
        }
    }
    response = client.post("/", json=data)
    assert response.status_code == 200
    assert response.get_json()["status"] == "skipped"

def test_main_invalid_message(client):
    response = client.post("/", json={"not_message": {}})
    assert response.status_code == 400