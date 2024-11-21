import sys
sys.path.append('./pipeline')

import pytest
import requests
from unittest.mock import patch
from custom_func.func import get_data_from_server
from datetime import datetime
import json

# Successful response with valid data
def test_get_data_success(mocker):
    mock_data = {'id': 1, 'temperature': 20.00, 'humidity': 50.00}

    mock_get = mocker.patch('custom_func.func.requests.get')
    mock_get.return_value.status_code = 200
    mock_get.return_value.text = json.dumps(mock_data)

    result = get_data_from_server()

    assert result['id'] == 1
    assert result['temperature'] == 20.00
    assert result['humidity'] == 50.00
    assert 'timestamp' in result


# Successful response with valid data -> WebServer work, but issue with DTH22
def test_get_data_invalid_json(mocker):
    mock_get = mocker.patch('custom_func.func.requests.get')

    mock_get.return_value.status_code = 200
    mock_get.return_value.text = '{"id": 1, "temperature": nan, "humidity": nan}'

    result = get_data_from_server()

    assert result['id'] == -1
    assert result['error'] == 'Data not received'
    assert 'timestamp' in result


# Failed response -> No connection with WebServer
def test_get_data_connection_error(mocker):
    mock_get = mocker.patch('custom_func.func.requests.get')
    mock_get.side_effect = requests.exceptions.RequestException

    result = get_data_from_server()

    assert result['id'] == -1
    assert result['error'] == 'Cannot connect with webserver'
    assert 'timestamp' in result
