from unittest.mock import MagicMock, patch

import pandas as pd

from dags.scripts.extract import latest_fx


mock_api_response = {
    "result": "success",
    "conversion_rates": {
        "NOK": 11.28,
        "EUR": 1.0,
        "SEK": 10.65,
        "PLN": 4.22,
        "RON": 5.09,
        "DKK": 7.46,
        "CZK": 24.23,
    },
}

base_url = "https://v6.exchangerate-api.com/v6"


@patch("dags.scripts.extract.requests.get")
def test_latest_fx_returns_one_row(mock_get: MagicMock) -> None:
    mock_response = MagicMock()
    mock_response.json.return_value = mock_api_response
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    df = latest_fx(base_url)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1


@patch("dags.scripts.extract.requests.get")
def test_latest_fx_rates_positive(mock_get: MagicMock) -> None:
    mock_response = MagicMock()
    mock_response.json.return_value = mock_api_response
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    df = latest_fx(base_url)

    for currency in ["NOK", "EUR", "SEK", "PLN", "RON", "DKK", "CZK"]:
        assert df[currency].iloc[0] > 0
