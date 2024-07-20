import os
from unittest.mock import patch

import pytest
import requests
from dotenv import load_dotenv

from extractors.strava_extractor import (
    api_error_handler,
    get_activity_ids,
    refresh_access_token,
    strava_extractor,
)
from utility.log_manager import setup_logging

logger = setup_logging()

load_dotenv(".env_dev")


def test_refresh_access_token(requests_mock):
    # Test scenario 1: Successful token refresh
    logger.debug("!!!!!! Testing refresh_access_token function: Successs Scenario")
    requests_mock.post(
        "https://www.strava.com/oauth/token",
        json={"access_token": "new_access_token", "refresh_token": "new_refresh_token"},
    )
    assert refresh_access_token(
        "https://www.strava.com/oauth/token",
        "client_id",
        "client_secret",
        "refresh_token",
    )

    # Check that .env_dev was updated
    assert os.getenv("STRAVA_ACCESS_TOKEN") == "new_access_token"
    assert os.getenv("STRAVA_REFRESH_TOKEN") == "new_refresh_token"

    # Test scenario 2: HTTP error occurs (status code 400)
    logger.debug("!!!!!! Testing refresh_access_token function: HTTP Error Scenario")
    requests_mock.post(
        "https://www.strava.com/oauth/token",
        status_code=400,
        json={"error": "Invalid request"},
    )

    with pytest.raises(SystemExit) as exc_info:
        refresh_access_token(
            "https://www.strava.com/oauth/token",
            "client_id",
            "client_secret",
            "refresh_token",
        )
    assert exc_info.value.code == 1

    # Test scenario 3: Other error occurs
    logger.debug("!!!!!! Testing refresh_access_token function: Timeout Scenario")
    requests_mock.post(
        "https://www.strava.com/oauth/token",
        exc=requests.exceptions.Timeout("Timeout occurred"),
    )

    with pytest.raises(SystemExit) as exc_info:
        refresh_access_token(
            "https://www.strava.com/oauth/token",
            "client_id",
            "client_secret",
            "refresh_token",
        )
    assert exc_info.value.code == 1


def test_api_error_handler():
    # Test scenario 1: Rate limit exceeded (status code 429)
    logger.debug("!!!!!! Testing api_error_handler function: Rate Limit Scenario")
    assert api_error_handler(429)

    logger.debug("!!!!!! Testing api_error_handler function: Unauthorised Scenario")
    # Test scenario 2: Unauthorised (status code 401)
    with patch(
        "extractors.strava_extractor.refresh_access_token",
        return_value="new_access_token",
    ):
        assert api_error_handler(401)

    # Test scenario 3: Not found (status code 404)
    logger.debug("!!!!!! Testing api_error_handler function: Not Found Scenario")
    assert not api_error_handler(404, activity_id="1234")

    # Test scenario 4: Other 4xx status code
    logger.debug("!!!!!! Testing api_error_handler function: Other Error Scenarios")
    with pytest.raises(SystemExit) as exc_info:
        api_error_handler(400)
    assert exc_info.value.code == 1


@pytest.fixture
def mock_headers():
    return {"Authorization": "Bearer test_token"}


def test_get_activity_ids(requests_mock, mock_headers):
    # Test scenario 1: Single page of results
    logger.debug("!!!!!! Testing get_activity_ids function: Single Page Scenario")
    requests_mock.get(
        "https://www.strava.com/api/v3/athlete/activities",
        json=[{"id": 1234}, {"id": 5678}],
    )

    result = get_activity_ids(mock_headers)
    assert result == {"1234", "5678"}
    assert requests_mock.call_count == 1

    # Test scenario 2: Multiple pages of results
    logger.debug("!!!!!! Testing get_activity_ids function: Multiple Page Scenario")
    requests_mock.get(
        "https://www.strava.com/api/v3/athlete/activities",
        [
            {"json": [{"id": i} for i in range(50)], "status_code": 200},
            {"json": [{"id": i} for i in range(50, 100)], "status_code": 200},
            {"json": [], "status_code": 200},
        ],
    )

    result = get_activity_ids(mock_headers)
    assert len(result) == 100
    assert all(str(i) in result for i in range(100))
    assert requests_mock.call_count == 4

    # Test scenario 3: Rate limit exceeded
    logger.debug("!!!!!! Testing get_activity_ids function: Rate Limit Scenario")
    requests_mock.get(
        "https://www.strava.com/api/v3/athlete/activities",
        [
            {"json": [{"id": i} for i in range(50)], "status_code": 200},
            {"status_code": 429},
            {"json": [{"id": i} for i in range(50, 75)], "status_code": 200},
            {"json": [], "status_code": 200},
        ],
    )

    with patch(
        "extractors.strava_extractor.api_error_handler", side_effect=api_error_handler
    ) as mock_error_handler:
        result = get_activity_ids(mock_headers)

    assert len(result) == 75
    assert all(str(i) in result for i in range(75))
    assert requests_mock.call_count == 7
    mock_error_handler.assert_called_once_with(429)

    # Test scenario 4: Unauthorised
    logger.debug("!!!!!! Testing get_activity_ids function: Unauthorised Scenario")
    requests_mock.get(
        "https://www.strava.com/api/v3/athlete/activities",
        [
            {"json": [{"id": i} for i in range(50)], "status_code": 200},
            {"status_code": 401},
            {"json": [{"id": i} for i in range(50, 75)], "status_code": 200},
            {"json": [], "status_code": 200},
        ],
    )

    with patch(
        "extractors.strava_extractor.api_error_handler", side_effect=api_error_handler
    ) as mock_error_handler:
        with patch("extractors.strava_extractor.refresh_access_token", return_value="new_token"):
            result = get_activity_ids(mock_headers)

    assert len(result) == 75
    assert all(str(i) in result for i in range(75))
    assert requests_mock.call_count == 10
    mock_error_handler.assert_called_once_with(401)

    # Test scenario 5: Not found
    logger.debug("!!!!!! Testing get_activity_ids function: Not Found Scenario")
    requests_mock.get("https://www.strava.com/api/v3/athlete/activities", status_code=500)
    with pytest.raises(SystemExit) as exc_info:
        get_activity_ids(mock_headers)

    assert exc_info.value.code == 1


def test_strava_extractor_comprehensive(requests_mock, mock_headers):
    # Mock the cache
    with patch("extractors.strava_extractor.cache_data.get_cached_ids", return_value=set()):
        # Mock the API calls
        requests_mock.get(
            "https://www.strava.com/api/v3/athlete/activities",
            [
                {"json": [{"id": i} for i in range(50)], "status_code": 200},
                {"status_code": 401},  # Simulate expired token
                {"json": [{"id": i} for i in range(50, 100)], "status_code": 200},
                {"json": [], "status_code": 200},
            ],
        )

        # Mock individual activity data responses
        for i in range(100):
            requests_mock.get(
                f"https://www.strava.com/api/v3/activities/{i}",
                json={"id": i, "name": f"Activity {i}", "distance": i * 1000},
            )

        # Mock the token refresh
        with patch(
            "extractors.strava_extractor.refresh_access_token", return_value="new_token"
        ) as mock_refresh:
            # Mock the data upload and local update
            with patch(
                "extractors.strava_extractor.upload_data_s3.post_data_to_s3"
            ) as mock_upload:
                with patch(
                    "extractors.strava_extractor.download_data_local.update_local_data"
                ) as mock_update:
                    # Mock caching of new IDs
                    with patch("extractors.strava_extractor.cache_data.cache_ids") as mock_cache:
                        # Run the extractor
                        strava_extractor()

        # Assertions
        assert requests_mock.call_count == 104  # 4 for activity IDs, 100 for individual activities
        mock_refresh.assert_called_once()
        mock_upload.assert_called_once()
        mock_update.assert_called_once()
        mock_cache.assert_called_once()

        # Check that the correct data was processed
        args, _ = mock_upload.call_args
        df = args[0]
        assert len(df) == 100
        assert all(df.columns == ["id", "name", "distance"])


def test_strava_extractor(requests_mock):
    # Mock the API calls
    requests_mock.get(
        "https://www.strava.com/api/v3/athlete/activities", json=[{"id": 1}, {"id": 2}]
    )
    requests_mock.get(
        "https://www.strava.com/api/v3/activities/1",
        json={"id": 1, "name": "Activity 1"},
    )
    requests_mock.get(
        "https://www.strava.com/api/v3/activities/2",
        json={"id": 2, "name": "Activity 2"},
    )

    # Mock the cache
    with patch(
        "extractors.strava_extractor.cache_data.get_cached_ids", return_value=set()
    ) as mock_get_cache:
        with patch("extractors.strava_extractor.cache_data.cache_ids") as mock_set_cache:
            # Mock the data storage
            with patch("extractors.strava_extractor.upload_data_s3.post_data_to_s3") as mock_s3:
                with patch(
                    "extractors.strava_extractor.download_data_local.update_local_data"
                ) as mock_local:
                    # Run the function
                    strava_extractor()

                    # Assert that the expected side effects occurred
                    mock_get_cache.assert_called_once()
                    mock_set_cache.assert_called_once_with("strava_activity_ids", {"1", "2"})
                    mock_s3.assert_called_once()
                    mock_local.assert_called_once()

                    # Check the data that was "uploaded" to S3
                    args, _ = mock_s3.call_args
                    df = args[0]
                    assert len(df) == 2
                    assert set(df["id"]) == {1, 2}
                    assert set(df["name"]) == {"Activity 1", "Activity 2"}

    # Assert that all expected API calls were made
    assert requests_mock.call_count == 3
