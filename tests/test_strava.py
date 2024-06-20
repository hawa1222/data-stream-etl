from extractors.strava import (
    get_activity_ids,
    refresh_access_token,
)


def test_refresh_access_token(requests_mock):
    # Mock the token refresh API response
    requests_mock.post(
        "https://www.strava.com/oauth/token",
        json={"access_token": "new_access_token", "refresh_token": "new_refresh_token"},
    )

    # Call the function and assert the expected behavior
    new_access_token = refresh_access_token(
        "https://www.strava.com/oauth/token",
        "client_id",
        "client_secret",
        "refresh_token",
    )
    assert new_access_token == "new_access_token"


def test_get_activity_ids(requests_mock):
    # Mock the API response for fetching activity IDs
    requests_mock.get(
        "https://www.strava.com/api/v3/athlete/activities",
        json=[{"id": 1234}, {"id": 5678}],
    )

    # Call the function and assert the expected behavior
    activity_ids = get_activity_ids({"Authorization": "Bearer access_token"})
    assert activity_ids == {"1234", "5678"}
