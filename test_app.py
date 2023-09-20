import pytest
import requests

BASE_URL = "http://localhost:8000"


@pytest.fixture
def upload_file():
    files = {"file": ("data_2.csv", open("data_2.csv", "rb"))}

    response = requests.post(f"{BASE_URL}/upload/", files=files)
    assert response.status_code == 200


def test_upload_and_download(upload_file):
    version = 2

    response = requests.get(f"{BASE_URL}/download/?version={version}")
    assert response.status_code == 200

    data = response.json()
    assert len(data) > 0


if __name__ == "__main__":
    pytest.main()
