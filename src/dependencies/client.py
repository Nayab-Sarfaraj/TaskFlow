from fastapi import Header, HTTPException, status

clients = [
    {"client_id": "client_001", "api_key": "test_key_abc123"},
    {"client_id": "client_002", "api_key": "test_key_def456"},
    {"client_id": "client_003", "api_key": "test_key_ghi789"},
    {"client_id": "client_004", "api_key": "test_key_jkl012"},
    {"client_id": "client_005", "api_key": "test_key_mno345"},
]


 # client id assingment for now just use a random client id 
    # TODO : replace with actual client and api key implementation in later stage

def authenticate_client(x_api_key: str = Header(...)):
    client = next(
        (c for c in clients if c["api_key"] == x_api_key),
        None
    )

    if client is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key"
        )

    return client