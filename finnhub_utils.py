import finnhub

finnhub_key = "c52afc2ad3i9pcg02ro0"


def get_finnhub_client():
    finnhub_client = finnhub.Client(api_key=finnhub_key)
    return finnhub_client
