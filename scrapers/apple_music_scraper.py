import time
import jwt
import asyncio
import logging
from typing import Any, AsyncGenerator, Dict, Optional, List

import aiohttp
from aiohttp import ClientResponse, ClientTimeout


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)


class AsyncAppleMusicScraper:
    API_URL: str = "https://api.music.apple.com/v1"
    RATE_LIMIT_HEADERS: dict = {
        "limit":       "X-Rate-Limit-Limit",
        "remaining":   "X-Rate-Limit-Remaining",
        "reset":       "X-Rate-Limit-Reset",
        "retry_after": "Retry-After",
    }

    def __init__(
        self,
        logger: logging.Logger,
        key_file: str,
        team_id: str,
        key_id: str,
        storefront: str = "us",
        max_concurrent: int = 20,
        max_retries: int = 5,
        token_ttl: int = 60 * 60 * 24 * 30 * 6,  # six months in seconds
    ):
        self.logger = logger
        self.key_file = key_file
        self.team_id = team_id
        self.key_id = key_id
        self.storefront = storefront

        self._token: Optional[str] = None
        self._token_expiry: float = 0
        self.token_ttl = token_ttl

        self._sem = asyncio.Semaphore(max_concurrent)
        self._pause_event = asyncio.Event()
        self._pause_event.set()

        self._session: Optional[aiohttp.ClientSession] = None
        self.max_retries = max_retries

    def _generate_jwt(self) -> str:
        now = int(time.time())
        # header with key ID
        headers = {"alg": "ES256", "kid": self.key_id}
        payload = {
            "iss": self.team_id,
            "iat": now,
            "exp": now + self.token_ttl,
        }
        with open(self.key_file, 'r') as f:
            private_key = f.read()
        token = jwt.encode(payload, private_key, algorithm="ES256", headers=headers)
        self._token = token
        self._token_expiry = now + self.token_ttl
        return token

    def _ensure_token(self) -> None:
        """
        Lazily generate or refresh the JWT if it's near expiration.
        """
        now = time.time()
        # regenerate if expired or within 1 minute of expiry
        if self._token is None or now >= self._token_expiry - 60:
            self.logger.info("Generating new Apple Music JWT")
            self._generate_jwt()

    async def _ensure_session(self) -> None:
        if not self._session:
            timeout = ClientTimeout(total=60)
            self._ensure_token()
            headers = {
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json"
            }
            self._session = aiohttp.ClientSession(timeout=timeout, headers=headers)

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        await self._ensure_session()
        # rebuild Authorization in case token refreshed
        assert self._session is not None
        self._ensure_token()
        self._session.headers.update({"Authorization": f"Bearer {self._token}"})

        url = f"{self.API_URL}/catalog/{self.storefront}/{endpoint}"
        backoff = 1.0

        for attempt in range(1, self.max_retries + 1):
            await self._pause_event.wait()

            async with self._sem:
                resp: ClientResponse = await self._session.request(method, url, params=params)
                self.logger.debug(f"[{attempt}] {method} {url} → {resp.status}")

                # Parse rate-limit headers for logging and pre-emptive pause
                limit       = resp.headers.get(self.RATE_LIMIT_HEADERS["limit"])
                remaining   = resp.headers.get(self.RATE_LIMIT_HEADERS["remaining"])
                reset       = resp.headers.get(self.RATE_LIMIT_HEADERS["reset"])
                retry_after = resp.headers.get(self.RATE_LIMIT_HEADERS["retry_after"])
                if limit or remaining or reset or retry_after:
                    self.logger.debug(f"Rate-Limit: limit={limit}, remaining={remaining}, reset={reset}, retry_after={retry_after}")

                # 200 OK
                if resp.status == 200:
                    # if we’ve just consumed the last request, honour reset
                    if remaining == "0" and reset:
                        # reset is a UNIX timestamp in seconds
                        reset_ts = int(reset)
                        now = int(asyncio.get_event_loop().time())
                        delay = max(0, reset_ts - now)
                        self.logger.warning(f"Rate limit exhausted; pausing for {delay}s until reset")
                        self._pause_event.clear()
                        asyncio.get_event_loop().call_later(delay, self._pause_event.set)
                    return await resp.json()

                # 429 Too Many Requests
                if resp.status == 429:
                    retry_after = (
                        int(retry_after) if retry_after and retry_after.isdigit()
                        else (int(reset) - int(asyncio.get_event_loop().time()) if reset else 1)
                    )
                    self.logger.warning(f"429 rate-limit; pausing all requests for {retry_after}s")
                    self._pause_event.clear()
                    asyncio.get_event_loop().call_later(retry_after, self._pause_event.set)
                    await self._pause_event.wait()
                    self.logger.info("Resuming after rate-limit pause")
                    continue

                # 5xx Server error
                if 500 <= resp.status < 600:
                    self.logger.warning(f"{resp.status} server error; backing off {backoff}s")
                    await asyncio.sleep(backoff)
                    backoff *= 2
                    continue

                # 4xx Client error - fatal
                text = await resp.text()
                self.logger.error(f"{resp.status} client error on {url}: {text}")
                resp.raise_for_status()

        raise RuntimeError(f"Failed {method} {url} after {self.max_retries} attempts")

    async def get_artist(self, artist_id: str) -> Dict[str, Any]:
        return await self._request("GET", f"artists/{artist_id}")

    async def get_artist_albums(
        self,
        artist_id: str,
        limit: int = 50,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        offset = 0
        while True:
            page = await self._request(
                "GET",
                f"artists/{artist_id}/albums",
                params={"limit": limit, "offset": offset},
            )
            yield page
            items = page.get("data", [])
            if len(items) < limit:
                break
            offset += limit

    async def get_album_tracks(
        self,
        album_id: str,
        limit: int = 50,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        offset = 0
        while True:
            page = await self._request(
                "GET",
                f"albums/{album_id}/tracks",
                params={"limit": limit, "offset": offset},
            )
            yield page
            items = page.get("data", [])
            if len(items) < limit:
                break
            offset += limit

    async def close(self) -> None:
        if self._session:
            await self._session.close()


async def fetch_all_artist_albums(
    scraper: AsyncAppleMusicScraper,
    artist_id: str
) -> List[Dict]:
    albums: List[Dict] = []
    async for page in scraper.get_artist_albums(artist_id):
        albums.extend(page.get("data", []))
    return albums


async def fetch_all_album_tracks(
    scraper: AsyncAppleMusicScraper,
    album_id: str
) -> List[Dict]:
    tracks: List[Dict] = []
    async for page in scraper.get_album_tracks(album_id):
        tracks.extend(page.get("data", []))
    return tracks


async def main():
    import os
    from dotenv import load_dotenv

    load_dotenv()

    logger = logging.getLogger(f"applemusic_scraper_{os.getpid()}")
    logger.setLevel(logging.DEBUG)

    scraper = AsyncAppleMusicScraper(
        logger=logger,
        key_file=os.getenv("APPLE_PRIVATE_KEY_PATH"),
        team_id=os.getenv("APPLE_TEAM_ID"),
        key_id=os.getenv("APPLE_KEY_ID"),
        storefront="us",
        max_concurrent=10,
        max_retries=3,
    )

    try:
        artist_ids = ["1531294549", "909253"]  # example Apple Music artist IDs
        artists = await asyncio.gather(
            *(scraper.get_artist(aid) for aid in artist_ids)
        )

        album_lists = await asyncio.gather(
            *(fetch_all_artist_albums(scraper, artist["data"][0]["id"]) for artist in artists)
        )
        all_albums = [alb for sub in album_lists for alb in sub]

        track_lists = await asyncio.gather(
            *(fetch_all_album_tracks(scraper, album["id"]) for album in all_albums)
        )
        all_tracks = [trk for sub in track_lists for trk in sub]

        print(f"Fetched {len(all_tracks)} tracks from {len(all_albums)} albums for {len(artist_ids)} artists")
    finally:
        await scraper.close()


if __name__ == "__main__":
    asyncio.run(main())
