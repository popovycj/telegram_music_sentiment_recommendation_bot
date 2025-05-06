import base64
import logging
import asyncio
from typing import Any, AsyncGenerator, Dict, Optional, List

import aiohttp
from aiohttp import ClientResponse, ClientTimeout


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

class AsyncSpotifyScraper:
    API_URL   = "https://api.spotify.com/v1"
    TOKEN_URL = "https://accounts.spotify.com/api/token"

    def __init__(
        self,
        logger: logging.Logger,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        max_concurrent: int = 20,
        max_retries: int = 5,
    ):
        self.logger = logger

        self.client_id     = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token

        self.access_token: Optional[str] = None
        self._token_lock = asyncio.Lock()

        self._sem = asyncio.Semaphore(max_concurrent)
        self._global_pause_until = 0.0

        self._session: Optional[aiohttp.ClientSession] = None
        self.max_retries = max_retries


    async def _ensure_session(self) -> None:
        if not self._session:
            timeout = ClientTimeout(total=60)
            headers = {"Content-Type": "application/json"}
            self._session = aiohttp.ClientSession(timeout=timeout, headers=headers)
            await self._refresh_token()

    async def _refresh_token(self) -> None:
        self.logger.info("Refreshing Spotify access token")
        # Prevent parallel refreshes
        async with self._token_lock:
            # If another coroutine refreshed while we waited, skip
            if self.access_token:
                return

            auth = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()
            data = {
                "grant_type":    "refresh_token",
                "refresh_token": self.refresh_token,
            }
            headers = {
                "Authorization":  f"Basic {auth}",
                "Content-Type":   "application/x-www-form-urlencoded",
            }
            async with aiohttp.ClientSession() as s:
                async with s.post(self.TOKEN_URL, data=data, headers=headers) as resp:
                    resp.raise_for_status()
                    payload = await resp.json()
                    self.access_token = payload["access_token"]

            # inject into our main session
            assert self._session is not None
            self._session.headers.update({"Authorization": f"Bearer {self.access_token}"})

    async def _maybe_wait_pause(self) -> None:
        # if global pause set, wait until it expires
        now = asyncio.get_event_loop().time()
        if now < self._global_pause_until:
            await asyncio.sleep(self._global_pause_until - now)

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        await self._ensure_session()
        url = f"{self.API_URL}/{endpoint}"
        self.logger.debug(f"Request {method} {url} params={params} attempt={1}")
        backoff = 1.0

        for attempt in range(1, self.max_retries + 1):
            await self._maybe_wait_pause()

            async with self._sem:
                resp: ClientResponse
                resp = await self._session.request(method, url, params=params)
                self.logger.debug(f"Received status {resp.status} for {method} {url}")

                # 200 OK
                if resp.status == 200:
                    return await resp.json()

                # 401: token may be expired
                if resp.status == 401 and attempt == 1:
                    self.logger.info("Access token expired, refreshing and retrying")
                    # clear token and retry once
                    self.access_token = None
                    await self._refresh_token()
                    continue

                # 429: rate limited
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", "1"))
                    self.logger.warning(f"Rate limited on {url}, retrying after {retry_after}s")
                    self._global_pause_until = asyncio.get_event_loop().time() + retry_after
                    await asyncio.sleep(retry_after)
                    continue

                # 5xx: server error
                if 500 <= resp.status < 600:
                    self.logger.warning(f"Server error {resp.status} on {url}, backing off for {backoff}s")
                    await asyncio.sleep(backoff)
                    backoff *= 2
                    continue

                # other 4xx: fatal
                text = await resp.text()
                self.logger.error(f"Client error {resp.status} on {url}: {text}")
                resp.raise_for_status()

        raise RuntimeError(f"Failed {method} {url} after {self.max_retries} attempts")

    async def get_artist(self, artist_id: str) -> Dict[str, Any]:
        return await self._request("GET", f"artists/{artist_id}")

    async def get_artist_albums(
        self,
        artist_id: str,
        limit: int = 50,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Paginate through all albums for an artist.
        Yields each page as a dict.
        """
        offset = 0
        while True:
            page = await self._request(
                "GET",
                f"artists/{artist_id}/albums",
                params={"limit": limit, "offset": offset},
            )
            yield page
            items = page.get("items", [])
            if len(items) < limit:
                break
            offset += limit

    async def get_album_tracks(
        self,
        album_id: str,
        limit: int = 50,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Paginate through all tracks for an album.
        Yields each page as a dict.
        """
        offset = 0
        while True:
            page = await self._request(
                "GET",
                f"albums/{album_id}/tracks",
                params={"limit": limit, "offset": offset},
            )
            yield page
            items = page.get("items", [])
            if len(items) < limit:
                break
            offset += limit

    async def close(self) -> None:
        if self._session:
            await self._session.close()


async def fetch_all_artist_albums(
    scraper: AsyncSpotifyScraper,
    artist_id: str
) -> List[Dict]:
    """Collect *all* album items for a given artist into a single list."""
    albums: List[Dict] = []
    async for page in scraper.get_artist_albums(artist_id):
        albums.extend(page.get("items", []))
    return albums


async def fetch_all_album_tracks(
    scraper: AsyncSpotifyScraper,
    album_id: str
) -> List[Dict]:
    """Collect *all* track items for a given album into a single list."""
    tracks: List[Dict] = []
    async for page in scraper.get_album_tracks(album_id):
        tracks.extend(page.get("items", []))
    return tracks


async def main():
    import os
    from dotenv import load_dotenv

    load_dotenv()

    logger = logging.getLogger(f"spotify_scraper_{os.getpid()}")
    logger.setLevel(logging.DEBUG)

    scraper = AsyncSpotifyScraper(
        logger=logger,
        client_id=os.getenv("SPOTIFY_CLIENT_ID"),
        client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
        refresh_token=os.getenv("SPOTIFY_REFRESH_TOKEN"),
        max_concurrent=20,
        max_retries=5,
    )

    try:
        artist_ids = ["0OdUWJ0sBjDrqHygGUXeCF", "1vCWHaC5f2uS3yhpwWbIA6"]
        artists = await asyncio.gather(
            *(scraper.get_artist(aid) for aid in artist_ids),
            return_exceptions=False
        )

        album_lists: List[List[Dict]] = await asyncio.gather(
            *(fetch_all_artist_albums(scraper, artist["id"]) for artist in artists),
            return_exceptions=False
        )
        all_albums = [alb for sub in album_lists for alb in sub]

        track_lists: List[List[Dict]] = await asyncio.gather(
            *(fetch_all_album_tracks(scraper, album["id"]) for album in all_albums),
            return_exceptions=False
        )
        all_tracks = [trk for sub in track_lists for trk in sub]
        print(f"Fetched {len(all_tracks)} tracks from {len(all_albums)} albums for {len(artists)} artists")
        return all_tracks

    finally:
        await scraper.close()


if __name__ == "__main__":
    asyncio.run(main())
