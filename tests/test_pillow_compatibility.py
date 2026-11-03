"""Exercise the crawler's Pillow paths without a browser or external website."""

import base64
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from PIL import Image, __version__ as pillow_version
from packaging.version import Version

from crawl4ai.async_crawler_strategy import AsyncPlaywrightCrawlerStrategy


def test_pillow_security_floor():
    assert Version(pillow_version) >= Version("13.0.0")


def make_strategy():
    strategy = AsyncPlaywrightCrawlerStrategy.__new__(AsyncPlaywrightCrawlerStrategy)
    strategy.logger = Mock()
    strategy.get_page_dimensions = AsyncMock(return_value={"width": 64, "height": 90})
    return strategy


@pytest.mark.asyncio
@pytest.mark.parametrize("height", [64, 90])
async def test_segmented_screenshot_preserves_pixels_and_height(height):
    strategy = make_strategy()
    strategy.get_page_dimensions.return_value["height"] = height
    page = SimpleNamespace(
        viewport_size={"width": 64, "height": 64},
        evaluate=AsyncMock(),
    )

    async def set_viewport_size(size):
        page.viewport_size = size

    async def screenshot(**kwargs):
        output = BytesIO()
        Image.new("RGB", tuple(page.viewport_size.values()), "red").save(output, "JPEG")
        return output.getvalue()

    page.set_viewport_size = AsyncMock(side_effect=set_viewport_size)
    page.screenshot = AsyncMock(side_effect=screenshot)
    encoded = await strategy.take_screenshot_scroller(page, screenshot_height_threshold=64)
    with Image.open(BytesIO(base64.b64decode(encoded))) as result:
        result.load()
        assert result.format == "BMP"
        assert result.size == (64, height)
        assert result.getpixel((0, height - 1))[0] > 250
    strategy.logger.error.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("segmented", [False, True])
async def test_screenshot_failure_returns_error_image(segmented):
    strategy = make_strategy()
    page = SimpleNamespace(
        viewport_size={"width": 64, "height": 64},
        set_viewport_size=AsyncMock(),
        evaluate=AsyncMock(),
        screenshot=AsyncMock(side_effect=RuntimeError("Screenshot unavailable")),
    )
    capture = strategy.take_screenshot_scroller if segmented else strategy.take_screenshot_naive
    encoded = await capture(page, **({"screenshot_height_threshold": 64} if segmented else {}))
    with Image.open(BytesIO(base64.b64decode(encoded))) as result:
        result.load()
        assert result.format == "JPEG"
        assert result.size == (800, 600)
    strategy.logger.error.assert_called_once()
