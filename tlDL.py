import asyncio
import logging
import os
from typing import List, Optional, Tuple, Union

import pyrogram
import yaml
from pyrogram.types import Audio, Document, Photo, Video, VideoNote, Voice
from rich.logging import RichHandler

from utils.file_management import get_next_name, manage_duplicate_file
from utils.log import LogFilter
from utils.meta import print_meta
from utils.updates import check_for_updates

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler()],
)
logging.getLogger("pyrogram.session.session").addFilter(LogFilter())
logging.getLogger("pyrogram.client").addFilter(LogFilter())
logger = logging.getLogger("media_downloader")

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
FAILED_IDS: list = []
DOWNLOADED_IDS: list = []


def update_config(config: dict):
    config["ids_to_retry"] = (
        list(set(config["ids_to_retry"]) - set(DOWNLOADED_IDS)) + FAILED_IDS
    )
    with open("config.yaml", "w") as yaml_file:
        yaml.dump(config, yaml_file, default_flow_style=False)
    logger.info("Updated last read message_ids to config file")


def _can_download(_type: str, file_formats: dict, file_format: Optional[str]) -> bool:
    if _type in ["audio", "document", "video"]:
        allowed_formats: list = file_formats[_type]
        if not file_format in allowed_formats and allowed_formats[0] != "all":
            return False
    return True


def _is_exist(file_path: str) -> bool:
    return not os.path.isdir(file_path) and os.path.exists(file_path)


async def _get_media_meta(
    media_obj: Union[Audio, Document, Photo, Video, VideoNote, Voice],
    _type: str,
) -> Tuple[str, Optional[str]]:
    if _type in ["audio", "document", "video"]:
        file_format: Optional[str] = media_obj.mime_type.split("/")[-1]
    else:
        file_format = None

    if _type in ["voice", "video_note"]:
        file_format = media_obj.mime_type.split("/")[-1]
        file_name: str = os.path.join(
            THIS_DIR,
            _type,
            "{}_{}.{}".format(
                _type,
                media_obj.date.isoformat(),
                file_format,
            ),
        )
    else:
        file_name = os.path.join(
            THIS_DIR, _type, getattr(media_obj, "file_name", None) or ""
        )
    return file_name, file_format


async def download_media(
    client: pyrogram.client.Client,
    message: pyrogram.types.Message,
    media_types: List[str],
    file_formats: dict,
):
    for retry in range(3):
        try:
            if message.media is None:
                return message.id
            for _type in media_types:
                _media = getattr(message, _type, None)
                if _media is None:
                    continue
                file_name, file_format = await _get_media_meta(_media, _type)
                if _can_download(_type, file_formats, file_format):
                    if _is_exist(file_name):
                        file_name = get_next_name(file_name)
                    download_path = await client.download_media(
                        message, file_name=file_name
                    )
                    download_path = manage_duplicate_file(download_path)
                    if download_path:
                        logger.info("Media downloaded - %s", download_path)
                    DOWNLOADED_IDS.append(message.id)
            break
        except pyrogram.errors.exceptions.bad_request_400.BadRequest:
            message = await handle_bad_request(client, message, retry)
        except TypeError:
            await handle_timeout_error(message, retry)
        except Exception as e:
            handle_generic_exception(message, e)
            break
    return message.id


async def handle_bad_request(client, message, retry):
    logger.warning(
        "Message[%d]: file reference expired, refetching...",
        message.id,
    )
    message = await client.get_messages(
        chat_id=message.chat.id,
        message_ids=message.id,
    )
    if retry == 2:
        logger.error(
            "Message[%d]: file reference expired for 3 retries, download skipped.",
            message.id,
        )
        FAILED_IDS.append(message.id)
    return message


async def handle_timeout_error(message, retry):
    logger.warning(
        "Timeout Error occurred when downloading Message[%d], retrying after 5 seconds",
        message.id,
    )
    await asyncio.sleep(5)
    if retry == 2:
        logger.error(
            "Message[%d]: Timing out after 3 reties, download skipped.",
            message.id,
        )
        FAILED_IDS.append(message.id)


def handle_generic_exception(message, e):
    logger.error(
        "Message[%d]: could not be downloaded due to following exception:\n[%s].",
        message.id,
        e,
        exc_info=True,
    )
    FAILED_IDS.append(message.id)


async def process_messages(
    client: pyrogram.client.Client,
    messages: List[pyrogram.types.Message],
    media_types: List[str],
    file_formats: dict,
) -> int:
    message_ids = await asyncio.gather(
        *[
            download_media(client, message, media_types, file_formats)
            for message in messages
        ]
    )

    last_message_id: int = max(message_ids)
    return last_message_id


async def process_chat(
    client: pyrogram.client.Client,
    chat_id: int,
    last_read_message_id: int,
    media_types: List[str],
    file_formats: dict,
    pagination_limit: int,
) -> int:
    messages_iter = client.get_chat_history(
        chat_id, offset_id=last_read_message_id, #reverse=True
    )
    messages_list: list = []
    pagination_count: int = 0

    async for message in messages_iter:
        if pagination_count != pagination_limit:
            pagination_count += 1
            messages_list.append(message)
        else:
            last_read_message_id = await process_messages(
                client,
                messages_list,
                media_types,
                file_formats,
            )
            pagination_count = 0
            messages_list = [message]

    if messages_list:
        last_read_message_id = await process_messages(
            client,
            messages_list,
            media_types,
            file_formats,
        )

    return last_read_message_id


async def begin_import(config: dict, pagination_limit: int) -> dict:
    client = pyrogram.Client(
        "media_downloader",
        api_id=config["api_id"],
        api_hash=config["api_hash"],
        proxy=config.get("proxy"),
    )
    await client.start()

    for chat_id in config["chat_ids"]:
        last_read_message_id = config["last_read_message_ids"].get(chat_id, 0)
        logger.info(f"Processing chat_id: {chat_id}")

        if config["ids_to_retry"]:
            logger.info("Downloading files failed during last run...")
            skipped_messages = await client.get_messages(
                chat_id=chat_id, message_ids=config["ids_to_retry"]
            )
            last_read_message_id = await process_messages(
                client,
                skipped_messages,
                config["media_types"],
                config["file_formats"],
            )

        last_read_message_id = await process_chat(
            client,
            chat_id,
            last_read_message_id,
            config["media_types"],
            config["file_formats"],
            pagination_limit,
        )

        config["last_read_message_ids"][chat_id] = last_read_message_id

    await client.stop()
    return config


def main():
    with open(os.path.join(THIS_DIR, "config.yaml")) as f:
        config = yaml.safe_load(f)
    updated_config = asyncio.get_event_loop().run_until_complete(
        begin_import(config, pagination_limit=100)
    )
    if FAILED_IDS:
        logger.info(
            "Downloading of %d files failed. "
            "Failed message ids are added to config file.\n"
            "These files will be downloaded on the next run.",
            len(set(FAILED_IDS)),
        )
    update_config(updated_config)
    check_for_updates()


if __name__ == "__main__":
    print_meta(logger)
    main()
