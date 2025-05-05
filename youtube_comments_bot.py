import logging
import csv
import traceback
from telegram import Update, InputFile
from telegram.ext import Application, CommandHandler, CallbackContext
from googleapiclient.discovery import build
from dotenv import load_dotenv
import os

# Загрузка переменных из .env
load_dotenv()

# Включаем логирование
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Укажите ваш API-ключ YouTube Data API
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')

async def start(update: Update, context: CallbackContext) -> None:
    await update.message.reply_text('Привет! Отправь мне ссылку на канал YouTube, и я выгружу комментарии с последних пяти видео в CSV файл.')

def get_channel_videos(channel_id):
    youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
    video_ids = []
    try:
        request = youtube.search().list(
            part='snippet',
            channelId=channel_id,
            maxResults=50,  # Максимальное количество видео за один запрос
            order='date'
        )

        while request and len(video_ids) < 5000:  # Продолжаем, пока не наберем достаточно видео
            response = request.execute()
            video_ids.extend(
                [item['id']['videoId'] for item in response['items'] if item['id']['kind'] == 'youtube#video']
            )
            request = youtube.search().list_next(request, response)

        logger.info(f'Total video IDs fetched: {len(video_ids)}')
        return video_ids
    except Exception as e:
        logger.error(f'Error fetching videos for channel {channel_id}: {e}')
        return []

def get_comments(video_id, video_title, youtube):
    comments = []

    try:
        request = youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            maxResults=100
        )

        page_count = 0  # Счётчик страниц
        while request and len(comments) < 5000:  # Ограничение на 5000 комментариев
            page_count += 1
            logger.info(f'Fetching page {page_count} of comments for video ID {video_id}')
            response = request.execute()
            for item in response['items']:
                comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
                author = item['snippet']['topLevelComment']['snippet']['authorDisplayName']
                comments.append((comment, video_title, author))
                if len(comments) >= 5000:
                    break
            logger.info(f'Fetched {len(response["items"])} comments from page {page_count}')
            # Получение следующей страницы комментариев
            request = youtube.commentThreads().list_next(request, response)

        logger.info(f'Total comments fetched for video ID {video_id}: {len(comments)}')

    except Exception as e:
        logger.error(f'Error fetching comments for video ID {video_id}: {e}')
        if 'commentsDisabled' in str(e):
            raise ValueError(f'Комментарии отключены для видео {video_id}.')
        else:
            raise

    return comments

def save_to_csv(comments, filename):
    with open(filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Comment', 'Video Title', 'Author'])
        for comment, video_title, author in comments:
            writer.writerow([comment, video_title, author])

async def handle_message(update: Update, context: CallbackContext) -> None:
    try:
        message = update.message.text
        logger.info(f'Received message: {message}')

        if 'youtube.com/channel/' in message or 'youtube.com/@' in message:
            channel_id = message.split('channel/')[-1].split('/')[0] if 'channel/' in message else message.split('@')[-1]
            logger.info(f'Extracted channel ID: {channel_id}')

            await update.message.reply_text('Загружаю комментарии с последних пяти видео, подождите...')
            video_ids = get_channel_videos(channel_id)
            logger.info(f'Fetched video IDs: {video_ids}')

            if not video_ids:
                await update.message.reply_text('Не удалось найти видео на канале. Проверьте ссылку.')
                return

            all_comments = []
            youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
            for video_id in video_ids:
                logger.info(f'Fetching comments for video ID: {video_id}')
                video_title = youtube.videos().list(
                    part='snippet',
                    id=video_id
                ).execute()['items'][0]['snippet']['title']
                comments = get_comments(video_id, video_title, youtube)
                logger.info(f'Fetched {len(comments)} comments for video ID {video_id}')
                all_comments.extend(comments)

            if not all_comments:
                await update.message.reply_text('Не удалось найти комментарии к видео.')
                return

            # Получение названия канала
            channel_title = youtube.channels().list(
                part='snippet',
                id=channel_id
            ).execute()['items'][0]['snippet']['title']

            # Формирование имени файла с названием канала
            filename = f'{channel_title}_comments_{channel_id}.csv'
            logger.info(f'Saving comments to file: {filename}')
            save_to_csv(all_comments, filename)
            logger.info(f'Comments successfully saved to file: {filename}')

            # Проверка существования файла перед отправкой
            if not os.path.exists(filename):
                logger.error(f'File not found: {filename}')
                await update.message.reply_text(f'Ошибка: файл {filename} не найден.')
                return

            # Логирование перед отправкой файла
            logger.info(f'Attempting to send file: {filename}')

            # Отправка файла в диалог
            with open(filename, 'rb') as file:
                try:
                    await update.message.reply_document(document=InputFile(file), filename=filename)
                    logger.info(f'File {filename} successfully sent.')
                except Exception as e:
                    logger.error(f'Error sending file {filename}: {e}')
                    await update.message.reply_text(f'Ошибка при отправке файла {filename}.')
        else:
            logger.warning('Invalid YouTube channel link received.')
            await update.message.reply_text('Пожалуйста, отправьте корректную ссылку на канал YouTube.')
    except Exception as e:
        logger.error(f'Error in handle_message: {e}')
        logger.error(traceback.format_exc())
        await update.message.reply_text('Произошла ошибка при обработке канала.')

async def parse(update: Update, context: CallbackContext) -> None:
    try:
        if not context.args:
            await update.message.reply_text('Пожалуйста, отправьте ссылку на видео вместе с командой /parse.')
            return

        message = context.args[0]
        logger.info(f'Received message: {message}')

        if 'youtube.com/channel/' in message or 'youtube.com/@' in message:
            channel_id = message.split('channel/')[-1].split('/')[0] if 'channel/' in message else message.split('@')[-1]
            logger.info(f'Extracted channel ID: {channel_id}')

            await update.message.reply_text('Загружаю комментарии с последних видео, пока не наберется 5000 комментариев. Подождите...')
            try:
                youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
                video_ids = get_channel_videos(channel_id)
                logger.info(f'Fetched video IDs: {video_ids}')
            except Exception as e:
                logger.error(f'Error fetching video IDs: {e}')
                await update.message.reply_text('Ошибка при получении видео с канала.')
                return

            if not video_ids:
                await update.message.reply_text('Не удалось найти видео на канале. Проверьте ссылку.')
                return

            all_comments = []
            for video_id in video_ids:
                if len(all_comments) >= 5000:
                    break

                try:
                    logger.info(f'Fetching comments for video ID: {video_id}')
                    video_title = youtube.videos().list(
                        part='snippet',
                        id=video_id
                    ).execute()['items'][0]['snippet']['title']
                    comments = get_comments(video_id, video_title, youtube)
                    logger.info(f'Fetched {len(comments)} comments for video ID {video_id}')
                    all_comments.extend(comments)
                except Exception as e:
                    if 'commentsDisabled' in str(e):
                        logger.warning(f'Comments are disabled for video ID {video_id}. Skipping.')
                        await update.message.reply_text(f'Комментарии отключены для видео {video_id}. Пропускаю.')
                    else:
                        logger.error(f'Error fetching comments for video ID {video_id}: {e}')
                        await update.message.reply_text(f'Ошибка при получении комментариев для видео {video_id}.')

            if not all_comments:
                await update.message.reply_text('Не удалось найти подходящие видео с комментариями.')
                return

            # Получение названия канала
            channel_title = youtube.channels().list(
                part='snippet',
                id=channel_id
            ).execute()['items'][0]['snippet']['title']

            # Формирование имени файла с названием канала
            filename = f'{channel_title}_comments_{channel_id}.csv'
            try:
                logger.info(f'Saving comments to file: {filename}')
                save_to_csv(all_comments[:5000], filename)
                logger.info(f'Comments successfully saved to file: {filename}')
            except Exception as e:
                logger.error(f'Error saving comments to file: {e}')
                await update.message.reply_text('Ошибка при сохранении комментариев в файл.')
                return

            await update.message.reply_text(f'Комментарии сохранены в файл {filename}')
        else:
            logger.warning('Invalid YouTube channel link received.')
            await update.message.reply_text('Пожалуйста, отправьте корректную ссылку на канал YouTube.')
    except Exception as e:
        logger.error(f'Unhandled error in parse: {e}')
        logger.error(traceback.format_exc())
        await update.message.reply_text('Произошла ошибка при обработке канала.')

def main():
    # Укажите токен вашего Telegram-бота
    TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')

    application = Application.builder().token(TELEGRAM_TOKEN).build()

    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('help', start))
    application.add_handler(CommandHandler('comments', handle_message))
    application.add_handler(CommandHandler('parse', parse))

    # Возвращаемся к использованию polling
    application.run_polling()

if __name__ == '__main__':
    main()