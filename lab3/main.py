import faust


app = faust.App(
    'messaging_app',
    broker='kafka://kafka:9092',
    store='memory://',
    topic_partitions=3,
)


class UserMessage(faust.Record, serializer='json'):
    """Пользовательское сообщение"""
    sender: str
    receiver: str
    text: str


class FilteredMessage(faust.Record, serializer='json'):
    """Отфильтрованное сообщение"""
    original_sender: str
    receiver: str
    filtered_text: str


class BlockUser(faust.Record, serializer='json'):
    """Блокировка пользователя"""
    user: str
    blocked_user: str
    action: str  # block or unblock


class CensorWord(faust.Record, serializer='json'):
    """добавление слова """
    word: str
    action: str # add or remove


# топики
messages_topic = app.topic('messages', value_type=UserMessage)
filtered_messages_topic = app.topic('filtered_messages', value_type=FilteredMessage)
blocked_users_topic = app.topic('blocked_users', value_type=BlockUser)
censored_words_topic = app.topic('censored_words', value_type=CensorWord)

# таблицы
blocked_users_table = app.Table(
    'blocked_users_table',
    default=set,
    key_type=str,
    value_type=set,
)

censored_words_table = app.Table(
    'censored_words_table',
    default=set,
    key_type=str,
    value_type=set,
)


@app.agent(blocked_users_topic)
async def process_users_blocking(messages):
    """Обработчик блокировок"""
    async for m in messages:
        if m.user not in blocked_users_table:
            blocked_users_table[m.user] = set()

        blocked_users = blocked_users_table[m.user].copy()
        if m.action == 'block':
            blocked_users.add(m.blocked_user)
        elif m.action == 'unblock':
            blocked_users.discard(m.blocked_user)

        blocked_users_table[m.user] = blocked_users # здесь именно заново присваиваем, чтобы faust обработал данное событие


@app.agent(censored_words_topic)
async def process_censor_words(messages):
    """Обработчик обновления списка запрещённых слов"""

    async for m in messages:
        if 'words' not in censored_words_table:
            censored_words_table['words'] = set()

        censored_words = censored_words_table['words'].copy()

        if m.action == 'add':
            censored_words.add(m.word)
        elif m.action == 'remove':
            censored_words.discard(m.word)
        censored_words_table['words'] = censored_words


def censor_text(text: str) -> str:
    """Цензура текста"""

    banned_words = censored_words_table.get('words', set())

    words = text.split(' ')
    censored_words = []

    for word in words:
        if word.lower() in banned_words:
            word = '*'*len(word)
            censored_words.append(word)
        else:
            censored_words.append(word)

    return ' '.join(censored_words)


@app.agent(messages_topic)
async def process_messages(messages):
    """
    Основной обработчик сообщений.
    Фильтрует сообщения от заблокированных пользователей и применяет цензуру.
    """
    async for m in messages:
        # провеяем, не заблокирован ли юзер
        if m.receiver in blocked_users_table:
            recipient_blocks = blocked_users_table[m.receiver]
            if m.sender in recipient_blocks:
                continue

        # применяем цензуру
        filtered_text = censor_text(m.text)

        # выходное сообщение
        filtered_message = FilteredMessage(
            original_sender=m.sender,
            receiver=m.receiver,
            filtered_text=filtered_text,
        )

        # отправляем в выходной топик
        await filtered_messages_topic.send(value=filtered_message)


if __name__ == '__main__':
    app.main()
