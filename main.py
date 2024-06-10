from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
from telethon.sync import TelegramClient
from telethon import events

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime)
    status = Column(String)
    status_updated_at = Column(DateTime)
    last_message = Column(String)


engine = create_engine('postgresql://postgres:postgres@localhost/test_bot')
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

app = TelegramClient("my_account", api_id='***', api_hash='****')


async def send_message(user_id, text):
    try:
        await app.send_message(user_id, text)
    except Exception as e:
        print(f"Error sending message to user {user_id}: {e}")
        user = session.query(User).filter(User.id == user_id).first()
        user.status = 'dead'
        user.status_updated_at = datetime.now()
        session.commit()


async def main():
    while True:
        users = session.query(User).filter(User.status == 'alive').all()

        for user in users:
            if datetime.now() - user.created_at >= timedelta(minutes=6):
                await send_message(user.id, "Текст1")
                user.status = 'dead'
                user.status_updated_at = datetime.now()

        session.commit()

        for user in users:
            if user.status == 'dead' and datetime.now() - user.status_updated_at >= timedelta(days=1, hours=2):
                await send_message(user.id, "Текст3")
                user.status = 'finished'
                user.status_updated_at = datetime.now()

        session.commit()

        for user in users:
            if "прекрасно" in user.last_message or "ожидать" in user.last_message:
                user.status = 'finished'
                user.status_updated_at = datetime.now()
                await send_message(user.id, "Ваш запрос обработан. Воронка завершена.")

        session.commit()

        for user in users:
            if "Триггер1" in user.last_message:
                await send_message(user.id, "Текст2")
                user.status = 'dead'
                user.status_updated_at = datetime.now()
                user.last_message = "Текст2"

        session.commit()


@app.on(events.NewMessage())
async def on_message(event):
    message = event.message
    user = session.query(User).filter(User.id == message.chat_id).first()
    if user:
        user.last_message = message.text
        session.commit()
        user.status = 'alive'
        user.status_updated_at = datetime.now()
        session.commit()


if __name__ == '__main__':
    import asyncio

    app.start()
    asyncio.run(main())
