from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import DATABASE_URL
# Database engine
engine = create_async_engine(
    DATABASE_URL,
    echo=False,           # SQL-логи (только для отладки)
    pool_pre_ping=True,   # Проверка соединения
)


# Session factory
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)


# Dependency
async def get_db() -> AsyncSession:
    """
    FastAPI dependency.
    Создаёт и корректно закрывает сессию БД.
    """
    async with AsyncSessionLocal() as session:
        yield session