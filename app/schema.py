from .db import get_engine
from .models import Base

def init_db(echo: bool = False) -> None:
    engine = get_engine(echo=echo)
    Base.metadata.create_all(engine)
