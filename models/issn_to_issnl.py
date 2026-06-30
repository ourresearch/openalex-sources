from sqlalchemy import Column, DateTime, Text, func

from .base import Base


class IssnToIssnl(Base):
    """ISSN -> ISSN-L mapping (port of guts `mid.journal_issn_to_issnl`)."""

    __tablename__ = "issn_to_issnl"

    issn = Column(Text, primary_key=True)
    issn_l = Column(Text, index=True)
    note = Column(Text)
    updated_date = Column(DateTime(timezone=True), server_default=func.now())
