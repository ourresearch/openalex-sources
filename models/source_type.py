from sqlalchemy import Column, Text

from .base import Base


class SourceType(Base):
    """Controlled vocabulary for `sources.type` (journal, ebook platform,
    conference, repository, book series, ...)."""

    __tablename__ = "source_type"

    source_type_id = Column(Text, primary_key=True)
    display_name = Column(Text)
    description = Column(Text)
