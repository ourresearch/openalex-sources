from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    ForeignKey,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from .base import Base


class SourceISSN(Base):
    """Normalized ISSN membership. The UNIQUE(issn) constraint *is* the registry's
    one-ISSN-belongs-to-one-source invariant (the dedup the Spark `CreateSources`
    job did by hand). PK (source_id, issn) keeps every pair addressable."""

    __tablename__ = "source_issn"
    __table_args__ = (UniqueConstraint("issn", name="uq_source_issn_issn"),)

    source_id = Column(
        BigInteger, ForeignKey("sources.id", ondelete="CASCADE"), primary_key=True
    )
    issn = Column(Text, primary_key=True)
    is_issn_l = Column(Boolean, nullable=False, default=False)

    source = relationship("Source", back_populates="issns")

    def __repr__(self) -> str:
        return f"<SourceISSN {self.issn} -> S{self.source_id}>"
