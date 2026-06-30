from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from .base import Base

# Public OpenAlex host used to render the API id (https://openalex.org/S<id>).
API_HOST = "https://openalex.org"


class Source(Base):
    """Authoritative sources registry row. PK is the OpenAlex S-id (a BIGINT).
    Existing ids are preserved from the migration; new ids auto-mint from
    `source_id_seq` (wired as the column DEFAULT) on a plain insert. Explicit-id
    inserts still work and bypass the sequence."""

    __tablename__ = "sources"

    id = Column(
        BigInteger,
        primary_key=True,
        server_default=text("nextval('source_id_seq'::regclass)"),
    )

    display_name = Column(Text)
    type = Column(Text, ForeignKey("source_type.source_type_id"))
    issn_l = Column(Text)  # canonical ISSN-L (was `issn` in the Spark table)

    publisher = Column(Text)
    publisher_id = Column(BigInteger)
    institution_id = Column(BigInteger)
    homepage_url = Column(Text)  # was `webpage`

    country = Column(Text)
    country_code = Column(Text)

    apc_usd = Column(Integer)
    apc_prices = Column(JSONB)            # [{price, currency}]
    societies = Column(JSONB)             # [{url, organization}]
    is_society_journal = Column(Boolean)
    alternate_titles = Column(JSONB)      # [str]

    wikidata_id = Column(Text)
    fatcat_id = Column(Text)
    crossref_id = Column(Text)
    datacite_id = Column(Text)
    datacite_ids = Column(JSONB)          # [str]

    endpoint_id = Column(Text)            # repository OAI endpoint
    sample_pmh_record = Column(Text)

    is_oa = Column(Boolean)
    is_in_doaj = Column(Boolean)
    is_in_doaj_start_year = Column(Integer)
    doaj_license = Column(Text)
    is_in_scielo = Column(Boolean)
    is_ojs = Column(Boolean)
    is_oa_high_oa_rate = Column(Boolean)
    high_oa_rate_start_year = Column(BigInteger)
    is_fully_open_in_jstage = Column(Boolean)
    is_core = Column(Boolean)
    is_preprint_repository = Column(Boolean)

    merge_into_id = Column(BigInteger, ForeignKey("sources.id"))
    merge_into_date = Column(DateTime(timezone=True))

    display_name_before_override = Column(Text)
    override_timestamp = Column(DateTime(timezone=True))

    created_date = Column(DateTime(timezone=True), server_default=func.now())
    updated_date = Column(DateTime(timezone=True), server_default=func.now())

    issns = relationship(
        "SourceISSN", back_populates="source", cascade="all, delete-orphan"
    )

    @property
    def openalex_id(self) -> str:
        return f"{API_HOST}/S{self.id}"

    def __repr__(self) -> str:
        return f"<Source S{self.id} {self.display_name!r}>"
