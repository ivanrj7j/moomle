from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from moomle.database.connection import SessionLocal
from moomle.database.models import Advertiser


def seedAdvertisers(seedPath: Path | str = "adservers_seed.txt", session: Session | None = None) -> int:
    """Populates the advertisers table from a seed file containing blocked hostnames.

    Args:
        seedPath: Path to the seed text file containing one hostname per line.
        session: Optional SQLAlchemy session. If not provided, a new SessionLocal is used.

    Returns:
        The total number of newly inserted advertiser hostnames.
    """
    filePath = Path(seedPath)
    if not filePath.exists():
        print(f"[seed] Seed file '{filePath}' not found.")
        return 0

    with open(filePath, "r", encoding="utf-8") as f:
        uniqueHosts = {
            line.strip().lower()
            for line in f
            if line.strip() and not line.startswith("#")
        }

    def _executeSeed(dbSession: Session) -> int:
        Advertiser.__table__.create(dbSession.get_bind(), checkfirst=True)

        existingHosts = set(dbSession.scalars(select(Advertiser.hostname)).all())
        newHosts = [h for h in uniqueHosts if h not in existingHosts and len(h) <= 255]

        if not newHosts:
            print("[seed] Advertisers table is already up to date.")
            return 0

        print(f"[seed] Inserting {len(newHosts)} new advertiser hostnames...")
        batchSize = 2000
        for i in range(0, len(newHosts), batchSize):
            batch = [Advertiser(hostname=h) for h in newHosts[i : i + batchSize]]
            dbSession.add_all(batch)
            dbSession.commit()

        print(f"[seed] Successfully populated {len(newHosts)} advertiser hostnames.")
        return len(newHosts)

    if session is not None:
        return _executeSeed(session)

    with SessionLocal() as dbSession:
        return _executeSeed(dbSession)


if __name__ == "__main__":
    seedAdvertisers()
