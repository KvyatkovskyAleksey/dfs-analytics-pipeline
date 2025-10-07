from scripts.rotogrinders_scraper import Sport, RotogrindersScraper
from scripts.staging_processor import StagingProcessor


def load_data_to_staging(date: str):
    sports: list[Sport] = ["NFL"]
    for sport in sports:
        scraper = RotogrindersScraper(date, sport)
        scraper.scrape()
        processor = StagingProcessor(scraper.get_data())
        processor.save_data_to_s3()


if __name__ == "__main__":
    load_data_to_staging("2025-10-02")
