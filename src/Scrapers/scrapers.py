if __name__ == "__main__":
    import gevent 
    from gevent import monkey; monkey.patch_all()

import forebet, sofascore, betexplorer, odds

if __name__ == "__main__":
    scrapers = [betexplorer.outcomes, forebet.predictions,
        sofascore.votes, sofascore.h2h, sofascore.pregame,
        odds.neds, odds.tab, odds.bluebet,
        odds.ps3838, odds.palmerbet,
        odds.onex, odds.interwetten,
        odds.playup, odds.betfair]
    scrapers = [odds.betfair]

    MIN_GAMES = 50
    def length_test(scraper):
        games = scraper()
        if len(games) >= MIN_GAMES:
            print(f"{scraper.name} passed length test with {len(games)} games")
        else:
            print(f"{scraper.name} failed length test with {len(games)} games")

    jobs = [gevent.spawn(length_test, scraper) for scraper in scrapers]
    gevent.wait(jobs)
