class Queries:
    def __init__(self, connection, cursor):
        self.connection = connection
        self.cursor = cursor

    def za1(self, since, until):
        """
        Wyszukanie wszystkich filmów, których conajmniej jeden członek został nominowany w okresie od X do Y oraz są aktualnie na promocji.	<- do zmiany
        """
        c = self.cursor

        c.execute(
            f"""SELECT movie.title, movie.promo, COUNT(crewmember.crewmemberid) FROM movie
JOIN CrewMember_Movie ON Movie.MovieId = CrewMember_Movie.MovieMovieId
JOIN CrewMember ON CrewMember_Movie.CrewMemberCrewMemberId = CrewMember.CrewMemberId
JOIN CrewMember_Award On CrewMember.CrewMemberId = CrewMember_Award.CrewMemberCrewMemberId
JOIN Award ON CrewMember_Award.AwardAwardId = Award.AwardId
WHERE Movie.Promo > 0.0 AND Award.DeliveryDate > :since AND Award.DeliveryDate < :until
    AND movie.title LIKE '%' AND movie.description LIKE '%'
    AND movie.budget > 0 AND EXTRACT(YEAR FROM movie.premieredate) > 0
    AND EXTRACT(YEAR FROM movie.premieredate) > 1
    AND movie.title in (SELECT movie.title FROM movie)
GROUP BY movie.movieid, movie.title, movie.promo
ORDER BY COUNT(crewmember.crewmemberid) DESC""",
            since=since,
            until=until,
        )

        return c.fetchall()

    def za2(self):
        c = self.cursor

        c.execute(
            f"""""",
        )

        return c.fetchall()

    def za3(self, rent_since, rent_until, premiere_date, title, awards, translations):
        c = self.cursor

        c.execute(
            f"""
SELECT rent.period, rent.price, rent.promo, rent.quality, rent.rentdate, count(award.awardid), count(language.languageid) FROM RENT
JOIN TRANSLATION ON RENT.RENTID = TRANSLATION.RENTRENTID
JOIN LANGUAGE ON translation.translationid = language.translationid
JOIN MOVIE ON MOVIE.MOVIEID = RENT.MOVIEMOVIEID
JOIN MOVIE_AWARD ON MOVIE.MOVIEID = MOVIE_AWARD.MOVIEMOVIEID
JOIN AWARD ON MOVIE_AWARD.AWARDAWARDID = AWARD.AWARDID
WHERE RENT.RENTDATE > :rent_since AND RENT.RENTDATE < :rent_until AND MOVIE.PREMIEREDATE < :premiere_date 
AND Movie.Title LIKE '%' || :title || '%'
group by rent.period, rent.price, rent.promo, rent.quality, rent.rentdate
HAVING count(award.awardid) > :awards AND count(language.languageid) > :translations
ORDER BY count(award.awardid)""",
            rent_since=rent_since,
            rent_until=rent_until,
            premiere_date=premiere_date,
            title=title,
            awards=awards,
            translations=translations
        )

        return c.fetchall()

    def zd1(self, free_movies, register_date, rent_count):
        c = self.cursor

        c.execute(
            f"""
UPDATE customer
SET customer.freemovies = :free_movies
WHERE (
    SELECT COUNT(cc.rentid) FROM Rent cc WHERE customer.customerId = cc.customercustomerid 
    AND customer.emailverified = 1 
    AND customer.registrationdate < :register_date
    HAVING COUNT(cc.rentid) > :rent_count
) > :rent_count""",
            free_movies=free_movies,
            register_date=register_date,
            rent_count=rent_count
        )

    def zd2(self, rents, promo):
        c = self.cursor

        c.execute(
            f"""
UPDATE movie
SET promo = CASE
            WHEN title IN (SELECT * FROM (SELECT title FROM movie
                                            JOIN RENT ON movie.movieid = rent.moviemovieid
                                            GROUP BY title
                                            ORDER BY COUNT(rent.rentid) ASC)
                            WHERE ROWNUM <= :rents) 
            THEN :promo
            ELSE 0.0
            END""",
            rents=rents,
            promo=promo
        )

    def zd3(self, days_not_verified):
        c = self.cursor

        c.execute(
            f"""
DELETE FROM customer
WHERE (
    SELECT COUNT(cc.rentid) FROM Rent cc WHERE customer.customerId = cc.customercustomerid 
    AND customer.emailverified = 0 
    AND customer.freemovies = 0
    AND CURRENT_DATE - customer.registrationdate > :days_not_verified
    HAVING COUNT(cc.rentid) = 0
) = 0""",
            days_not_verified=days_not_verified
        )
