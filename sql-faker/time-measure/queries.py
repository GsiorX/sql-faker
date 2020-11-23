class Queries:
    def __init__(self, connection, cursor):
        self.connection = connection
        self.cursor = cursor

    def za1(self, since, until):
        c = self.cursor

        c.execute(
            f"""SELECT DISTINCT mov.movieid
FROM Movie mov
JOIN GENRE_MOVIE gm ON mov.movieid = gm.moviemovieid
JOIN GENRE g ON g.genreid = gm.genregenreid
WHERE
    (SELECT COUNT(mov2.movieid) FROM Movie mov2
        JOIN GENRE_MOVIE gm2 ON mov2.movieid = gm2.moviemovieid
        JOIN GENRE g2 ON g2.genreid = gm2.genregenreid
        WHERE mov.movieid = mov2.movieid AND g2.name LIKE 'Histor%') > 0
ORDER BY mov.movieid"""
        )

        return c.fetchall()

    def za2(self, before_birth, delivery_date, awards):
        c = self.cursor

        c.execute(
            f"""SELECT c.crewmemberid, COUNT(a.awardid), 
    ROUND(AVG((SELECT COUNT(*) FROM movie m WHERE m.premieredate > c.birthdate AND m.movieid < a.awardid AND ca.awardawardid > m.promo)))
FROM crewmember c
JOIN crewmember_job ON c.crewmemberid = crewmember_job.crewmembercrewmemberid
JOIN job j ON j.jobid = crewmember_job.jobjobid
JOIN crewmember_award ca ON ca.crewmembercrewmemberid = c.crewmemberid
JOIN award a ON ca.awardawardid = a.awardid
WHERE c.birthdate < TO_DATE('2020-01-01', 'YYYY-MM-DD') AND j.name LIKE 'Director'
    AND a.deliverydate >= 0
GROUP BY c.crewmemberid
HAVING COUNT(a.awardid) > 1"""
        )

        return c.fetchall()

    def za3(self, rent_since, rent_until_years, premiere_date, title, awards, translations):
        c = self.cursor

        c.execute(
            f"""SELECT * FROM (
(Select rent.period, rent.price, rent.promo, rent.quality, rent.rentdate from rent)
MINUS
(SELECT rent.period, rent.price, rent.promo, rent.quality, rent.rentdate from rent
JOIN TRANSLATION ON RENT.RENTID = TRANSLATION.RENTRENTID
JOIN LANGUAGE ON translation.translationid = language.translationid
JOIN MOVIE ON MOVIE.MOVIEID = RENT.MOVIEMOVIEID
JOIN MOVIE_AWARD ON MOVIE.MOVIEID = MOVIE_AWARD.MOVIEMOVIEID
JOIN AWARD ON MOVIE_AWARD.AWARDAWARDID = AWARD.AWARDID
JOIN CrewMember_Movie ON Movie.MovieId = CrewMember_Movie.MovieMovieId
JOIN CrewMember ON crewmember_movie.crewmembercrewmemberid = crewmember.crewmemberid
JOIN (SELECT * FROM (SELECT rent.rentid as ren, avg(rent.price) as avr FROM rent Group by rent.rentid) WHERE avr > 30) ON ren = rent.rentid
WHERE RENT.RENTDATE IN (Select rentdate from rent where sysdate > add_months(RENTDATE, -12*50) AND RENTDATE > TO_DATE('1970-01-01', 'YYYY-MM-DD'))
AND MOVIE.PREMIEREDATE IN (Select premieredate from movie where MOVIE.PREMIEREDATE < TO_DATE('2020-11-01', 'YYYY-MM-DD'))
AND Movie.Title IN (Select title from movie where substr(trim(lower(title)), 1, 4) LIKE '%' || '%' || '%')
AND (SELECT DISTINCT CrewMember.Name from CrewMember where crewmember.crewmemberid = crewmember_movie.crewmembercrewmemberid) Like '%' || '%'
AND (SELECT AVG(movie.budget) from movie) < movie.budget
AND (SELECT Count(*) from rate WHERE rate.MovieMovieId = movie.movieid) > 0
group by rent.period, rent.price, rent.promo, rent.quality, rent.rentdate
)
)"""
        )

        return c.fetchall()

    def zd1(self, free_movies, register_date, rent_count):
        c = self.cursor

        c.execute(
            f"""
UPDATE customer
SET customer.freemovies = 3
WHERE (
    SELECT COUNT(cc.rentid) FROM Rent cc WHERE customer.customerId = cc.customercustomerid 
    AND customer.emailverified = 1 
    AND customer.registrationdate < TO_DATE('1970-01-01', 'YYYY-MM-DD')
    HAVING COUNT(cc.rentid) > 2
) > 2"""
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
                            WHERE ROWNUM <= 50) 
            THEN 0.3
            ELSE 0.0
            END"""
        )

    def zd3(self, days_not_verified):
        c = self.cursor

        c.execute(          
            f"""
DELETE FROM customer
WHERE customer.emailverified = 0 
    AND customer.freemovies = 0 AND
    (SELECT COUNT(cc.rentid) FROM Rent cc WHERE customer.customerId = cc.customercustomerid 
    AND CURRENT_DATE > customer.registrationdate + :days_not_verified
    HAVING COUNT(cc.rentid) = 0
) = 0
AND (SELECT AVG(value) from Rate WHERE customer.customerId = Rate.customercustomerid AND value < 5) < 5
AND (SELECT Length(Description) from Rate WHERE customer.customerId = Rate.customercustomerid AND Length(Description) > 5) > 5"""
        )

    def zd4(self):
        c = self.cursor

        c.execute(
            f"""
INSERT INTO Movie (MovieId, title, premieredate, duration, budget, description, studio, promo, translationtranslationid)
SELECT
(SELECT MAX(MovieId) + 1 FROM Movie),
(SELECT CONCAT('TEST__', CONCAT(title, '__TEST')) from movie where Length(title) = (SELECT MAX(Length(Title)) from movie) FETCH NEXT 1 ROW ONLY),
PREMIEREDATE,
(SELECT AVG(Duration) from movie),
(SELECT SUM(Budget) from movie),
DESCRIPTION,
(select substr(studio, 1, 1) from movie group by substr(studio, 1, 1) order by count(*) desc FETCH NEXT 1 ROW ONLY),
PROMO, TRANSLATIONTRANSLATIONID from Movie
WHERE (SELECT AVG(budget) from Movie) > 5096078
ORDER BY MovieId desc
FETCH NEXT 1 ROW ONLY"""
        )
