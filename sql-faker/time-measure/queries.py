class Queries:
    def __init__(self, connection, cursor):
        self.connection = connection
        self.cursor = cursor

    def query_1(self, since, until):
        c = self.cursor

        c.execute(
            f"""
                SELECT * FROM movie
JOIN CrewMember_Movie ON Movie.MovieId = CrewMember_Movie.MovieMovieId
JOIN CrewMember ON CrewMember_Movie.CrewMemberCrewMemberId = CrewMember.CrewMemberId
JOIN CrewMember_Award On CrewMember.CrewMemberId = CrewMember_Award.CrewMemberCrewMemberId
JOIN Award ON CrewMember_Award.AwardAwardId = Award.AwardId
WHERE Movie.Promo > 0.0 AND Award.DeliveryDate > :since AND Award.DeliveryDate < :until""",
            since=since,
            until=until,
        )

        return c.fetchall()
