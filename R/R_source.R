options(stringsAsFactors=FALSE, dplyr.summarise.inform = FALSE)
library(sqldf)
library(dplyr)
library(data.table)
library(microbenchmark)
library(stringi)

Badges <- read.csv("data/Badges.csv.gz")
Comments <- read.csv("data/Comments.csv.gz")
PostLinks <- read.csv("data/PostLinks.csv.gz")
Posts <- read.csv("data/Posts.csv.gz")
Tags <- read.csv("data/Tags.csv.gz")
Users <- read.csv("data/Users.csv.gz")
Votes <- read.csv("data/Votes.csv.gz")


# ----------------------------- (1) 

df_sql_1 <- function() {
  sqldf("
    SELECT Count, TagName 
    FROM Tags
    WHERE Count > 1000 
    ORDER BY Count DESC
    ")
}

df_base_1 <- function(Tags) {
  
  x <- Tags[Tags$Count > 1000, c("Count", "TagName")] 
  x <- x[order(x$Count, decreasing = TRUE), ]
  rownames(x) <- NULL
  
  x
  
}

df_dplyr_1 <- function(Tags) {
  
  filter(Tags, Count > 1000) %>% select(Count, TagName) %>% arrange(desc(Count))

}

df_table_1 <- function(Tags) {
  
  TagsDT <- as.data.table(Tags)
  TagsDT <- TagsDT[Count > 1000, .(Count, TagName)][order(-Count)] 
  
  as.data.frame(TagsDT)
  
}

dplyr::all_equal(df_sql_1(), df_base_1(Tags))
compare::compare(df_sql_1(), df_base_1(Tags))

dplyr::all_equal(df_sql_1(), df_dplyr_1(Tags))
compare::compare(df_sql_1(), df_dplyr_1(Tags))

dplyr::all_equal(df_sql_1(), df_table_1(Tags))
compare::compare(df_sql_1(), df_table_1(Tags))

microbenchmark::microbenchmark(
  sqldf = df_sql_1(), 
  base = df_base_1(Tags),
  dplyr = df_dplyr_1(Tags),
  data.table = df_table_1(Tags)
)


# ----------------------------- (2)

df_sql_2 <- function() {
  sqldf("
    SELECT Location, COUNT(*) AS Count 
    FROM (
            SELECT Posts.OwnerUserId, Users.Id, Users.Location 
            FROM Users
            JOIN Posts ON Users.Id = Posts.OwnerUserId
          )
    WHERE Location NOT IN ('') 
    GROUP BY Location
    ORDER BY Count DESC
    LIMIT 10
    ")
}

df_base_2 <- function(Users, Posts) {
  
  x <- merge(x=Users, y=Posts, by.x="Id", by.y="OwnerUserId")
  x <- x[x$Location != "", c("Id", "Location")]
  
  x <- as.data.frame(table(x$Location), stringsAsFactors = FALSE)
  names(x) <- c("Location", "Count")

  x <- x[order(x$Count, decreasing = TRUE),][1:10,]
  row.names(x) <- NULL
  
  x
  
}

df_dplyr_2 <- function(Users, Posts) {
  
  x <- inner_join(x=Posts, y=Users, by=c("OwnerUserId" = "Id")) %>% select(Id, Location)
  
  x <- filter(x, Location != "") %>% group_by(Location) %>% summarise(Count = n()) %>% arrange(desc(Count)) %>% 
    slice(1:10)
  
  as.data.frame(x)
  
}

df_table_2 <- function(Users, Posts) {
  
  UsersDT <- as.data.table(Users)
  PostsDT <- as.data.table(Posts)
  
  x <- merge(x=UsersDT, y=PostsDT, by.x="Id", by.y="OwnerUserId")
  x <- x[Location != "", .(Id, Location)][, .(Count = .N), Location][order(-Count)][1:10]
  
  as.data.frame(x)
  
}

dplyr::all_equal(df_sql_2(), df_base_2(Users, Posts))
compare::compare(df_sql_2(), df_base_2(Users, Posts))

dplyr::all_equal(df_sql_2(), df_dplyr_2(Users, Posts))
compare::compare(df_sql_2(), df_dplyr_2(Users, Posts))

dplyr::all_equal(df_sql_2(), df_table_2(Users, Posts))
compare::compare(df_sql_2(), df_table_2(Users, Posts))

microbenchmark::microbenchmark(
  sqldf = df_sql_2(), 
  base = df_base_2(Users, Posts),
  dplyr = df_dplyr_2(Users, Posts),
  data.table = df_table_2(Users, Posts)
)


# ----------------------------- (3)

df_sql_3 <- function() {
  
  sqldf("
    SELECT Year, SUM(Number) AS TotalNumber 
    FROM (
            SELECT 
                Name, 
                COUNT(*) AS Number, 
                STRFTIME('%Y', Badges.Date) AS Year
            FROM Badges
            WHERE Class = 1 
            GROUP BY Name, Year
          )
    GROUP BY Year
    ORDER BY TotalNumber
    ")
}

df_base_3 <- function(Badges) {
  
  x <- Badges[Badges$Class == 1,]
  x <- as.data.frame(table(x$Name, stri_sub(x$Date, 1, 4)), stringsAsFactors = FALSE)
  names(x) <- c("Name", "Year", "Number")
  
  x <- sapply(split(x, x$Year), function(x) sum(x$Number))
  x <- structure(data.frame(names(x), x, row.names=NULL), names=c("Year", "TotalNumber"))
  
  x <- x[order(x$TotalNumber),]
  row.names(x) <- NULL
  
  x
  
}

df_dplyr_3 <- function(Badges) {
  
  x <- filter(Badges, Class == 1) %>% mutate(Year = stri_sub(Date, 1, 4)) %>% group_by(Name, Year)
  x <- summarise(x, Number=n()) %>% group_by(Year) %>% summarise(TotalNumber=sum(Number)) %>% arrange(TotalNumber)
  
  as.data.frame(x)
  
}

df_table_3 <- function(Badges) {
  
  BadgesDT <- as.data.table(Badges)

  BadgesDT <- BadgesDT[Class == 1][, Year := stri_sub(Date, 1, 4)][, .(Number = .N), .(Name, Year)]
  BadgesDT <- BadgesDT[, .(TotalNumber=sum(Number)), Year][order(TotalNumber)]
  
  as.data.frame(BadgesDT)
  
}

dplyr::all_equal(df_sql_3(), df_base_3(Badges))
compare::compare(df_sql_3(), df_base_3(Badges))

dplyr::all_equal(df_sql_3(), df_dplyr_3(Badges))
compare::compare(df_sql_3(), df_dplyr_3(Badges))

dplyr::all_equal(df_sql_3(), df_table_3(Badges))
compare::compare(df_sql_3(), df_table_3(Badges))

microbenchmark::microbenchmark(
  sqldf = df_sql_3(), 
  base = df_base_3(Badges),
  dplyr = df_dplyr_3(Badges),
  data.table = df_table_3(Badges)
)


# ----------------------------- (4)

df_sql_4 <- function() {
  sqldf("
        SELECT
            Users.AccountId,
            Users.DisplayName,
            Users.Location,
            AVG(PostAuth.AnswersCount) as AverageAnswersCount
        FROM
          (
            SELECT
                AnsCount.AnswersCount,
                Posts.Id,
                Posts.OwnerUserId
            FROM 
              (
                SELECT Posts.ParentId, COUNT(*) AS AnswersCount 
                FROM Posts
                WHERE Posts.PostTypeId = 2
                GROUP BY Posts.ParentId
              ) AS AnsCount
            JOIN Posts ON Posts.Id = AnsCount.ParentId
          ) AS PostAuth
        JOIN Users ON Users.AccountId=PostAuth.OwnerUserId 
        GROUP BY OwnerUserId
        ORDER BY AverageAnswersCount DESC, AccountId ASC 
        LIMIT 10
        ")
} 

df_base_4 <- function(Posts, Users) {
  
  AnsCount <- Posts[Posts$PostTypeId == 2, ]
  AnsCount <- as.data.frame(table(AnsCount$ParentId), stringsAsFactors = FALSE)
  names(AnsCount) <- c("ParentId", "AnswersCount")

  PostAuth <- merge(x=Posts, y=AnsCount, by.x="Id", by.y="ParentId")
  PostAuth <- PostAuth[c("AnswersCount", "Id", "OwnerUserId")] 
  
  PostAuth <- aggregate(PostAuth["AnswersCount"], PostAuth["OwnerUserId"], mean)
  names(PostAuth)[names(PostAuth) == "AnswersCount"] <- "AverageAnswersCount"
  
  x <- merge(x=Users, y=PostAuth, by.x="AccountId", by.y="OwnerUserId")
  x <- x[c("AccountId", "DisplayName", "Location", "AverageAnswersCount")]

  x <- x[order(x$AccountId),]
  x <- x[order(x$AverageAnswersCount, decreasing = TRUE),][1:10,]
  row.names(x) <- NULL
  
  x
  
}

df_dplyr_4 <- function(Posts, Users) {

  AnsCount <- filter(Posts, PostTypeId == 2) %>% group_by(ParentId) %>% summarise(AnswersCount = n())

  PostAuth <- inner_join(x=Posts, y=AnsCount, by=c("Id" = "ParentId")) %>% select(AnswersCount, Id, OwnerUserId) %>%
    group_by(OwnerUserId) %>% summarise(AverageAnswersCount = mean(AnswersCount)) 

  x <- inner_join(Users, PostAuth, by=c("AccountId" = "OwnerUserId")) %>% 
    select(AccountId, DisplayName, Location, AverageAnswersCount) %>% arrange(desc(AverageAnswersCount), AccountId) %>% 
    slice(1:10)
  
  x
  
}

df_table_4 <- function(Posts, Users) {
  
  PostsDT <- as.data.table(Posts)
  UsersDT <- as.data.table(Users)

  AnsCount <- PostsDT[PostTypeId == 2, .(AnswersCount = .N), ParentId]
  
  PostAuth <- merge(x=PostsDT, y=AnsCount, by.x="Id", by.y="ParentId")[, .(AnswersCount, Id, OwnerUserId)][
    , .(AverageAnswersCount = mean(AnswersCount)), OwnerUserId]
  
  x <- merge(x=UsersDT, y=PostAuth, by.x="AccountId", by.y="OwnerUserId")[
    ,.(AccountId, DisplayName, Location, AverageAnswersCount)][order(-AverageAnswersCount, AccountId)][1:10]
  
  as.data.frame(x)
  
}

dplyr::all_equal(df_sql_4(), df_base_4(Posts, Users))
compare::compare(df_sql_4(), df_base_4(Posts, Users))

dplyr::all_equal(df_sql_4(), df_dplyr_4(Posts, Users))
compare::compare(df_sql_4(), df_dplyr_4(Posts, Users))

dplyr::all_equal(df_sql_4(), df_table_4(Posts, Users))
compare::compare(df_sql_4(), df_table_4(Posts, Users))

microbenchmark::microbenchmark(
  sqldf = df_sql_4(), 
  base = df_base_4(Posts, Users),
  dplyr = df_dplyr_4(Posts, Users),
  data.table = df_table_4(Posts, Users)
)


# ----------------------------- (5)

df_sql_5 <- function() {
  sqldf("
    SELECT Posts.Title, Posts.Id,
            STRFTIME('%Y-%m-%d', Posts.CreationDate) AS Date, 
            VotesByAge.Votes
    FROM Posts 
    JOIN (
            SELECT
                  PostId,
                  MAX(CASE WHEN VoteDate = 'new' THEN Total ELSE 0 END) NewVotes, 
                  MAX(CASE WHEN VoteDate = 'old' THEN Total ELSE 0 END) OldVotes, 
                  SUM(Total) AS Votes
            FROM ( 
                  SELECT
                        PostId,
                        CASE STRFTIME('%Y', CreationDate)
                            WHEN '2021' THEN 'new'
                            WHEN '2020' THEN 'new' 
                            ELSE 'old'
                            END VoteDate,
                        COUNT(*) AS Total 
                  FROM Votes
                  WHERE VoteTypeId IN (1, 2, 5)
                  GROUP BY PostId, VoteDate 
                  ) AS VotesDates
            GROUP BY VotesDates.PostId
            HAVING NewVotes > OldVotes
          ) AS VotesByAge ON Posts.Id = VotesByAge.PostId 
    WHERE Title NOT IN ('')
    ORDER BY Votes DESC
    LIMIT 10
    ")
} 

df_base_5 <- function(Votes, Posts) {
  
  x <- Votes[Votes$VoteTypeId == 1 | Votes$VoteTypeId == 2 | Votes$VoteTypeId == 5,]
  x$VoteDate <- sapply(stri_sub(x$CreationDate, 1, 4), switch,
                       "2021" = "new",
                       "2020" = "new",
                       "old"
                       )
 
  x <- as.data.frame(table(x$PostId, x$VoteDate), stringsAsFactors = FALSE)
  names(x) <- c("PostId", "VoteDate", "Total")
  VotesDates <- x[x$Total != 0,]
  
  VotesDates$NewVotes <- apply(VotesDates, 1, function(x) 
    as.integer(switch(x["VoteDate"], "new" = x["Total"], "old" = 0)))
  VotesDates$OldVotes <- apply(VotesDates, 1, function(x) 
    as.integer(switch(x["VoteDate"], "new" = 0, "old" = x["Total"])))
  VotesByAge <- aggregate(VotesDates[c("NewVotes", "OldVotes", "Total")], VotesDates["PostId"], sum)
  names(VotesByAge) <- c("PostId", "NewVotes", "OldVotes", "Votes")

  VotesByAge <- VotesByAge[VotesByAge$NewVotes > VotesByAge$OldVotes,]
  
  x <- merge(x=Posts, y=VotesByAge, by.x="Id", by.y="PostId")
  x <- x[x$Title != "",]
  x$Date <- stri_sub(x$CreationDate, 1, 10)
  x <- x[c("Title", "Id", "Date", "Votes")]
  
  x <- x[order(x$Votes, decreasing = TRUE),][1:10,]
  row.names(x) <- NULL
  
  x
  
}

df_dplyr_5 <- function(Votes, Posts) {
  
  VotesDates <- filter(Votes, VoteTypeId == 1 | VoteTypeId == 2 | VoteTypeId == 5) %>%
    mutate(VoteDate = case_when(
      stri_sub(CreationDate, 1, 4) == "2021" ~ "new",
      stri_sub(CreationDate, 1, 4) == "2020" ~ "new",
      TRUE ~ "old"
    )) %>% group_by(PostId, VoteDate) %>% summarise(Total = n())

  VotesByAge <- group_by(VotesDates, PostId) %>% 
    mutate(NewVotes = max(
      case_when(
        VoteDate == "new" ~ Total, 
        TRUE ~ 0L))) %>% 
    mutate(OldVotes = max(
      case_when(
        VoteDate == "old" ~ Total, 
        TRUE ~ 0L))) %>% 
    mutate(Votes = sum(Total)) %>% filter(NewVotes > OldVotes) %>% select(PostId, NewVotes, OldVotes, Votes) 

  x <- inner_join(Posts, VotesByAge, by=c("Id" = "PostId")) %>% filter(Title != "") %>% 
    mutate(Date = stri_sub(CreationDate, 1, 10)) %>% select(Title, Id, Date, Votes) %>%
    arrange(desc(Votes)) %>% slice(1:10)
  
  x
  
} 


df_table_5 <- function(Votes, Posts) {
  
  VotesDT <- as.data.table(Votes)
  PostsDT <- as.data.table(Posts)
  
  VotesDates <- VotesDT[VoteTypeId == 1 | VoteTypeId == 2 | VoteTypeId == 5, 
                        .(VoteDate = fcase(
                          stri_sub(CreationDate, 1, 4) == "2021", "new",
                          stri_sub(CreationDate, 1, 4) == "2020", "new",
                          default = "old"
                        ), PostId)][, .(Total = .N), .(PostId, VoteDate)]
  
  VotesByAge <- VotesDates[ , .(NewVotes = max(
                                    fcase(
                                      VoteDate == "new", Total,
                                      default = 0L)),
                                    OldVotes = max(
                                      fcase(
                                        VoteDate == "old", Total,
                                        default = 0L)
                                    ), 
                                Votes = sum(Total)), PostId][NewVotes>OldVotes]
  
  x <- merge(x=PostsDT, y=VotesByAge, by.x="Id", by.y="PostId")
  x <- x[Title != "", .(Title, Id, Date = stri_sub(CreationDate, 1, 10), Votes)][order(-Votes)][1:10]
  
  as.data.frame(x)
  
}


dplyr::all_equal(df_sql_5(), df_base_5(Votes, Posts))
compare::compare(df_sql_5(), df_base_5(Votes, Posts))

dplyr::all_equal(df_sql_5(), df_dplyr_5(Votes, Posts))
compare::compare(df_sql_5(), df_dplyr_5(Votes, Posts))

dplyr::all_equal(df_sql_5(), df_table_5(Votes, Posts))
compare::compare(df_sql_5(), df_table_5(Votes, Posts))

microbenchmark::microbenchmark(
  sqldf = df_sql_5(),
  base = df_base_5(Votes, Posts),
  dplyr = df_dplyr_5(Votes, Posts),
  data.table = df_table_5(Votes, Posts),
  times = 10L
)
