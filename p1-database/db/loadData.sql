INSERT INTO USERS (USER_ID, FIRST_NAME, LAST_NAME, 
YEAR_OF_BIRTH, MONTH_OF_BIRTH, DAY_OF_BIRTH, GENDER)
SELECT 
    DISTINCT USER_ID, FIRST_NAME, LAST_NAME, YEAR_OF_BIRTH, MONTH_OF_BIRTH, DAY_OF_BIRTH, GENDER
FROM project1.PUBLIC_USER_INFORMATION PUI;

INSERT INTO FRIENDS
SELECT 
    USER1_ID, USER2_ID
FROM project1.PUBLIC_ARE_FRIENDS PAF
WHERE USER1_ID <> USER2_ID;

INSERT INTO CITIES (CITY_NAME, STATE_NAME, COUNTRY_NAME)
SELECT
    DISTINCT C.CURRENT_CITY, C.CURRENT_STATE, C.CURRENT_COUNTRY
FROM project1.PUBLIC_USER_INFORMATION C
WHERE C.CURRENT_CITY IS NOT NULL
UNION
SELECT
    DISTINCT H.HOMETOWN_CITY, H.HOMETOWN_STATE, H.HOMETOWN_COUNTRY
FROM project1.PUBLIC_USER_INFORMATION H
WHERE H.HOMETOWN_CITY IS NOT NULL
UNION
SELECT
    DISTINCT E.EVENT_CITY, E.EVENT_STATE, E.EVENT_COUNTRY
FROM project1.PUBLIC_EVENT_INFORMATION E
WHERE E.EVENT_CITY IS NOT NULL;

INSERT INTO USER_CURRENT_CITIES (USER_ID, CURRENT_CITY_ID)
SELECT 
    DISTINCT P.USER_ID, C.CITY_ID
FROM project1.PUBLIC_USER_INFORMATION P
LEFT JOIN CITIES C
ON (P.CURRENT_CITY = C.CITY_NAME 
AND P.CURRENT_STATE = C.STATE_NAME 
AND P.CURRENT_COUNTRY = C.COUNTRY_NAME);

INSERT INTO USER_HOMETOWN_CITIES (USER_ID, HOMETOWN_CITY_ID)
SELECT
    DISTINCT P.USER_ID, C.CITY_ID
FROM project1.PUBLIC_USER_INFORMATION P
LEFT JOIN CITIES C
ON (P.HOMETOWN_CITY = C.CITY_NAME 
AND P.HOMETOWN_STATE = C.STATE_NAME 
AND P.HOMETOWN_COUNTRY = C.COUNTRY_NAME);

INSERT INTO PROGRAMS (INSTITUTION, CONCENTRATION, DEGREE)
SELECT 
    DISTINCT INSTITUTION_NAME, PROGRAM_CONCENTRATION, PROGRAM_DEGREE
FROM project1.PUBLIC_USER_INFORMATION P
WHERE INSTITUTION_NAME IS NOT NULL 
AND PROGRAM_CONCENTRATION IS NOT NULL
AND PROGRAM_DEGREE IS NOT NULL;

INSERT INTO EDUCATION (USER_ID, PROGRAM_ID, PROGRAM_YEAR)
SELECT 
    DISTINCT P.USER_ID, PRO.PROGRAM_ID, P.PROGRAM_YEAR
FROM project1.PUBLIC_USER_INFORMATION P
RIGHT JOIN PROGRAMS PRO
ON (P.INSTITUTION_NAME = PRO.INSTITUTION
AND P.PROGRAM_CONCENTRATION = PRO.CONCENTRATION
AND P.PROGRAM_DEGREE = PRO.DEGREE);

INSERT INTO USER_EVENTS (
    EVENT_ID,
    EVENT_CREATOR_ID,
    EVENT_NAME,
    EVENT_TAGLINE,
    EVENT_DESCRIPTION,
    EVENT_HOST,
    EVENT_TYPE,
    EVENT_SUBTYPE,
    EVENT_ADDRESS,
    EVENT_CITY_ID,
    EVENT_START_TIME,
    EVENT_END_TIME
)
SELECT 
    DISTINCT E.EVENT_ID, E.EVENT_CREATOR_ID, E.EVENT_NAME, 
    E.EVENT_TAGLINE, E.EVENT_DESCRIPTION, E.EVENT_HOST, 
    E.EVENT_TYPE, E.EVENT_SUBTYPE, E.EVENT_ADDRESS, C.CITY_ID, 
    E.EVENT_START_TIME, E.EVENT_END_TIME
FROM project1.PUBLIC_EVENT_INFORMATION E
LEFT JOIN CITIES C 
ON (E.EVENT_CITY = C.CITY_NAME 
AND E.EVENT_STATE = C.STATE_NAME 
AND E.EVENT_COUNTRY = C.COUNTRY_NAME)
WHERE E.EVENT_CREATOR_ID IS NOT NULL;

SET AUTOCOMMIT OFF;

INSERT INTO ALBUMS (
    ALBUM_ID,
    ALBUM_OWNER_ID,
    ALBUM_NAME,
    ALBUM_CREATED_TIME,
    ALBUM_MODIFIED_TIME,
    ALBUM_LINK,
    ALBUM_VISIBILITY,
    COVER_PHOTO_ID
)
SELECT 
    DISTINCT ALBUM_ID, OWNER_ID, ALBUM_NAME, 
    ALBUM_CREATED_TIME, ALBUM_MODIFIED_TIME, ALBUM_LINK,
    ALBUM_VISIBILITY, COVER_PHOTO_ID
FROM project1.PUBLIC_PHOTO_INFORMATION PHO;

INSERT INTO PHOTOS (
    PHOTO_ID,
    ALBUM_ID,
    PHOTO_CAPTION,
    PHOTO_CREATED_TIME,
    PHOTO_MODIFIED_TIME,
    PHOTO_LINK
)
SELECT 
    DISTINCT PHOTO_ID, ALBUM_ID, PHOTO_CAPTION,
    PHOTO_CREATED_TIME, PHOTO_MODIFIED_TIME, PHOTO_LINK
FROM project1.PUBLIC_PHOTO_INFORMATION PHO;

SET AUTOCOMMIT ON;

INSERT INTO TAGS (
    TAG_PHOTO_ID, TAG_SUBJECT_ID, TAG_CREATED_TIME, TAG_X, TAG_Y
)
SELECT 
    PHOTO_ID, TAG_SUBJECT_ID, TAG_CREATED_TIME, 
    TAG_X_COORDINATE, TAG_Y_COORDINATE
FROM project1.PUBLIC_TAG_INFORMATION T;