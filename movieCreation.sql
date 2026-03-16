CREATE DATABASE TMDBMovie;

USE TMDBMovie;

CREATE TABLE Languages(
	language_code CHAR(2) PRIMARY KEY
);

CREATE TABLE Movies(
	id INT PRIMARY KEY,
    title VARCHAR(250) NOT NULL,
    vote_average DECIMAL(4,3), 
    vote_count INT,
    status VARCHAR(25),
    release_date DATE, 
    revenue BIGINT, 
    runtime INT,
    adult BOOLEAN,
    backdrop_path VARCHAR(50), -- many null values
    budget BIGINT,
    homepage VARCHAR(50), -- most values are null
    imdb_id VARCHAR(15), -- many null vals
    original_language_code CHAR(2), 
    original_title VARCHAR(250) NOT NULL,
    overview VARCHAR(2500), -- 23% null
    popularity DECIMAL(10,3),
    poster_path VARCHAR(25), -- 35% null
    tagline VARCHAR(500), -- 86% null
    
    FOREIGN KEY (original_language_code) REFERENCES Languages(language_code)
);

CREATE TABLE Genres(
	id INT AUTO_INCREMENT PRIMARY KEY,
    genre_name VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE MovieGenre(
	movie_id INT NOT NULL,
    genre_id INT NOT NULL,
    PRIMARY KEY(movie_id, genre_id),
    
    FOREIGN KEY (movie_id) REFERENCES Movies(id),
    FOREIGN KEY (genre_id) REFERENCES Genres(id)
);

CREATE TABLE ProductionCompany(
	id INT AUTO_INCREMENT PRIMARY KEY,
    company_name VARCHAR(250) NOT NULL UNIQUE
);

CREATE TABLE MovieProductionCompany(
	movie_id INT NOT NULL, 
    company_id INT NOT NULL,
    PRIMARY KEY(movie_id, company_id),
    
    FOREIGN KEY (movie_id) REFERENCES Movies(id),
    FOREIGN KEY (company_id) REFERENCES ProductionCompany(id)
);

CREATE TABLE Country(
	id INT AUTO_INCREMENT PRIMARY KEY,
    countryName VARCHAR(200) NOT NULL UNIQUE
);

CREATE TABLE MovieProductionCountry(
	movie_id INT NOT NULL, 
    country_id INT NOT NULL,
    PRIMARY KEY(movie_id, country_id),
    
    FOREIGN KEY (movie_id) REFERENCES Movies(id),
    FOREIGN KEY (country_id) REFERENCES Country(id)
);

CREATE TABLE Keywords(
	id INT AUTO_INCREMENT PRIMARY KEY,
    keyword_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE MovieKeyword(
	movie_id INT NOT NULL, 
    keyword_id INT NOT NULL,
    PRIMARY KEY(movie_id, keyword_id),
    
    FOREIGN KEY (movie_id) REFERENCES Movies(id),
    FOREIGN KEY (keyword_id) REFERENCES Keywords(id)
);


