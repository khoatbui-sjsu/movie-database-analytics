CREATE DATABASE TMDBMovie;
USE TMDBMovie;

CREATE TABLE language(
    language_code VARCHAR(5) PRIMARY KEY,
    language_name VARCHAR(50)
);

CREATE TABLE movie(
    movie_id INT PRIMARY KEY,
    title VARCHAR(250) NOT NULL,
    vote_average DECIMAL(5,3), 
    vote_count INT,
    status VARCHAR(25),
    release_date DATE, 
    revenue DOUBLE, 
    runtime INT,
    adult BOOLEAN,
    backdrop_path VARCHAR(50), 
    budget BIGINT,
    homepage VARCHAR(255), 
    imdb_id VARCHAR(15),
    original_language_code VARCHAR(5), 
    original_title VARCHAR(250) NOT NULL,
    overview TEXT, 
    popularity DECIMAL(10,3),
    poster_path VARCHAR(50),
    tagline VARCHAR(500),

    FOREIGN KEY (original_language_code) REFERENCES language(language_code)
);

CREATE TABLE genre(
    genre_id INT AUTO_INCREMENT PRIMARY KEY,
    genre_name VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE movie_genre(
    movie_id INT NOT NULL,
    genre_id INT NOT NULL,
    PRIMARY KEY(movie_id, genre_id),

    FOREIGN KEY (movie_id) REFERENCES movie(movie_id),
    FOREIGN KEY (genre_id) REFERENCES genre(genre_id)
);

CREATE TABLE company(
    company_id INT AUTO_INCREMENT PRIMARY KEY,
    company_name VARCHAR(250) NOT NULL UNIQUE
);

CREATE TABLE movie_company(
    movie_id INT NOT NULL, 
    company_id INT NOT NULL,
    PRIMARY KEY(movie_id, company_id),

    FOREIGN KEY (movie_id) REFERENCES movie(movie_id),
    FOREIGN KEY (company_id) REFERENCES company(company_id)
);

CREATE TABLE country(
    country_id INT AUTO_INCREMENT PRIMARY KEY,
    country_name VARCHAR(200) NOT NULL UNIQUE
);

CREATE TABLE movie_country(
    movie_id INT NOT NULL, 
    country_id INT NOT NULL,
    PRIMARY KEY(movie_id, country_id),

    FOREIGN KEY (movie_id) REFERENCES movie(movie_id),
    FOREIGN KEY (country_id) REFERENCES country(country_id)
);

CREATE TABLE keyword(
    keyword_id INT AUTO_INCREMENT PRIMARY KEY,
    keyword_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE movie_keyword(
    movie_id INT NOT NULL, 
    keyword_id INT NOT NULL,
    PRIMARY KEY(movie_id, keyword_id),

    FOREIGN KEY (movie_id) REFERENCES movie(movie_id),
    FOREIGN KEY (keyword_id) REFERENCES keyword(keyword_id)
);

CREATE TABLE movie_language(
    movie_id INT NOT NULL,
    language_code VARCHAR(5) NOT NULL,
    PRIMARY KEY(movie_id, language_code),

    FOREIGN KEY (movie_id) REFERENCES movie(movie_id),
    FOREIGN KEY (language_code) REFERENCES language(language_code)
);
