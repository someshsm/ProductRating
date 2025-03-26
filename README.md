# **ProductRating**
This project provides the list of top 3 rated products for every month of 2024.

### About:
This project uses:
- Python 3.x as interpreter.
- SQLite3 as database.
- PyTest for unit testing.
- Poetry for dependency management.

### Installation:

##### Install Python 3.x
##### Install Poetry:
    'pip install poetry'

### Setting up poetry virtual environment configuration   
    `poetry config virtualenvs.in-project true`

### Create and Install the dependency 
    ´poetry install´

### To see all the dependencies 
    ´poetry show´

### Running the Application:
Run the project with command: "product-rating"
    `poetry run product-rating`

#### Running Test:
To run tests:
    `poetry run pytest`

### Database Schema:

#### Table "Ratings" :
    "CREATE TABLE Ratings(timestamp DATE, user_id INT, product_id INT, rating INT);"

#### Table "RatingsMonthlyAggregate" :
    "CREATE TABLE RatingsMonthlyAggregate(product_id INT NOT NULL, Jan2024 DECIMAL(10,2), Feb2024 DECIMAL(10,2), Mar2024 DECIMAL(10,2), 
    Apr2024 DECIMAL(10,2), May2024 DECIMAL(10,2), Jun2024 DECIMAL(10,2), Jul2024 DECIMAL(10,2), Aug2024 DECIMAL(10,2), Sep2024 DECIMAL(10,2), 
    Oct2024 DECIMAL(10,2), Nov2024 DECIMAL(10,2), Dec2024 DECIMAL(10,2));"