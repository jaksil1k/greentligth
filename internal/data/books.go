package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"greenlight.zhaksylyk.kz/internal/validator"
	"time"
)

type Books struct {
	ID        int64     `json:"id"`
	CreatedAt time.Time `json:"-"`
	Title     string    `json:"title"`
	Sales     int32     `json:"sales"`
	Pages     int32     `json:"pages"`
	Year      int32     `json:"year,omitempty"`
	Runtime   Runtime   `json:"runtime,omitempty"`
	Genres    []string  `json:"genres,omitempty"`
	Version   int32     `json:"version"`
}

func ValidateBook(v *validator.Validator, book *Books) {
	v.Check(book.Title != "", "title", "must be provided")
	v.Check(len(book.Title) <= 500, "title", "must not be more than 500 bytes long")
	v.Check(book.Year != 0, "year", "must be provided")
	v.Check(book.Year >= 1888, "year", "must be greater than 1888")
	v.Check(book.Year <= int32(time.Now().Year()), "year", "must not be in the future")
	v.Check(book.Runtime != 0, "runtime", "must be provided")
	v.Check(book.Runtime > 0, "runtime", "must be a positive integer")
	v.Check(book.Genres != nil, "genres", "must be provided")
	v.Check(len(book.Genres) >= 1, "genres", "must contain at least 1 genre")
	v.Check(len(book.Genres) <= 5, "genres", "must not contain more than 5 genres")
	v.Check(validator.Unique(book.Genres), "genres", "must not contain duplicate values")
	v.Check(book.Sales >= 0, "sales", "must positive")
	v.Check(book.Pages >= 0, "pages", "must be positive")
	v.Check(book.Pages <= 10000, "pages", "must be less than 10000")
}

type BookModel struct {
	DB *pgxpool.Pool
}

func (m BookModel) Insert(books *Books) error {

	query := `
INSERT INTO books (title, sales, pages, year, runtime, genres)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING id, created_at, version`

	args := []any{books.Title, books.Sales, books.Pages, books.Year, books.Runtime, books.Genres}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	return m.DB.QueryRow(ctx, query, args...).Scan(&books.ID, &books.CreatedAt, &books.Version)
}

func (m BookModel) Get(id int64) (*Books, error) {
	// The PostgreSQL bigserial type that we're using for the book ID starts
	// auto-incrementing at 1 by default, so we know that no movies will have ID values
	// less than that. To avoid making an unnecessary database call, we take a shortcut
	// and return an ErrRecordNotFound error straight away.
	if id < 1 {
		return nil, ErrRecordNotFound
	}
	// Define the SQL query for retrieving the book data.
	query := `
SELECT id, created_at, title, sales, pages, year, runtime, genres, version
FROM books
WHERE id = $1`
	// Declare a Books struct to hold the data returned by the query.
	var book Books

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	// Importantly, use defer to make sure that we cancel the context before the Get()
	// method returns.
	defer cancel()

	err := m.DB.QueryRow(ctx, query, id).Scan(
		&book.ID,
		&book.CreatedAt,
		&book.Title,
		&book.Sales,
		&book.Pages,
		&book.Year,
		&book.Runtime,
		&book.Genres,
		&book.Version,
	)
	// Handle any errors. If there was no matching book found, Scan() will return
	// a sql.ErrNoRows error. We check for this and return our custom ErrRecordNotFound
	// error instead.
	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return nil, ErrRecordNotFound
		default:
			return nil, err
		}
	}
	// Otherwise, return a pointer to the Books struct.
	return &book, nil
}

func (m BookModel) Update(book *Books) error {
	// Add the 'AND version = $6' clause to the SQL query.
	query := `
SELECT id, created_at, title, sales, pages, year, runtime, genres, version
FROM books
WHERE (to_tsvector('simple', title) @@ plainto_tsquery('simple', $1) OR $1 = '')
AND (genres @> $2 OR $2 = '{}')
ORDER BY id`

	args := []any{
		book.Title,
		book.Year,
		book.Sales,
		book.Pages,
		book.Runtime,
		book.Genres,
		book.ID,
		book.Version, // Add the expected book version.
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Execute the SQL query. If no matching row could be found, we know the book
	// version has changed (or the record has been deleted) and we return our custom
	// ErrEditConflict error.
	err := m.DB.QueryRow(ctx, query, args...).Scan(&book.Version)
	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return ErrEditConflict
		default:
			return err
		}
	}
	return nil
}

func (m BookModel) Delete(id int64) error {
	// Return an ErrRecordNotFound error if the movie ID is less than 1.
	if id < 1 {
		return ErrRecordNotFound
	}
	// Construct the SQL query to delete the record.
	query := `
DELETE FROM books
WHERE id = $1`
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// Execute the SQL query using the Exec() method, passing in the id variable as
	// the value for the placeholder parameter. The Exec() method returns a sql.Result
	// object.
	result, err := m.DB.Exec(ctx, query, id)
	if err != nil {
		return err
	}
	// Call the RowsAffected() method on the sql.Result object to get the number of rows
	// affected by the query.
	rowsAffected := result.RowsAffected()
	if err != nil {
		return err
	}
	// If no rows were affected, we know that the movies table didn't contain a record
	// with the provided ID at the moment we tried to delete it. In that case we
	// return an ErrRecordNotFound error.
	if rowsAffected == 0 {
		return ErrRecordNotFound
	}
	return nil
}

func (m BookModel) GetAll(title string, sales int32, pages int32, genres []string, filters Filters) ([]*Books, Metadata, error) {
	// Construct the SQL query to retrieve all movie records.
	query := fmt.Sprintf(`
SELECT count(*) OVER(), id, created_at, title, sales, pages, year, runtime, genres, version
FROM books
WHERE (to_tsvector('simple', title) @@ plainto_tsquery('simple', $1) OR $1 = '')
AND (genres @> $2 OR $2 = '{}')
ORDER BY %s %s, id ASC
LIMIT $3 OFFSET $4`, filters.sortColumn(), filters.sortDirection())

	// Create a context with a 3-second timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	args := []any{title, sales, pages, genres, filters.limit(), filters.offset()}
	// Use QueryContext() to execute the query. This returns a sql.Rows resultset
	// containing the result.
	rows, err := m.DB.Query(ctx, query, args...)
	if err != nil {
		return nil, Metadata{}, err
	}

	defer rows.Close()
	// Initialize an empty slice to hold the movie data.
	books := []*Books{}
	totalRecords := 0
	// Use rows.Next to iterate through the rows in the resultset.
	for rows.Next() {
		// Initialize an empty Books struct to hold the data for an individual book.
		var book Books
		// Scan the values from the row into the Books struct. Again, note that we're
		// using the pq.Array() adapter on the genres field here.
		err := rows.Scan(
			&totalRecords,
			&book.ID,
			&book.CreatedAt,
			&book.Title,
			&book.Sales,
			&book.Pages,
			&book.Year,
			&book.Runtime,
			&book.Genres,
			&book.Version,
		)
		if err != nil {
			return nil, Metadata{}, err
		}
		// Add the Books struct to the slice.
		books = append(books, &book)
	}
	// When the rows.Next() loop has finished, call rows.Err() to retrieve any error
	// that was encountered during the iteration.
	if err = rows.Err(); err != nil {
		return nil, Metadata{}, err
	}

	metadata := calculateMetadata(totalRecords, filters.Page, filters.PageSize)
	// If everything went OK, then return the slice of books.
	return books, metadata, nil

}
