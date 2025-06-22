CREATE KEYSPACE IF NOT EXISTS sample_airbnb
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE sample_airbnb;

-- UDT para reviews
CREATE TYPE IF NOT EXISTS review_udt (
    id text,
    date_review timestamp,
    listing_id text,
    reviewer_id text,
    reviewer_name text,
    comments text
);

CREATE TABLE IF NOT EXISTS listingsAndReviews (
    id text PRIMARY KEY,
    listing_url text,
    name text,
    summary text,
    space text,
    description text,
    neighborhood_overview text,
    price decimal,
    amenities set<text>,
    reviews list<frozen<review_udt>>
);
