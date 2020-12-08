create table various_types (
    int64 bigint as (1 + 2) stored,
    uint64 bigint unsigned as (pow(7, 8)) stored, -- 5764801
    float32 float as (9 / 16) stored,
    float64 double as (5e222) stored,
    string text as (sha1(repeat('x', uint64))) stored, -- '6ad8402ba6610f04d3ec5c9875489a7bc8e259c5'
    bytes blob as (unhex(string)) stored,
    `decimal` decimal(8, 4) as (1234.5678) stored,
    duration time as ('1:2:3') stored,
    enum enum('a','b','c') as ('c') stored,
    bit bit(4) as (int64) stored,
    `set` set('a','b','c') as (enum) stored,
    time timestamp(3) as ('1987-06-05 04:03:02.100') stored,
    json json as (json_object(string, float32)) stored
);
