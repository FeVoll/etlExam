CREATE TABLE transactions_v2 (
    id Uint64,
    Timestamp Utf8,
    TwizzlerRating Uint64,
    TwizzlerFrequency Utf8,
    Race Utf8,
    Gender Utf8,
    Income Utf8,
    Education Utf8,
    SleepHours Double,
    WorkHoursPerWeek Uint64,
    PRIMARY KEY (id)
);
