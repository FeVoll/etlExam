CREATE TABLE transactions (
  Customer_ID        Uint64,
  Name               Utf8,
  Surname            Utf8,
  Gender             Utf8,
  Birthdate          Utf8,
  Transaction_Amount Double,
  Date               Date,
  Merchant_Name      Utf8,
  Category           Utf8,
  PRIMARY KEY (Customer_ID, Date)
) 